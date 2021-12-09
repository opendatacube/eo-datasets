"""
Convert an EO3 metadata doc to a Stac Item. (BETA/Incomplete)
"""
import datetime
import math
import mimetypes
import warnings
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin

import datacube.utils.uris as dc_uris
from datacube.utils.geometry import CRS, Geometry
from pystac import Asset, Item, Link, MediaType
from pystac.errors import STACError
from pystac.extensions.eo import Band, EOExtension
from pystac.extensions.projection import ProjectionExtension
from pystac.extensions.view import ViewExtension
from pystac.utils import datetime_to_str
from requests_cache import CachedSession

from eodatasets3.model import DatasetDoc, GridDoc

# Mapping between EO3 field names and STAC properties object field names
MAPPING_EO3_TO_STAC = {
    "dtr:end_datetime": "end_datetime",
    "dtr:start_datetime": "start_datetime",
    "eo:gsd": "gsd",
    "eo:instrument": "instruments",
    "eo:platform": "platform",
    "eo:constellation": "constellation",
    "eo:off_nadir": "view:off_nadir",
    "eo:azimuth": "view:azimuth",
    "eo:sun_azimuth": "view:sun_azimuth",
    "eo:sun_elevation": "view:sun_elevation",
    "odc:processing_datetime": "created",
}


def _as_stac_instruments(value: str):
    """
    >>> _as_stac_instruments('TM')
    ['tm']
    >>> _as_stac_instruments('OLI')
    ['oli']
    >>> _as_stac_instruments('ETM+')
    ['etm']
    >>> _as_stac_instruments('OLI_TIRS')
    ['oli', 'tirs']
    """
    return [i.strip("+-").lower() for i in value.split("_")]


def _convert_value_to_stac_type(key: str, value):
    """
    Convert return type as per STAC specification
    """
    # In STAC spec, "instruments" have [String] type
    if key == "eo:instrument":
        return _as_stac_instruments(value)
    # Convert the non-default datetimes to a string
    elif isinstance(value, datetime.datetime) and key != "datetime":
        return datetime_to_str(value)
    else:
        return value


def _media_type(path: Path) -> str:
    """
    Add media type of the asset object
    """
    mime_type = mimetypes.guess_type(path.name)[0]
    if path.suffix == ".sha1":
        return MediaType.TEXT
    elif path.suffix == ".yaml":
        return "text/yaml"
    elif mime_type:
        if mime_type == "image/tiff":
            return MediaType.COG
        else:
            return mime_type
    else:
        return "application/octet-stream"


def _asset_roles_fields(asset_name: str) -> List[str]:
    """
    Add roles of the asset object
    """
    if asset_name.startswith("thumbnail"):
        return ["thumbnail"]
    else:
        return ["metadata"]


def _asset_title_fields(asset_name: str) -> Optional[str]:
    """
    Add title of the asset object
    """
    if asset_name.startswith("thumbnail"):
        return "Thumbnail image"
    else:
        return None


def _proj_fields(grid: Dict[str, GridDoc], grid_name: str = "default") -> Dict:
    """
    Get any proj (Stac projection extension) fields if we have them for the grid.
    """
    if not grid:
        return {}

    grid_doc = grid.get(grid_name or "default")
    if not grid_doc:
        return {}

    return {
        "shape": grid_doc.shape,
        "transform": grid_doc.transform,
    }


def _lineage_fields(lineage: Dict) -> Dict:
    """
    Add custom lineage field to a STAC Item
    """
    if lineage:
        lineage_dict = {
            key: [str(uuid) for uuid in value] for key, value in lineage.items()
        }

        return {"odc:lineage": lineage_dict}
    else:
        return {}


def _odc_links(
    explorer_base_url: str,
    dataset: DatasetDoc,
    collection_url: Optional[str],
) -> List[Link]:
    """
    Add links for ODC product into a STAC Item
    """

    if collection_url:
        yield Link(
            rel="collection",
            target=collection_url,
        )
    if explorer_base_url:
        if not collection_url:
            yield Link(
                rel="collection",
                target=urljoin(
                    explorer_base_url, f"/stac/collections/{dataset.product.name}"
                ),
            )
        yield Link(
            title="ODC Product Overview",
            rel="product_overview",
            media_type="text/html",
            target=urljoin(explorer_base_url, f"product/{dataset.product.name}"),
        )
        yield Link(
            title="ODC Dataset Overview",
            rel="alternative",
            media_type="text/html",
            target=urljoin(explorer_base_url, f"dataset/{dataset.id}"),
        )

    if not collection_url and not explorer_base_url:
        warnings.warn("No collection provided for Stac Item.")


def _get_projection(dataset: DatasetDoc) -> Tuple[int, str]:
    if dataset.crs is None:
        return None

    crs_l = dataset.crs.lower()
    epsg = None
    wkt = None
    if crs_l.startswith("epsg:"):
        epsg = int(crs_l.lstrip("epsg:"))
    else:
        wkt = dataset.crs

    return epsg, wkt


def eo3_to_stac_properties(
    dataset: DatasetDoc, crs: Optional[str] = None, title: str = None
) -> Dict:
    """
    Convert EO3 properties dictionary to the Stac equivalent.
    """
    properties = {
        # Put the title at the top for document readability.
        **(dict(title=title) if title else {}),
        **{
            MAPPING_EO3_TO_STAC.get(key, key): _convert_value_to_stac_type(key, val)
            for key, val in dataset.properties.items()
        },
    }

    return properties


def to_stac_item(
    dataset: DatasetDoc,
    stac_item_destination_url: str,
    dataset_location: Optional[str] = None,
    odc_dataset_metadata_url: Optional[str] = None,
    explorer_base_url: Optional[str] = None,
    collection_url: Optional[str] = None,
) -> dict:
    """
    Convert the given ODC Dataset into a Stac Item document.

    Note: You may want to call `validate_item(doc)` on the outputs to find any
    incomplete properties.

    :param collection_url: URL to the Stac Collection. Either this or an explorer_base_url
                           should be specified for Stac compliance.
    :param stac_item_destination_url: Public 'self' URL where the stac document will be findable.
    :param dataset_location: Use this location instead of picking from dataset.locations
                             (for calculating relative band paths)
    :param odc_dataset_metadata_url: Public URL for the original ODC dataset yaml document
    :param explorer_base_url: An Explorer instance that contains this dataset.
                              Will allow links to things such as the product definition.
    """

    if dataset.geometry is not None:
        geom = Geometry(dataset.geometry, CRS(dataset.crs))
        wgs84_geometry = geom.to_crs(CRS("epsg:4326"), math.inf)

        geometry = wgs84_geometry.json
        bbox = wgs84_geometry.boundingbox
    else:
        geometry = None
        bbox = None

    properties = eo3_to_stac_properties(dataset, title=dataset.label)
    properties.update(_lineage_fields(dataset.lineage))

    dt = properties["datetime"]
    del properties["datetime"]

    # TODO: choose remote if there's multiple locations?
    # Without a dataset location, all paths will be relative.
    dataset_location = dataset_location or (
        dataset.locations[0] if dataset.locations else None
    )

    item = Item(
        id=str(dataset.id),
        datetime=dt,
        properties=properties,
        geometry=geometry,
        bbox=bbox,
        collection=dataset.product.name,
    )

    # Add links
    if stac_item_destination_url:
        item.links.append(
            Link(
                rel="self",
                media_type=MediaType.JSON,
                target=stac_item_destination_url,
            )
        )
    if odc_dataset_metadata_url:
        item.links.append(
            Link(
                title="ODC Dataset YAML",
                rel="odc_yaml",
                media_type="text/yaml",
                target=odc_dataset_metadata_url,
            )
        )

    for link in _odc_links(explorer_base_url, dataset, collection_url):
        item.links.append(link)

    EOExtension.ext(item, add_if_missing=True)

    if dataset.geometry:
        proj = ProjectionExtension.ext(item, add_if_missing=True)
        epsg, wkt = _get_projection(dataset)
        if epsg is not None:
            proj.apply(epsg=epsg, **_proj_fields(dataset.grids))
        elif wkt is not None:
            proj.apply(wkt2=wkt, **_proj_fields(dataset.grids))
        else:
            raise STACError("Projection extension requires either epsg or wkt for crs.")

    # To pass validation, only add 'view' extension when we're using it somewhere.
    if any(k.startswith("view:") for k in properties.keys()):
        ViewExtension.ext(item, add_if_missing=True)

    # Add assets that are data
    for name, measurement in dataset.measurements.items():
        if not dataset_location and not measurement.path:
            # No URL to link to. URL is mandatory for Stac validation.
            continue

        asset = Asset(
            href=_uri_resolve(dataset_location, measurement.path),
            media_type=_media_type(Path(measurement.path)),
            title=name,
            roles=["data"],
        )
        eo = EOExtension.ext(asset)

        # TODO: pull out more information about the band
        band = Band.create(name)
        eo.apply(bands=[band])

        if dataset.grids:
            proj_fields = _proj_fields(dataset.grids, measurement.grid)
            if proj_fields is not None:
                proj = ProjectionExtension.ext(asset)
                # Not sure how this handles None for an EPSG code
                proj.apply(
                    shape=proj_fields["shape"],
                    transform=proj_fields["transform"],
                    epsg=epsg,
                )

        item.add_asset(name, asset=asset)

    # Add assets that are accessories
    for name, measurement in dataset.accessories.items():
        if not dataset_location and not measurement.path:
            # No URL to link to. URL is mandatory for Stac validation.
            continue

        asset = Asset(
            href=_uri_resolve(dataset_location, measurement.path),
            media_type=_media_type(Path(measurement.path)),
            title=_asset_title_fields(name),
            roles=_asset_roles_fields(name),
        )

        item.add_asset(name, asset=asset)

    return item.to_dict()


def validate_item(
    item_doc: Dict,
    allow_cached_specs: bool = True,
):
    """
    Validate a document against the Stac Item schema and its declared extensions

    Requires an internet connection the first time to fetch the relevant specs,
    but will cache them locally for repeated requests.

    :param item_doc:
    :param allow_cached_specs: Allow using a cached spec.
                              Disable to force-download the spec again.
    :param disallow_network_access: Only allow validation using cached specs.
    :param log: Callback for human-readable progress/status (eg: 'print').
    :param schema_host: The host to download stac schemas from.

    :raises NoAvailableSchemaError: When cannot find a spec for the given Stac version+extentions
    """

    one_day = 60 * 60 * 24

    with CachedSession(
        "stac_schema_cache",
        backend="sqlite",
        expire_after=one_day,
        old_data_on_error=True,
    ) as session:
        if not allow_cached_specs:
            session.cache.clear()

        item = Item.from_dict(item_doc)
        item.validate()


def _uri_resolve(location: str, path: str):
    # ODC's method doesn't support empty locations. Fall back to the path alone.
    if not location:
        return path

    return dc_uris.uri_resolve(location, path)
