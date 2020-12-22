"""
Convert an EO3 metadata doc to a Stac Item. (BETA/Incomplete)
"""
import math
import mimetypes
import warnings
from pathlib import Path
from typing import Dict, List, Optional, Callable, Mapping
from urllib.parse import urljoin

import jsonschema
import rapidjson
from datacube.utils.geometry import Geometry, CRS
from requests_cache.core import CachedSession

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
    else:
        return value


def _media_fields(path: Path) -> Dict:
    """
    Add media type of the asset object
    """
    mime_type = mimetypes.guess_type(path.name)[0]
    if path.suffix == ".sha1":
        return {"type": "text/plain"}
    elif path.suffix == ".yaml":
        return {"type": "text/yaml"}
    elif mime_type:
        if mime_type == "image/tiff":
            return {"type": "image/tiff; application=geotiff"}
        else:
            return {"type": mime_type}
    else:
        return {}


def _asset_roles_fields(asset_name: str) -> Dict:
    """
    Add roles of the asset object
    """
    if asset_name.startswith("thumbnail"):
        return {"roles": ["thumbnail"]}
    elif asset_name.startswith("metadata"):
        return {"roles": ["metadata"]}
    else:
        return {}


def _asset_title_fields(asset_name: str) -> Dict:
    """
    Add title of the asset object
    """
    if asset_name.startswith("thumbnail"):
        return {"title": "Thumbnail image"}
    else:
        return {}


def _proj_fields(grid: Dict[str, GridDoc], grid_name: str = "default") -> Dict:
    """
    Add fields of the STAC Projection (proj) Extension to a STAC Item
    """
    grid_doc = grid.get(grid_name)
    if grid_doc:
        return {
            "proj:shape": grid_doc.shape,
            "proj:transform": grid_doc.transform,
        }
    else:
        return {}


def _lineage_fields(lineage: Dict) -> Dict:
    """
    Add custom lineage field to a STAC Item
    """
    if lineage:
        return {"odc:lineage": lineage}
    else:
        return {}


def _odc_links(
    explorer_base_url: str,
    dataset: DatasetDoc,
    collection_url: Optional[str],
) -> List:
    """
    Add links for ODC product into a STAC Item
    """

    if collection_url:
        yield {
            "rel": "collection",
            "href": collection_url,
        }
    if explorer_base_url:
        if not collection_url:
            yield {
                "rel": "collection",
                "href": urljoin(
                    explorer_base_url, f"/stac/collections/{dataset.product.name}"
                ),
            }
        yield {
            "title": "ODC Product Overview",
            "rel": "product_overview",
            "type": "text/html",
            "href": urljoin(explorer_base_url, f"product/{dataset.product.name}"),
        }
        yield {
            "title": "ODC Dataset Overview",
            "rel": "alternative",
            "type": "text/html",
            "href": urljoin(explorer_base_url, f"dataset/{dataset.id}"),
        }

    if not collection_url and not explorer_base_url:
        warnings.warn("No collection provided for Stac Item.")


def eo3_to_stac_properties(
    properties: Mapping, crs: Optional[str] = None, title: str = None
) -> Dict:
    """
    Convert EO3 properties dictionary to the Stac equivalent.
    """
    properties = {
        # Put the title at the top for document readability.
        **(dict(title=title) if title else {}),
        **{
            MAPPING_EO3_TO_STAC.get(key, key): _convert_value_to_stac_type(key, val)
            for key, val in properties.items()
        },
        # This field is required to be present, even if null.
        "proj:epsg": None,
    }
    crs_l = crs.lower()
    if crs_l.startswith("epsg:"):
        properties["proj:epsg"] = int(crs_l.lstrip("epsg:"))
    else:
        properties["proj:wkt2"] = crs

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

    geom = Geometry(dataset.geometry, CRS(dataset.crs))
    wgs84_geometry = geom.to_crs(CRS("epsg:4326"), math.inf)

    properties = eo3_to_stac_properties(
        dataset.properties, dataset.crs, title=dataset.label
    )

    # TODO: choose remote if there's multiple locations?
    # Without a dataset location, all paths will be relative.
    dataset_location = dataset_location or (
        dataset.locations[0] if dataset.locations else None
    )

    links = []
    if stac_item_destination_url:
        links.append(
            {
                "rel": "self",
                "type": "application/json",
                "href": stac_item_destination_url,
            }
        )
    if odc_dataset_metadata_url:
        links.append(
            {
                "title": "ODC Dataset YAML",
                "rel": "odc_yaml",
                "type": "text/yaml",
                "href": odc_dataset_metadata_url,
            }
        )
    links.extend(_odc_links(explorer_base_url, dataset, collection_url))

    item_doc = dict(
        stac_version="1.0.0-beta.2",
        stac_extensions=["eo", "projection"],
        type="Feature",
        id=dataset.id,
        bbox=wgs84_geometry.boundingbox,
        geometry=wgs84_geometry.json,
        properties={
            **properties,
            "odc:product": dataset.product.name,
            **(_proj_fields(dataset.grids) if dataset.grids else {}),
            **_lineage_fields(dataset.lineage),
        },
        # TODO: Currently assuming no name collisions.
        assets={
            **{
                name: (
                    {
                        "eo:bands": [{"name": name}],
                        **_media_fields(Path(m.path)),
                        "roles": ["data"],
                        "href": urljoin(dataset_location, m.path),
                        **(
                            _proj_fields(dataset.grids, m.grid) if dataset.grids else {}
                        ),
                    }
                )
                for name, m in dataset.measurements.items()
            },
            **{
                name: (
                    {
                        **_asset_title_fields(name),
                        **_media_fields(Path(m.path)),
                        **_asset_roles_fields(name),
                        "href": urljoin(dataset_location, m.path),
                    }
                )
                for name, m in dataset.accessories.items()
            },
        },
        links=links,
    )

    # To pass validation, only add 'view' extension when we're using it somewhere.
    if any(k.startswith("view:") for k in item_doc["properties"].keys()):
        item_doc["stac_extensions"].append("view")

    return item_doc


def validate_item(
    item_doc: Dict,
    allow_cached_specs: bool = True,
    disallow_network_access: bool = False,
    log: Callable[[str], None] = lambda line: None,
    schema_host="https://schemas.stacspec.org",
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
    item_doc = _normalise_doc(item_doc)

    stac_version = item_doc.get("stac_version")

    one_day = 60 * 60 * 24
    max_cache_time = one_day if "beta" in stac_version else one_day * 365

    schemas = [
        (
            "Item",
            f"{schema_host}/v{stac_version}/item-spec/json-schema/item.json#",
        )
    ]
    for extension in item_doc.get("stac_extensions", []):
        schemas.append(
            (
                f"extension {extension!r}",
                f"{schema_host}/v{stac_version}/extensions/{extension}/json-schema/schema.json#",
            )
        )

    log(f"Stac version {stac_version}. Schema cache: {max_cache_time/60//60}hrs.")

    with CachedSession(
        "stac_schema_cache",
        backend="sqlite",
        expire_after=max_cache_time,
        old_data_on_error=True,
    ) as session:
        if not allow_cached_specs:
            session.cache.clear()

        for schema_label, schema_url in schemas:
            if not session.cache.has_url(schema_url):
                if disallow_network_access:
                    raise NoAvailableSchemaError(
                        f"{schema_label} schema is not cached, and network access is disabled: {schema_url}"
                    )

                log(f"{schema_url}")
            r = session.get(schema_url, timeout=60)
            if r.status_code == 404:
                raise NoAvailableSchemaError(
                    f"No schema found for Stac {stac_version} {schema_label}: "
                    f"{schema_url!r}"
                )
            r.raise_for_status()
            schema_json = r.json()
            log(f"Validating {schema_label}...")
            jsonschema.validate(item_doc, schema_json)


class NoAvailableSchemaError(Exception):
    pass


def _normalise_doc(doc: Dict) -> Dict:
    """
    Normalise all the embedded values to simple json types.

    (needed for jsonschema validation.)
    """
    return rapidjson.loads(rapidjson.dumps(doc, datetime_mode=True, uuid_mode=True))
