"""
Convert a new-style ODC metadata doc to a Stac Item. (BETA/Incomplete)
"""
import json
import math
import mimetypes
from pathlib import Path
from typing import Dict, List
from urllib.parse import urljoin

from datacube.utils.geometry import Geometry, CRS
from jsonschema import validate
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


def _odc_links(explorer_base_url: str, dataset: DatasetDoc) -> List:
    """
    Add links for ODC product into a STAC Item
    """
    if explorer_base_url:
        return [
            {
                "title": "ODC Product Overview",
                "rel": "product_overview",
                "type": "text/html",
                "href": urljoin(explorer_base_url, f"product/{dataset.product.name}"),
            },
            {
                "title": "ODC Dataset Overview",
                "rel": "alternative",
                "type": "text/html",
                "href": urljoin(explorer_base_url, f"dataset/{dataset.id}"),
            },
            {
                "rel": "parent",
                "href": urljoin(
                    explorer_base_url, f"/stac/collections/{dataset.product.name}"
                ),
            },
        ]
    else:
        return []


def to_stac_item(
    dataset: DatasetDoc,
    input_metadata_url: str,
    output_url: str,
    explorer_base_url: str,
    do_validate: bool = False,
) -> dict:
    """
    Convert the given ODC Dataset into a Stac Item document
    """

    geom = Geometry(dataset.geometry, CRS(dataset.crs))
    wgs84_geometry = geom.to_crs(CRS("epsg:4326"), math.inf)

    properties = {
        # This field is required to be present, even if null.
        "proj:epsg": None,
    }
    crs = dataset.crs.lower()
    if crs.startswith("epsg:"):
        properties["proj:epsg"] = int(crs.lstrip("epsg:"))
    else:
        properties["proj:wkt2"] = dataset.crs

    if dataset.label:
        properties["title"] = dataset.label

    # TODO: choose remote if there's multiple locations?
    dataset_location = dataset.locations[0] if dataset.locations else output_url

    item_doc = dict(
        stac_version="1.0.0-beta.2",
        stac_extensions=["eo", "projection"],
        type="Feature",
        id=dataset.id,
        bbox=wgs84_geometry.boundingbox,
        geometry=wgs84_geometry.json,
        properties={
            **{
                MAPPING_EO3_TO_STAC.get(key, key): _convert_value_to_stac_type(key, val)
                for key, val in dataset.properties.items()
            },
            "odc:product": dataset.product.name,
            **_lineage_fields(dataset.lineage),
            **properties,
            **(_proj_fields(dataset.grids) if dataset.grids else {}),
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
        links=[
            {
                "rel": "self",
                "type": "application/json",
                "href": output_url,
            },
            {
                "title": "ODC Dataset YAML",
                "rel": "odc_yaml",
                "type": "text/yaml",
                "href": input_metadata_url,
            },
            *_odc_links(explorer_base_url, dataset),
        ],
    )

    # To pass validation, only add 'view' extension when we're using it somewhere.
    if any(k.startswith("view:") for k in item_doc["properties"].keys()):
        item_doc["stac_extensions"].append("view")

    if do_validate:
        validate_stac(item_doc)
    return item_doc


def validate_stac(item_doc: Dict):
    # Validates STAC content against schema of STAC item and STAC extensions
    stac_content = json.loads(json.dumps(item_doc))
    schema_urls = [
        f"https://schemas.stacspec.org/"
        f"v{item_doc.get('stac_version')}"
        f"/item-spec/json-schema/item.json#"
    ]
    for extension in item_doc.get("stac_extensions", []):
        schema_urls.append(
            f"https://schemas.stacspec.org/"
            f"v{item_doc.get('stac_version')}"
            f"/extensions/{extension}"
            f"/json-schema/schema.json#"
        )

    for schema_url in schema_urls:
        # Caching downloaded Schemas which expires every 1 hour
        requests = CachedSession(
            "stac_schema_cache", backend="sqlite", expire_after=3600
        )
        r = requests.get(schema_url)
        schema_json = r.json()
        validate(stac_content, schema_json)
