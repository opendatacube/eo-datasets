"""
Convert a new-style ODC metadata doc to a Stac Item. (BETA/Incomplete)
"""
import json
import math
from datetime import datetime
from pathlib import Path
from typing import Iterable, Dict, List
from uuid import UUID
from urllib.parse import urljoin

import click
import mimetypes
from requests_cache.core import CachedSession
from jsonschema import validate

from eodatasets3 import serialise
from eodatasets3.model import DatasetDoc, GridDoc
from eodatasets3.ui import PathPath
from datacube.utils.geometry import Geometry, CRS


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
}


def convert_value_to_stac_type(key: str, value):
    """
    Convert return type as per STAC specification
    """
    # In STAC spec, "instruments" have [String] type
    if key == "eo:instrument":
        return [value]
    else:
        return value


def add_types(path: Path) -> Dict:
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


def add_roles(asset_name: str) -> Dict:
    """
    Add roles of the asset object
    """
    if asset_name.startswith("thumbnail"):
        return {"roles": ["thumbnail"]}
    elif asset_name.startswith("metadata"):
        return {"roles": ["metadata"]}
    else:
        return {}


def add_title(asset_name: str) -> Dict:
    """
    Add title of the asset object
    """
    if asset_name.startswith("thumbnail"):
        return {"title": "Thumbnail image"}
    else:
        return {}


def add_proj(
    field_name: str, grid: Dict[str, GridDoc], grid_name: str = "default"
) -> Dict:
    """
    Add fields of the STAC Projection (proj) Extension to a STAC Item
    """
    grid_doc = grid.get(grid_name)
    if grid_doc:
        proj_fields = {
            "proj:shape": grid_doc.shape,
            "proj:transform": grid_doc.transform,
        }
        field_value = proj_fields.get(field_name, None)
        return {field_name: field_value} if field_value else {}
    else:
        return {}


def add_lineage(lineage: Dict) -> Dict:
    """
    Add custom lineage field to a STAC Item
    """
    if lineage:
        return {"odc:lineage": lineage}
    else:
        return {}


def add_odc_links(explorer_base_url: str, dataset: DatasetDoc) -> List:
    """
        Add links for ODC product into a STAC Item
    """
    if explorer_base_url:
        return [
            {
                "title": "Open Data Cube Product Overview",
                "rel": "product_overview",
                "type": "text/html",
                "href": urljoin(explorer_base_url, f"product/{dataset.product.name}"),
            },
            {
                "title": "Open Data Cube Explorer",
                "rel": "alternative",
                "type": "text/html",
                "href": urljoin(explorer_base_url, f"dataset/{dataset.id}"),
            },
        ]
    else:
        return []


@click.command(help=__doc__)
@click.option("--stac-base-url", "-u", help="Base URL of the STAC file")
@click.option("--explorer-base-url", "-e", help="Base URL of the ODC Explorer")
@click.option(
    "--validate/--no-validate",
    default=False,
    help="Validate output STAC Item against online schemas",
)
@click.argument(
    "odc_metadata_files",
    type=PathPath(exists=True, readable=True, writable=False),
    nargs=-1,
)
def run(
    odc_metadata_files: Iterable[Path], stac_base_url, explorer_base_url, validate,
):
    for input_metadata in odc_metadata_files:
        dataset = serialise.from_path(input_metadata)

        name = input_metadata.stem.replace(".odc-metadata", "")
        output_path = input_metadata.with_name(f"{name}.stac-item.json")

        # Create STAC dict
        item_doc = dc_to_stac(
            dataset,
            input_metadata,
            output_path,
            stac_base_url,
            explorer_base_url,
            validate,
        )

        with output_path.open("w") as f:
            json.dump(item_doc, f, indent=4, default=json_fallback)


def dc_to_stac(
    dataset: DatasetDoc,
    input_metadata: Path,
    output_path: Path,
    stac_base_url: str,
    explorer_base_url: str,
    do_validate: bool,
) -> dict:
    """
    Creates a STAC document
    """

    geom = Geometry(dataset.geometry, CRS(dataset.crs))
    wgs84_geometry = geom.to_crs(CRS("epsg:4326"), math.inf)

    item_doc = dict(
        stac_version="1.0.0-beta.2",
        stac_extensions=["eo", "projection", "view"],
        type="Feature",
        id=dataset.id,
        bbox=wgs84_geometry.boundingbox,
        geometry=wgs84_geometry.json,
        properties={
            **{
                MAPPING_EO3_TO_STAC.get(key, key): convert_value_to_stac_type(key, val)
                for key, val in dataset.properties.items()
            },
            "odc:product": dataset.product.name,
            **add_lineage(dataset.lineage),
            "proj:epsg": int(dataset.crs.lstrip("epsg:")) if dataset.crs else None,
            **add_proj("proj:shape", dataset.grids),
            **add_proj("proj:transform", dataset.grids),
        },
        # TODO: Currently assuming no name collisions.
        assets={
            **{
                name: (
                    {
                        "eo:bands": [{"name": name}],
                        **add_types(Path(m.path)),
                        "roles": ["data"],
                        "href": urljoin(stac_base_url, m.path),
                        **add_proj("proj:shape", dataset.grids, m.grid),
                        **add_proj("proj:transform", dataset.grids, m.grid),
                    }
                )
                for name, m in dataset.measurements.items()
            },
            **{
                name: (
                    {
                        **add_title(name),
                        **add_types(Path(m.path)),
                        **add_roles(name),
                        "href": urljoin(stac_base_url, m.path),
                    }
                )
                for name, m in dataset.accessories.items()
            },
        },
        links=[
            {
                "rel": "self",
                "type": "application/json",
                "href": urljoin(stac_base_url, output_path.name),
            },
            {
                "title": "Source Dataset YAML",
                "rel": "odc_yaml",
                "type": "text/yaml",
                "href": urljoin(stac_base_url, input_metadata.name),
            },
            *add_odc_links(explorer_base_url, dataset),
        ],
    )
    if do_validate:
        validate_stac(item_doc)
    return item_doc


def json_fallback(o):
    if isinstance(o, datetime):
        return f"{o.isoformat()}" if o.tzinfo else f"{o.isoformat()}Z"

    if isinstance(o, UUID):
        return str(o)

    raise TypeError(
        f"Unhandled type for json conversion: "
        f"{o.__class__.__name__!r} "
        f"(object {o!r})"
    )


def validate_stac(item_doc: Dict):
    # Validates STAC content against schema of STAC item and STAC extensions
    stac_content = json.loads(json.dumps(item_doc, default=json_fallback))
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


if __name__ == "__main__":
    run()
