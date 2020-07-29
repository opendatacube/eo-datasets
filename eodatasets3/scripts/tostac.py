"""
Convert a new-style ODC metadata doc to a Stac Item. (BETA/Incomplete)
"""
import json
import math
from datetime import datetime
from functools import partial
from pathlib import Path
from typing import Iterable, Dict
from uuid import UUID
from urllib.parse import urljoin

import click
import pyproj
import requests
from jsonschema import validate

from eodatasets3 import serialise
from eodatasets3.model import DatasetDoc
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


@click.command(help=__doc__)
@click.option(
    "--stac-template",
    "-t",
    type=str,
    help="File path to the STAC document template with static content",
)
@click.option(
    "--stac-base-url", "-u", default="", type=str, help="Base URL of the STAC file"
)
@click.option(
    "--explorer-base-url",
    "-e",
    default="https://explorer.dea.ga.gov.au",
    type=str,
    help="Base URL of the ODC Explorer",
)
@click.option(
    "--validate/--no-validate",
    default=True,
    help="Flag it for stac document validation. By default flagged",
)
@click.argument(
    "odc_metadata_files",
    type=PathPath(exists=True, readable=True, writable=False),
    nargs=-1,
)
def run(
    odc_metadata_files: Iterable[Path],
    stac_template,
    stac_base_url,
    explorer_base_url,
    validate,
):
    for input_metadata in odc_metadata_files:
        dataset = serialise.from_path(input_metadata)

        name = input_metadata.stem.replace(".odc-metadata", "")
        output_path = input_metadata.with_name(f"{name}.stac-item.json")

        # Load STAC template data
        if stac_template:
            with open(stac_template, "r") as f:
                stac_data = json.load(f)
                if stac_data.get("stac_version", "") not in [
                    "1.0.0-beta.1",
                    "1.0.0-beta.2",
                ]:
                    raise NotImplementedError(
                        f"STAC version {stac_data.get('stac_version', '')} not implemented"
                    )
        else:
            stac_data = {}

        # Create STAC dict
        item_doc = dc_to_stac(
            dataset,
            input_metadata,
            output_path,
            stac_data,
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
    stac_data: dict,
    stac_base_url: str,
    explorer_base_url: str,
    do_validate: bool,
) -> dict:
    """
    Creates a STAC document
    """

    stac_ext = ["eo", "view", "projection"]
    stac_ext.extend(stac_data.get("stac_extensions", []))

    geom = Geometry(dataset.geometry, CRS(dataset.crs))
    wgs84_geometry = geom.to_crs(CRS("epsg:4326"), math.inf)

    item_doc = dict(
        stac_version="1.0.0-beta.2",
        stac_extensions=sorted(set(stac_ext)),
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
            "proj:epsg": int(dataset.crs.lstrip("epsg:")) if dataset.crs else None,
            "proj:shape": dataset.grids["default"].shape,
            "proj:transform": dataset.grids["default"].transform,
        },
        # TODO: Currently assuming no name collisions.
        assets={
            **{
                name: (
                    {
                        **stac_data.get("assets", {}).get(name, {}),
                        "href": urljoin(stac_base_url, m.path),
                        "proj.shape": dataset.grids[
                            m.grid if m.grid else "default"
                        ].shape,
                        "proj.transform": dataset.grids[
                            m.grid if m.grid else "default"
                        ].transform,
                    }
                )
                for name, m in dataset.measurements.items()
            },
            **{
                name: (
                    {
                        **stac_data.get("assets", {}).get(name, {}),
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
                "rel": "derived_from",
                "href": urljoin(stac_base_url, input_metadata.name),
            },
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
        ],
    )
    if do_validate:
        validate_stac(item_doc)
    return item_doc


def json_fallback(o):
    if isinstance(o, datetime):
        return f"{o.isoformat()}Z"

    if isinstance(o, UUID):
        return str(o)

    raise TypeError(
        f"Unhandled type for json conversion: "
        f"{o.__class__.__name__!r} "
        f"(object {o!r})"
    )


def validate_stac(item_doc: Dict):
    # Validates STAC content against schema of STAC item and STAC extensions
    stac_content = json.loads(json.dumps(item_doc, indent=4, default=json_fallback))
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
        schema_json = requests.get(schema_url).json()
        validate(stac_content, schema_json)


if __name__ == "__main__":
    run()
