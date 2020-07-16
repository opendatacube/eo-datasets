"""
Convert a new-style ODC metadata doc to a Stac Item. (BETA/Incomplete)
"""
import json
from datetime import datetime
from functools import partial
from pathlib import Path
from typing import Iterable
from uuid import UUID

import click
import pyproj
from click import echo
from shapely.geometry.base import BaseGeometry
from shapely.ops import transform

from eodatasets3 import serialise
from eodatasets3.ui import PathPath


@click.command(help=__doc__)
@click.option("--stac-template", '-t', type=str)
@click.option("--base-url", '-u', default="", type=str)
@click.argument(
    "odc_metadata_files",
    type=PathPath(exists=True, readable=True, writable=False),
    nargs=-1,
)
def run(odc_metadata_files: Iterable[Path], stac_template, base_url):
    for input_metadata in odc_metadata_files:
        dataset = serialise.from_path(input_metadata)

        name = input_metadata.stem.replace(".odc-metadata", "")
        output_path = input_metadata.with_name(f"{name}.stac-item.json")

        # Load STAC template data
        if stac_template:
            with open(stac_template, "r") as f:
                stac_data = json.load(f)
        else:
            stac_data = {}

        # Create STAC dict
        item_doc = create_stac(dataset, input_metadata, output_path, stac_data, base_url)

        with output_path.open("w") as f:
            json.dump(item_doc, f, indent=4, default=json_fallback)

        echo(output_path)


def create_stac(dataset, input_metadata, output_path, stac_data, base_url):
    """
    Create STAC document

    :param dataset: Dict of the metadata content
    :param input_metadata: Path of the Input metadata file
    :param output_path: Path of the STAC output file
    :param stac_data: Dict of the static STAC content
    :param base_url: Base URL path for STAC file
    :return: Dict of the STAC content
    """

    project = partial(
        pyproj.transform,
        pyproj.Proj(init=dataset.crs),
        pyproj.Proj(init="epsg:4326"),
    )
    wgs84_geometry: BaseGeometry = transform(project, dataset.geometry)
    item_doc = dict(
        stac_version=stac_data["stac_version"] if "stac_version" in stac_data else "1.0.0-beta.1",
        stac_extensions=stac_data["stac_extensions"] if "stac_extensions" in stac_data else [],
        id=dataset.id,
        type=stac_data["type"] if "type" in stac_data else "Feature",
        bbox=wgs84_geometry.bounds,
        geometry=wgs84_geometry.__geo_interface__,
        properties={**dataset.properties, "odc:product": dataset.product.name},
        # TODO: Currently assuming no name collisions.
        assets={
            **{name: (
                {**stac_data["assets"][name], "href": base_url + m.path}
                if "assets" in stac_data and name in stac_data["assets"] else
                {"href": base_url + m.path}
            ) for name, m in dataset.measurements.items()},
            **{name: (
                {**stac_data["assets"][name], "href": base_url + m.path}
                if "assets" in stac_data and name in stac_data["assets"] else
                {"href": base_url + m.path}
            ) for name, m in dataset.accessories.items()},
        },
        links=[
            # {
            #     "rel": "self",
            #     "href": '?',
            # },
            {
                "rel": "self",
                "type": "application/json",
                "href": base_url + output_path.name
            },
            {
                "title": "STAC's Source Item",
                "rel": "derived_from",
                "href": base_url + input_metadata.name
            },
            {
                "rel": "odc_product",
                "type": "text/html",
                "href": dataset.product.href
            },
            {
                "rel": "alternative",
                "type": "text/html",
                "href": f"https://explorer.dea.ga.gov.au/dataset/{dataset.id}",
            },
        ],
    )
    return item_doc


def json_fallback(o):
    if isinstance(o, datetime):
        return o.isoformat()

    if isinstance(o, UUID):
        return str(o)

    raise TypeError(
        f"Unhandled type for json conversion: "
        f"{o.__class__.__name__!r} "
        f"(object {o!r})"
    )


if __name__ == "__main__":
    run()
