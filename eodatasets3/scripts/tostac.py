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
@click.argument(
    "odc_metadata_files",
    type=PathPath(exists=True, readable=True, writable=False),
    nargs=-1,
)
def run(odc_metadata_files: Iterable[Path]):
    for input_metadata in odc_metadata_files:
        dataset = serialise.from_path(input_metadata)

        project = partial(
            pyproj.transform,
            pyproj.Proj(init=dataset.crs),
            pyproj.Proj(init="epsg:4326"),
        )
        wgs84_geometry: BaseGeometry = transform(project, dataset.geometry)
        item_doc = dict(
            id=dataset.id,
            type="Feature",
            bbox=wgs84_geometry.bounds,
            geometry=wgs84_geometry.__geo_interface__,
            properties={**dataset.properties, "odc:product": dataset.product.name},
            assets={
                # TODO: Currently assuming no name collisions.
                **{name: {"href": m.path} for name, m in dataset.measurements.items()},
                **{name: {"href": m.path} for name, m in dataset.accessories.items()},
            },
            links=[
                # {
                #     "rel": "self",
                #     "href": '?',
                # },
                {"rel": "odc_product", "href": dataset.product.href},
                {
                    "rel": "alternative",
                    "type": "text/html",
                    "href": f"https://explorer.dea.ga.gov.au/dataset/{dataset.id}",
                },
            ],
        )

        name = input_metadata.stem.replace(".odc-metadata", "")
        output_path = input_metadata.with_name(f"{name}.stac-item.json")

        with output_path.open("w") as f:
            json.dump(item_doc, f, indent=4, default=json_fallback)

        echo(output_path)


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
