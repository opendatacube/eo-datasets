"""
Convert a new-style ODC metadata doc to a Stac Item.
"""
import json
from datetime import datetime
from pathlib import Path
from typing import Iterable
from urllib.parse import urljoin
from uuid import UUID

import click

from eodatasets3 import serialise
from eodatasets3.model import DatasetDoc
from eodatasets3.stac import to_stac_item
from eodatasets3.ui import PathPath


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
    odc_metadata_files: Iterable[Path],
    stac_base_url: str,
    explorer_base_url: str,
    validate: bool,
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


def dc_to_stac(
    dataset: DatasetDoc,
    input_metadata: Path,
    output_path: Path,
    stac_base_url: str,
    explorer_base_url: str,
    do_validate: bool,
) -> dict:
    """
    Backwards compatibility wrapper as some other projects started using this
    method of the script.

    It's better to call eodatasets3.stac.dataset_as_stac_item() directly.
    """
    return to_stac_item(
        dataset,
        input_metadata_url=urljoin(stac_base_url, input_metadata.name),
        output_url=urljoin(stac_base_url, output_path.name),
        explorer_base_url=explorer_base_url,
        do_validate=do_validate,
    )


if __name__ == "__main__":
    run()
