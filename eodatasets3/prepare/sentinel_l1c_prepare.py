"""
Prepare eo3 metadata for Sentinel-2 Level 1C data produced by Sinergise or esa.

Takes ESA zipped datasets or Sinergise dataset directories
"""

import json
import sys
import uuid
import zipfile
from pathlib import Path
from typing import Tuple, Dict, List, Optional

from click import echo
from defusedxml import minidom
import click

from eodatasets3 import DatasetAssembler
from eodatasets3.ui import PathPath

SENTINEL_MSI_BAND_ALIASES = {
    "01": "coastal_aerosol",
    "02": "blue",
    "03": "green",
    "04": "red",
    "05": "red_edge_1",
    "06": "red_edge_2",
    "07": "red_edge_3",
    "08": "nir_1",
    "8A": "nir_2",
    "09": "water_vapour",
    "10": "swir_1_cirrus",
    "11": "swir_2",
    "12": "swir_3",
}


def process_product_info(product_path: Path) -> Dict:
    with product_path.open() as fp:
        product = json.load(fp)

    if len(product["tiles"]) > 1:
        raise NotImplementedError("No support for multi-tiled products yet")
    tile = product["tiles"][0]

    utm_zone = tile["utmZone"]
    latitude_band = tile["latitudeBand"]
    grid_square = tile["gridSquare"]
    return {
        "sinergise_product_name": product["name"],
        "sinergise_product_id": product["id"],
        "datetime": tile["timestamp"],
        "odc:region_code": f"{utm_zone}{latitude_band}{grid_square}",
        "sentinel:utm_zone": utm_zone,
        "sentinel:latitude_band": latitude_band,
        "sentinel:grid_square": grid_square,
    }


def _node_name(el):
    """Get a human-readable path to the given xml node"""
    branch = [el]
    while branch[-1].parentNode is not None:
        branch.append(branch[-1].parentNode)

    branch.reverse()
    return "/".join(n.localName for n in branch[1:])


def _value(root, *tags: str, type_=None):
    """Get the text contents of a tag in an xml document.

    Takes the tag name(s) to search for.
    """
    el = root

    for tag in tags:
        found = el.getElementsByTagName(tag)
        if not found:
            raise ValueError(f"Element not found in document: {tags!r}")
        if len(found) > 1:
            vs = "\n\t".join([_node_name(e) for e in found])
            raise ValueError(f"Multiple matches found for tag: {tags!r}: \n\t{vs}")

        el = found[0]

    el.normalize()
    if not len(el.childNodes) == 1:
        raise NotImplementedError(
            f"Not yet supported: Multiple child tags found for {tags!r}"
        )
    value = el.firstChild.data.strip()

    if not value:
        raise ValueError(f"Empty value for document at {tags!r}")

    if type_ is not None:
        value = type_(value)
    return value


def process_metadata_xml(metadata_xml_path: Path) -> Dict:
    root = minidom.parse(str(metadata_xml_path))

    resolution = min(
        int(i.attributes["resolution"].value) for i in root.getElementsByTagName("Size")
    )
    return {
        "eo:cloud_cover": _value(root, "CLOUDY_PIXEL_PERCENTAGE", type_=float),
        "sentinel:datastrip_id": _value(root, "DATASTRIP_ID"),
        "sentinel:sentinel_tile_id": _value(root, "TILE_ID"),
        "eo:sun_azimuth": _value(root, "Mean_Sun_Angle", "ZENITH_ANGLE", type_=float),
        "eo:sun_elevation": _value(
            root, "Mean_Sun_Angle", "AZIMUTH_ANGLE", type_=float
        ),
        "eo:gsd": resolution,
        "odc:processing_datetime": _value(root, "ARCHIVING_TIME"),
    }


def process_mtd_ds(contents: str) -> Dict:
    root = minidom.parseString(contents)

    resolution = min(
        int(i.firstChild.data) for i in root.getElementsByTagName("RESOLUTION")
    )
    return {
        "sentinel:reception_station": _value(root, "RECEPTION_STATION"),
        "sentinel:processing_center": _value(root, "PROCESSING_CENTER"),
        "eo:gsd": resolution,
    }


def process_mtd_tl(contents: str) -> Dict:
    root = minidom.parseString(contents)
    return {
        "eo:sun_azimuth": _value(root, "Mean_Sun_Angle", "AZIMUTH_ANGLE"),
        "eo:sun_elevation": _value(root, "Mean_Sun_Angle", "ZENITH_ANGLE"),
        "sentinel:datastrip_id": _value(root, "DATASTRIP_ID"),
        "datetime": _value(root, "SENSING_TIME"),
    }


def process_mtd_msil1c(contents: str) -> Dict:
    root = minidom.parseString(contents)

    product_uri = _value(root, "PRODUCT_URI")
    region_code = product_uri.split("_")[5][1:]
    return {
        "eo:platform": _value(root, "SPACECRAFT_NAME"),
        "sat:relative_orbit": _value(root, "SENSING_ORBIT_NUMBER", type_=int),
        "sat:orbit_state": _value(root, "SENSING_ORBIT_DIRECTION").lower(),
        "sentinel:datatake_type": _value(root, "DATATAKE_TYPE"),
        "odc:processing_datetime": _value(root, "GENERATION_TIME"),
        "sentinel:processing_baseline": _value(root, "PROCESSING_BASELINE"),
        "eo:cloud_cover": _value(root, "Cloud_Coverage_Assessment"),
        "odc:region_code": region_code,
    }


def prepare_and_write(
    dataset: Path,
    dataset_document: Path,
) -> Tuple[uuid.UUID, Path]:
    # Process esa dataset
    if dataset.suffix == ".zip":
        with zipfile.ZipFile(dataset, "r") as z:
            # Get file paths for esa metadata files
            mtd_ds_zip_path = [s for s in z.namelist() if "MTD_DS.xml" in s][0]
            mtd_tl_zip_path = [s for s in z.namelist() if "MTD_TL.xml" in s][0]
            mtd_msil1c_zip_path = [s for s in z.namelist() if "MTD_MSIL1C.xml" in s][0]

            # Crawl through metadata files and return a dict of useful information
            mtd_ds = process_mtd_ds(z.read(mtd_ds_zip_path).decode("utf-8"))
            mtd_tl = process_mtd_tl(z.read(mtd_tl_zip_path).decode("utf-8"))
            mtd_msil1c = process_mtd_msil1c(z.read(mtd_msil1c_zip_path).decode("utf-8"))

            with DatasetAssembler(
                metadata_path=dataset_document,
                dataset_location=dataset,
            ) as p:

                p.properties["eo:instrument"] = "MSI"
                p.properties["odc:producer"] = "esa.int"
                p.properties["odc:product_family"] = "level1"
                p.properties["odc:file_format"] = "JPEG2000"

                p.properties.update(mtd_ds)
                p.properties.update(mtd_tl)
                p.properties.update(mtd_msil1c)

                p.properties["odc:dataset_version"] = f"1.0.{p.processed:%Y%m%d}"

                for file in z.namelist():
                    # T55HFA_20201011T000249_B01.jp2
                    if ".jp2" in file and "TCI" not in file and "PVI" not in file:
                        # path = 'zip:%s!%s' % (str(dataset), str(file))
                        p.note_measurement(
                            path=file,
                            name=(
                                SENTINEL_MSI_BAND_ALIASES[
                                    (
                                        file.split("_")[len(file.split("_")) - 1]
                                        .replace(".jp2", "")
                                        .replace("B", "")
                                    )
                                ]
                            ),
                            relative_to_dataset_location=True
                            # path=path, name=name
                        )

                p.add_accessory_file("metadata:mtd_ds", mtd_ds_zip_path)
                p.add_accessory_file("metadata:mtd_tl", mtd_tl_zip_path)
                p.add_accessory_file("metadata:mtd_msil1c", mtd_msil1c_zip_path)

                return p.done()

    # process sinergise dataset
    elif dataset.is_dir():

        # Get file paths for sinergise metadata files
        product_info_path = dataset / "productInfo.json"
        metadata_xml_path = dataset / "metadata.xml"

        if not product_info_path.exists():
            raise ValueError(
                "No productInfo.json file found. "
                "Are you sure the input is a sinergise dataset folder?"
            )

        # Crawl through metadata files and return a dict of useful information
        product_info = process_product_info(product_info_path)
        metadata_xml = process_metadata_xml(metadata_xml_path)

        with DatasetAssembler(
            metadata_path=dataset_document,
            dataset_location=dataset,
        ) as p:
            p.properties["eo:platform"] = "sentinel-2a"
            p.properties["eo:instrument"] = "MSI"
            p.properties["odc:file_format"] = "JPEG2000"
            p.properties["odc:product_family"] = "level1"
            p.properties["odc:producer"] = "sinergise.com"

            p.properties.update(metadata_xml)
            p.properties.update(product_info)

            p.properties["odc:dataset_version"] = f"1.0.{p.processed:%Y%m%d}"

            for path in dataset.rglob("*.jp2"):
                if "preview" not in path.stem and "TCI" not in path.stem:
                    p.note_measurement(
                        path=path,
                        name=SENTINEL_MSI_BAND_ALIASES[path.stem.replace("B", "")],
                    )

            p.add_accessory_file("metadata:product_info", product_info_path)
            p.add_accessory_file("metadata:sinergise_metadata", metadata_xml_path)
            return p.done()
    else:
        raise NotImplementedError("Unknown input file type?")


@click.command(help=__doc__)
@click.argument(
    "datasets",
    type=PathPath(exists=True, readable=True, writable=False),
    nargs=-1,
)
@click.option(
    "--overwrite-existing/--skip-existing",
    is_flag=True,
    default=False,
    help="Overwrite if exists (otherwise skip)",
)
@click.option(
    "--output-base",
    help="Write metadata files into a directory instead of alongside each dataset",
    required=False,
    type=PathPath(exists=True, writable=True, dir_okay=True, file_okay=False),
)
def main(
    output_base: Optional[Path],
    datasets: List[Path],
    overwrite_existing: bool,
):
    for dataset in datasets:

        if dataset.is_dir():
            output_path = dataset / f"{dataset.stem}.odc-metadata.yaml"
        else:
            output_path = dataset.with_suffix(".odc-metadata.yaml")

        if output_base:
            output_path = output_base / output_path.name

        if output_path.exists() and not overwrite_existing:
            echo(f"Output exists. Skipping {output_path.name}")
            continue

        uuid, path = prepare_and_write(
            dataset,
            output_path,
        )
        echo(f"Wrote {path}")

        sys.exit(0)


if __name__ == "__main__":
    main()
