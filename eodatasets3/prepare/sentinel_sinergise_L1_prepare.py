"""
Prepare eo3 metadata for Sentinel-2 Level 1C data produced by Sinergise.
"""

from pathlib import Path
from xml.dom import minidom
import json
from os import listdir
from os.path import isfile, join
import uuid
import click
from typing import Dict, Tuple
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


def extract_metadata_from_product_info(product_path: Path) -> Dict:
    with open(product_path) as fp:
        product = json.loads(fp.read())

        synergise_product_name = product["name"]
        synergise_product_id = product["id"]
        if len(product["tiles"]) > 1:
            raise NotImplementedError("No support for multi-tiled products yet")
        timestamp = product["tiles"][0]["timestamp"]
        utm_zone = product["tiles"][0]["utmZone"]
        latitude_band = product["tiles"][0]["latitudeBand"]
        grid_square = product["tiles"][0]["gridSquare"]
        region_code = "%s%s%s" % (utm_zone, latitude_band, grid_square)

        return {
            "synergise_product_name": synergise_product_name,
            "synergise_product_id": synergise_product_id,
            "timestamp": timestamp,
            "region_code": region_code,
        }


def extract_metadata_from_metadata_xml(metadata_xml_path: str) -> Dict:
    xmldoc = minidom.parse(metadata_xml_path)

    cloud = float(
        xmldoc.getElementsByTagName("CLOUDY_PIXEL_PERCENTAGE")[0].firstChild.data
    )
    downlink_priority = xmldoc.getElementsByTagName("DOWNLINK_PRIORITY")[
        0
    ].firstChild.data
    datastrip_id = xmldoc.getElementsByTagName("DATASTRIP_ID")[0].firstChild.data
    solar_azimuth = float(
        xmldoc.getElementsByTagName("Mean_Sun_Angle")[0]
        .getElementsByTagName("ZENITH_ANGLE")[0]
        .firstChild.data
    )
    solar_zenith = float(
        xmldoc.getElementsByTagName("Mean_Sun_Angle")[0]
        .getElementsByTagName("AZIMUTH_ANGLE")[0]
        .firstChild.data
    )

    resolutions = xmldoc.getElementsByTagName("Size")
    r_list = []
    for i in resolutions:
        r_list.append(int(i.attributes["resolution"].value))
    resolution = min(r_list)

    return {
        "cloud": cloud,
        "downlink_priority": downlink_priority,
        "datastrip_id": datastrip_id,
        "solar_azimuth": solar_azimuth,
        "solar_zenith": solar_zenith,
        "resolution": resolution,
    }


def extract_metadata_from_format_correctness(format_correctness_path: str) -> Dict:
    xmldoc = minidom.parse(format_correctness_path)

    source_system = xmldoc.getElementsByTagName("System")[0].firstChild.data
    creator_version = xmldoc.getElementsByTagName("Creator_Version")[0].firstChild.data
    creation_date = xmldoc.getElementsByTagName("Creation_Date")[0].firstChild.data
    datastrip_metadata = xmldoc.getElementsByTagName("File_Name")[0].firstChild.data

    return {
        "source_system": source_system,
        "creator_version": creator_version,
        "creation_date": creation_date,
        "datastrip_metadata": datastrip_metadata,
    }


def prepare_and_write(
    output_yaml_path: Path,
    ds_path: Path,
    product_info: Dict,
    metadata_xml: Dict,
    format_correctness: Dict,
) -> Tuple[uuid.UUID, Path]:
    with DatasetAssembler(
        metadata_path=output_yaml_path, dataset_location=ds_path
    ) as p:
        p.datetime = product_info["timestamp"]
        p.properties["eo:instrument"] = "MSI"
        p.properties["eo:platform"] = "sentinel-2a"
        p.properties["odc:processing_datetime"] = (
            format_correctness["creation_date"].split("=")[1].replace("T", " ")
        )
        p.properties["odc:dataset_version"] = f"1.0.{p.processed:%Y%m%d}"
        p.properties["odc:producer"] = "sinergise.com"
        p.properties["odc:product_family"] = "level1"
        p.properties["eo:sun_elevation"] = metadata_xml["solar_zenith"]
        p.properties["eo:sun_azimuth"] = metadata_xml["solar_azimuth"]
        p.properties["eo:gsd"] = metadata_xml["resolution"]
        p.properties["eo:cloud_cover"] = metadata_xml["cloud"]
        p.properties["sentinel:sinergise_product_name"] = product_info[
            "synergise_product_name"
        ]
        p.properties["sentinel:sinergise_product_id"] = product_info[
            "synergise_product_id"
        ]
        p.properties["odc:file_format"] = "JPEG2000"
        p.properties["odc:region_code"] = product_info["region_code"]
        p.properties["sentinel:data_type"] = "Level-1C"
        p.properties["sentinel:product_type"] = "S2MSI1C"
        p.properties["sentinel:software_version"] = format_correctness[
            "creator_version"
        ]
        p.properties["sentinel:source_system"] = format_correctness["source_system"]
        p.properties["sentinel:datastrip_metadata"] = format_correctness[
            "datastrip_metadata"
        ]
        p.properties["sentinel:downlink_priority"] = metadata_xml["downlink_priority"]
        p.properties["sentinel:datastrip_id"] = metadata_xml["datastrip_id"]

        directory = [f for f in listdir(ds_path) if isfile(join(ds_path, f))]

        for dataset in directory:
            if ".jp2" in dataset and "preview" not in dataset and "TCI" not in dataset:
                band = dataset.replace(".jp2", "").replace("B", "")
                name = SENTINEL_MSI_BAND_ALIASES[band]
                path = Path(ds_path) / dataset
                p.note_measurement(path=path, name=name)

        return p.done()


@click.command(help=__doc__)
@click.option(
    "--product",
    type=PathPath(),
    required=True,
    help="Path to productInfo.json in sinergise dataset",
)
@click.option(
    "--metadata-xml",
    type=str,
    required=True,
    help="Path to metadata.xml in sinergise dataset",
)
@click.option(
    "--format-correctness",
    type=str,
    required=True,
    help="Path to FORMAT_CORRECTNESS.xml in sinergise dataset",
)
@click.option(
    "--dataset-document",
    type=PathPath(),
    required=True,
    help="Path to output dataset document (yaml)",
)
@click.option(
    "--dataset",
    type=PathPath(),
    required=True,
    help="Path to sinergise dataset",
)
def main(
    product: Path,
    metadata_xml: str,
    format_correctness: str,
    dataset_document: Path,
    dataset: Path,
):
    product_info = extract_metadata_from_product_info(product)
    metadata_xml_dict = extract_metadata_from_metadata_xml(metadata_xml)
    format_correctness_dict = extract_metadata_from_format_correctness(
        format_correctness
    )

    uuid, path = prepare_and_write(
        dataset_document,
        dataset,
        product_info,
        metadata_xml_dict,
        format_correctness_dict,
    )
    return path


if __name__ == "__main__":
    main()
