"""
Prepare eo3 metadata for Sentinel-2 Level 1C data produced by Sinergise or esa.
"""

from pathlib import Path
from xml.dom import minidom
import json
import os
from os import listdir
from os.path import isfile, join
import uuid
import click
from typing import Tuple, Dict
from eodatasets3 import DatasetAssembler
from eodatasets3.ui import PathPath
import zipfile

HARDCODED = {
    "file_format": "JPEG2000",
    "instrument": "MSI",
    "product_family": "level1",
    "data_type": "Level-1C",
    "product_type": "S2MSI1C",
}


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


def find_metadata_path(find_filename, dataset_dir):
    for root, dir, fn in os.walk(dataset_dir):
        if find_filename in fn:
            return os.path.join(root, fn[fn.index(find_filename)])


def process_product_info(product_path):
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


def process_metadata_xml(metadata_xml_path: str) -> Dict:
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


def process_mtd_ds(mtd_ds_zip_path: str, zip_object: object) -> Dict:
    xmldoc = minidom.parseString(zip_object.read(mtd_ds_zip_path))

    reception_station = xmldoc.getElementsByTagName("RECEPTION_STATION")[
        0
    ].firstChild.data
    downlink_orbit_number = xmldoc.getElementsByTagName("DOWNLINK_ORBIT_NUMBER")[
        0
    ].firstChild.data
    processing_center = xmldoc.getElementsByTagName("PROCESSING_CENTER")[
        0
    ].firstChild.data

    resolutions = xmldoc.getElementsByTagName("RESOLUTION")
    r_list = []
    for i in resolutions:
        r_list.append(int(i.firstChild.data))
    resolution = min(r_list)
    return {
        "reception_station": reception_station,
        "downlink_orbit_number": downlink_orbit_number,
        "processing_center": processing_center,
        "resolution": resolution,
    }


def process_mtd_tl(mtd_tl_zip_path: str, zip_object: object) -> Dict:
    xmldoc = minidom.parseString(zip_object.read(mtd_tl_zip_path))

    sun_azimuth = xmldoc.getElementsByTagName("AZIMUTH_ANGLE")[0].firstChild.data
    sun_elevation = xmldoc.getElementsByTagName("ZENITH_ANGLE")[0].firstChild.data

    return {
        "sun_azimuth": sun_azimuth,
        "sun_elevation": sun_elevation,
    }


def process_mtd_msil1c(mtd_msil1c_zip_path: str, zip_object: object) -> Dict:
    xmldoc = minidom.parseString(zip_object.read(mtd_msil1c_zip_path))

    data_type = xmldoc.getElementsByTagName("PROCESSING_LEVEL")[0].firstChild.data
    datastrip_id = xmldoc.getElementsByTagName("PRODUCT_URI")[0].firstChild.data
    product_type = xmldoc.getElementsByTagName("PRODUCT_TYPE")[0].firstChild.data
    platform = xmldoc.getElementsByTagName("SPACECRAFT_NAME")[0].firstChild.data
    orbit = xmldoc.getElementsByTagName("SENSING_ORBIT_NUMBER")[0].firstChild.data
    orbit_direction = xmldoc.getElementsByTagName("SENSING_ORBIT_DIRECTION")[
        0
    ].firstChild.data
    datatake_type = xmldoc.getElementsByTagName("DATATAKE_TYPE")[0].firstChild.data
    processing_datetime = xmldoc.getElementsByTagName("GENERATION_TIME")[
        0
    ].firstChild.data
    processing_baseline = xmldoc.getElementsByTagName("PROCESSING_BASELINE")[
        0
    ].firstChild.data
    datetime = xmldoc.getElementsByTagName("PRODUCT_START_TIME")[0].firstChild.data
    cloud_cover = xmldoc.getElementsByTagName("Cloud_Coverage_Assessment")[
        0
    ].firstChild.data
    region_code = datastrip_id.split("_")[5][1:]

    return {
        "data_type": data_type,
        "datastrip_id": datastrip_id,
        "product_type": product_type,
        "platform": platform,
        "orbit": orbit,
        "orbit_direction": orbit_direction,
        "datatake_type": datatake_type,
        "processing_datetime": processing_datetime,
        "processing_baseline": processing_baseline,
        "datetime": datetime,
        "cloud_cover": cloud_cover,
        "region_code": region_code,
    }


def process_format_correctness(
    format_correctness_path: str,
    zip_object: object,
) -> Dict:
    if zip_object is not None:
        xmldoc = minidom.parseString(zip_object.read(format_correctness_path))

        source_system = xmldoc.getElementsByTagName("System")[0].firstChild.data
        software_version = xmldoc.getElementsByTagName("Creator_Version")[
            0
        ].firstChild.data

        return {
            "source_system": source_system,
            "software_version": software_version,
        }
    else:
        xmldoc = minidom.parse(format_correctness_path)

        source_system = xmldoc.getElementsByTagName("System")[0].firstChild.data
        creator_version = xmldoc.getElementsByTagName("Creator_Version")[
            0
        ].firstChild.data
        creation_date = xmldoc.getElementsByTagName("Creation_Date")[0].firstChild.data
        datastrip_metadata = xmldoc.getElementsByTagName("File_Name")[0].firstChild.data

        return {
            "source_system": source_system,
            "creator_version": creator_version,
            "creation_date": creation_date,
            "datastrip_metadata": datastrip_metadata,
        }


def prepare_and_write(
    dataset: Path,
    dataset_document: Path,
) -> Tuple[uuid.UUID, Path]:
    # Process esa dataset
    if "S2A_MSIL1C_" in str(dataset) or "S2B_MSIL1C_" in str(dataset):
        with zipfile.ZipFile(dataset, "r") as z:
            # Get file paths for esa metadata files
            mtd_ds_zip_path = [s for s in z.namelist() if "MTD_DS.xml" in s][0]
            mtd_tl_zip_path = [s for s in z.namelist() if "MTD_TL.xml" in s][0]
            mtd_msil1c_zip_path = [s for s in z.namelist() if "MTD_MSIL1C.xml" in s][0]
            format_correctness_zip_path = [
                s for s in z.namelist() if "FORMAT_CORRECTNESS.xml" in s
            ][0]

            # Crawl through metadata files and return a dict of useful information
            mtd_ds = process_mtd_ds(mtd_ds_zip_path, z)
            mtd_tl = process_mtd_tl(mtd_tl_zip_path, z)
            format_correctness = process_format_correctness(
                format_correctness_zip_path, z
            )
            mtd_msil1c = process_mtd_msil1c(mtd_msil1c_zip_path, z)

            with DatasetAssembler(
                metadata_path=dataset_document,
                dataset_location=dataset,
            ) as p:

                p.datetime = mtd_msil1c["datetime"]
                p.properties["eo:instrument"] = HARDCODED["instrument"]
                p.properties["eo:platform"] = mtd_msil1c["platform"]
                p.properties["odc:processing_datetime"] = mtd_msil1c[
                    "processing_datetime"
                ]
                p.properties["odc:dataset_version"] = f"1.0.{p.processed:%Y%m%d}"
                p.properties["odc:producer"] = "esa.int"
                p.properties["odc:product_family"] = HARDCODED["product_family"]
                p.properties["eo:sun_elevation"] = mtd_tl["sun_elevation"]
                p.properties["eo:sun_azimuth"] = mtd_tl["sun_azimuth"]
                p.properties["eo:gsd"] = mtd_ds["resolution"]
                p.properties["eo:cloud_cover"] = mtd_msil1c["cloud_cover"]
                p.properties["odc:file_format"] = HARDCODED["file_format"]
                p.properties["odc:region_code"] = mtd_msil1c["region_code"]
                p.properties["sentinel:data_type"] = mtd_msil1c["data_type"]
                p.properties["sentinel:product_type"] = mtd_msil1c["product_type"]
                p.properties["sentinel:software_version"] = format_correctness[
                    "software_version"
                ]
                p.properties["sentinel:source_system"] = format_correctness[
                    "source_system"
                ]
                p.properties["sentinel:datastrip_id"] = mtd_msil1c["datastrip_id"]
                p.properties["sentinel:downlink_orbit_number"] = mtd_ds[
                    "downlink_orbit_number"
                ]
                p.properties["sentinel:reception_station"] = mtd_ds["reception_station"]
                p.properties["sentinel:processing_center"] = mtd_ds["processing_center"]
                p.properties["sentinel:orbit"] = mtd_msil1c["orbit"]
                p.properties["sentinel:orbit_direction"] = mtd_msil1c["orbit_direction"]
                p.properties["sentinel:datatake_type"] = mtd_msil1c["datatake_type"]
                p.properties["sentinel:processing_baseline"] = mtd_msil1c[
                    "processing_baseline"
                ]

                for file in z.namelist():
                    # T55HFA_20201011T000249_B01.jp2
                    if ".jp2" in file and "TCI" not in file and "PVI" not in file:
                        band = (
                            file.split("_")[len(file.split("_")) - 1]
                            .replace(".jp2", "")
                            .replace("B", "")
                        )
                        name = SENTINEL_MSI_BAND_ALIASES[band]
                        # path = 'zip:%s!%s' % (str(dataset), str(file))
                        p.note_measurement(
                            path=file,
                            name=name,
                            relative_to_dataset_location=True
                            # path=path, name=name
                        )

                return p.done()

    # process sinergise dataset
    else:
        directory = [f for f in listdir(dataset) if isfile(join(dataset, f))]

        # Get file paths for sinergise metadata files
        product_info_path = find_metadata_path("productInfo.json", dataset)
        metadata_xml_path = find_metadata_path("metadata.xml", dataset)
        format_correctness_path = find_metadata_path("FORMAT_CORRECTNESS.xml", dataset)

        # Crawl through metadata files and return a dict of useful information
        product_info = process_product_info(product_info_path)
        metadata_xml = process_metadata_xml(metadata_xml_path)
        format_correctness = process_format_correctness(format_correctness_path, None)

        with DatasetAssembler(
            metadata_path=dataset_document,
            dataset_location=dataset,
        ) as p:
            p.datetime = product_info["timestamp"]
            p.properties["eo:instrument"] = HARDCODED["instrument"]
            p.properties["eo:platform"] = "sentinel-2a"
            p.properties["odc:processing_datetime"] = (
                format_correctness["creation_date"].split("=")[1].replace("T", " ")
            )
            p.properties["odc:dataset_version"] = f"1.0.{p.processed:%Y%m%d}"
            p.properties["odc:producer"] = "sinergise.com"
            p.properties["odc:product_family"] = HARDCODED["product_family"]
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
            p.properties["odc:file_format"] = HARDCODED["file_format"]
            p.properties["odc:region_code"] = product_info["region_code"]
            p.properties["sentinel:data_type"] = HARDCODED["data_type"]
            p.properties["sentinel:product_type"] = HARDCODED["product_type"]
            p.properties["sentinel:software_version"] = format_correctness[
                "creator_version"
            ]
            p.properties["sentinel:source_system"] = format_correctness["source_system"]
            p.properties["sentinel:datastrip_metadata"] = format_correctness[
                "datastrip_metadata"
            ]
            p.properties["sentinel:downlink_priority"] = metadata_xml[
                "downlink_priority"
            ]
            p.properties["sentinel:datastrip_id"] = metadata_xml["datastrip_id"]

            for ds in directory:
                if ".jp2" in ds and "preview" not in ds and "TCI" not in ds:
                    band = ds.replace(".jp2", "").replace("B", "")
                    name = SENTINEL_MSI_BAND_ALIASES[band]
                    path = Path(dataset) / ds
                    p.note_measurement(path=path, name=name)

            return p.done()


@click.command(help=__doc__)
@click.option(
    "--dataset",
    type=PathPath(),
    required=True,
    help="Path to ESA zipped dataset or Sinergise dataset directory",
)
@click.option(
    "--dataset-document",
    type=PathPath(),
    required=True,
    help="Location to output the L1C dataset document (yaml)",
)
def main(
    dataset: Path,
    dataset_document: Path,
):

    uuid, path = prepare_and_write(
        dataset,
        dataset_document,
    )
    return path


if __name__ == "__main__":
    main()
