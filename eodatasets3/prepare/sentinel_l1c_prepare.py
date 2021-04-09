"""
Prepare eo3 metadata for Sentinel-2 Level 1C data produced by Sinergise or esa.
"""

import json
import uuid
import zipfile
from os import listdir
from os.path import isfile, join
from pathlib import Path
from typing import Tuple, Dict
from xml.dom import minidom

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
        "sinergise_product_name": synergise_product_name,
        "sinergise_product_id": synergise_product_id,
        "datetime": timestamp,
        "odc:region_code": region_code,
        "sentinel:utm_zone": utm_zone,
        "sentinel:latitude_band": latitude_band,
        "sentinel:grid_square": grid_square,
    }


def process_metadata_xml(metadata_xml_path: Path) -> Dict:
    xmldoc = minidom.parse(str(metadata_xml_path))

    cloud = float(
        xmldoc.getElementsByTagName("CLOUDY_PIXEL_PERCENTAGE")[0].firstChild.data
    )

    datastrip_id = xmldoc.getElementsByTagName("DATASTRIP_ID")[0].firstChild.data
    tile_id = xmldoc.getElementsByTagName("TILE_ID")[0].firstChild.data
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

    processed_time = xmldoc.getElementsByTagName("ARCHIVING_TIME")[0].firstChild.data

    resolutions = xmldoc.getElementsByTagName("Size")
    r_list = []
    for i in resolutions:
        r_list.append(int(i.attributes["resolution"].value))
    resolution = min(r_list)
    return {
        "eo:cloud_cover": cloud,
        "sentinel:datastrip_id": datastrip_id,
        "sentinel:sentinel_tile_id": tile_id,
        "eo:sun_azimuth": solar_azimuth,
        "eo:sun_elevation": solar_zenith,
        "eo:gsd": resolution,
        "odc:processing_datetime": processed_time,
    }


def process_mtd_ds(contents: str) -> Dict:
    xmldoc = minidom.parseString(contents)

    reception_station = xmldoc.getElementsByTagName("RECEPTION_STATION")[
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
        "sentinel:reception_station": reception_station,
        "sentinel:processing_center": processing_center,
        "eo:gsd": resolution,
    }


def process_mtd_tl(contents: str) -> Dict:
    xmldoc = minidom.parseString(contents)

    sun_azimuth = xmldoc.getElementsByTagName("AZIMUTH_ANGLE")[0].firstChild.data
    sun_elevation = xmldoc.getElementsByTagName("ZENITH_ANGLE")[0].firstChild.data
    datastrip_id = xmldoc.getElementsByTagName("DATASTRIP_ID")[0].firstChild.data

    return {
        "eo:sun_azimuth": sun_azimuth,
        "eo:sun_elevation": sun_elevation,
        "sentinel:datastrip_id": datastrip_id,
    }


def process_mtd_msil1c(contents: str) -> Dict:
    xmldoc = minidom.parseString(contents)

    product_uri = xmldoc.getElementsByTagName("PRODUCT_URI")[0].firstChild.data
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
    region_code = product_uri.split("_")[5][1:]

    return {
        "eo:platform": platform,
        "sat:relative_orbit": int(orbit),
        "sat:orbit_state": orbit_direction.lower(),
        "sentinel:datatake_type": datatake_type,
        "odc:processing_datetime": processing_datetime,
        "sentinel:processing_baseline": processing_baseline,
        "datetime": datetime,
        "eo:cloud_cover": cloud_cover,
        "odc:region_code": region_code,
    }


def prepare_and_write(
    dataset: Path,
    dataset_document: Path,
) -> Tuple[uuid.UUID, Path]:
    # Process esa dataset
    if "zip" in str(dataset):
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

                p.add_accessory_file("metadata:mtd_ds", mtd_ds_zip_path)
                p.add_accessory_file("metadata:mtd_tl", mtd_tl_zip_path)
                p.add_accessory_file("metadata:mtd_msil1c", mtd_msil1c_zip_path)

                return p.done()

    # process sinergise dataset
    else:
        directory = [f for f in listdir(dataset) if isfile(join(dataset, f))]

        # Get file paths for sinergise metadata files
        product_info_path = dataset / "productInfo.json"
        metadata_xml_path = dataset / "metadata.xml"

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

            for ds in directory:
                if ".jp2" in ds and "preview" not in ds and "TCI" not in ds:
                    band = ds.replace(".jp2", "").replace("B", "")
                    name = SENTINEL_MSI_BAND_ALIASES[band]
                    path = Path(dataset) / ds
                    p.note_measurement(path=path, name=name)

            p.add_accessory_file("metadata:product_info", product_info_path)
            p.add_accessory_file("metadata:sinergise_metadata", metadata_xml_path)
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
