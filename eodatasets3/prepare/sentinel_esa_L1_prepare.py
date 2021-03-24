"""
Prepare eo3 metadata for Sentinel-2 Level 1C data produced by ESA
"""

import os
import zipfile

from pathlib import Path
from xml.dom import minidom
from os import listdir
from os.path import isfile, join
import uuid
import click
from typing import Dict, Tuple
from eodatasets3 import DatasetAssembler
from eodatasets3.ui import PathPath


HARDCODED = {
    "file_format": "JPEG2000",
    "instrument": "MSI",
    "producer": "esa.int",
    "product_family": "level1",
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


def process_MTD_DS(MTD_DS_zip_path, zip_object):
    xmldoc = minidom.parseString(zip_object.read(MTD_DS_zip_path))

    reception_station = xmldoc.getElementsByTagName('RECEPTION_STATION')[0].firstChild.data
    downlink_orbit_number = xmldoc.getElementsByTagName('DOWNLINK_ORBIT_NUMBER')[0].firstChild.data
    processing_center = xmldoc.getElementsByTagName('PROCESSING_CENTER')[0].firstChild.data
    
    resolutions = xmldoc.getElementsByTagName('RESOLUTION')
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


def process_MTD_TL(MTD_TL_zip_path, zip_object):
    xmldoc = minidom.parseString(zip_object.read(MTD_TL_zip_path))

    sun_azimuth = xmldoc.getElementsByTagName('AZIMUTH_ANGLE')[0].firstChild.data
    sun_elevation = xmldoc.getElementsByTagName('ZENITH_ANGLE')[0].firstChild.data

    return {
        "sun_azimuth": sun_azimuth,
        "sun_elevation": sun_elevation,
    }

def process_format_correctness(FORMAT_CORRECTNESS_zip_path, zip_object):
    xmldoc = minidom.parseString(zip_object.read(FORMAT_CORRECTNESS_zip_path))

    source_system = xmldoc.getElementsByTagName('System')[0].firstChild.data
    software_version = xmldoc.getElementsByTagName('Creator_Version')[0].firstChild.data

    return {
        "source_system": source_system,
        "software_version": software_version,
    }


def process_MTD_MSIL1C(MTD_MSIL1C_zip_path, zip_object):
    xmldoc = minidom.parseString(zip_object.read(MTD_MSIL1C_zip_path))

    data_type = xmldoc.getElementsByTagName('PROCESSING_LEVEL')[0].firstChild.data
    datastrip_id = xmldoc.getElementsByTagName('PRODUCT_URI')[0].firstChild.data
    product_type = xmldoc.getElementsByTagName('PRODUCT_TYPE')[0].firstChild.data
    platform = xmldoc.getElementsByTagName('SPACECRAFT_NAME')[0].firstChild.data
    orbit = xmldoc.getElementsByTagName('SENSING_ORBIT_NUMBER')[0].firstChild.data
    orbit_direction = xmldoc.getElementsByTagName('SENSING_ORBIT_DIRECTION')[0].firstChild.data
    datatake_type = xmldoc.getElementsByTagName('DATATAKE_TYPE')[0].firstChild.data
    processing_datetime = xmldoc.getElementsByTagName('GENERATION_TIME')[0].firstChild.data
    processing_baseline = xmldoc.getElementsByTagName('PROCESSING_BASELINE')[0].firstChild.data
    datetime = xmldoc.getElementsByTagName('PRODUCT_START_TIME')[0].firstChild.data
    cloud_cover = xmldoc.getElementsByTagName('Cloud_Coverage_Assessment')[0].firstChild.data
    region_code = datastrip_id.split('_')[5][1:]

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


def prepare_and_write(
        dataset: Path,
        dataset_document: Path,
    ) -> Tuple[uuid.UUID, Path]:

    with zipfile.ZipFile(str(dataset), 'r') as z:
        MTD_DS_zip_path = [s for s in z.namelist() if 'MTD_DS.xml' in s][0]
        MTD_TL_zip_path = [s for s in z.namelist() if 'MTD_TL.xml' in s][0]
        MTD_MSIL1C_zip_path = [s for s in z.namelist() if 'MTD_MSIL1C.xml' in s][0]
        FORMAT_CORRECTNESS_zip_path = [s for s in z.namelist() if 'FORMAT_CORRECTNESS.xml' in s][0]    

        MTD_DS = process_MTD_DS(MTD_DS_zip_path, z)
        MTD_TL = process_MTD_TL(MTD_TL_zip_path, z)
        FORMAT_CORRECTNESS = process_format_correctness(FORMAT_CORRECTNESS_zip_path, z)
        MTD_MSIL1C = process_MTD_MSIL1C(MTD_MSIL1C_zip_path, z)

        with DatasetAssembler(
            metadata_path=dataset_document, 
            dataset_location=Path('zip:'+str(dataset)+'!'),
            allow_absolute_paths=False
        ) as p:
            p.datetime = MTD_MSIL1C["datetime"]
            p.properties["eo:instrument"] = HARDCODED["instrument"]
            p.properties["eo:platform"] = MTD_MSIL1C["platform"] 
            p.properties["odc:processing_datetime"] = MTD_MSIL1C["processing_datetime"]
            p.properties["odc:dataset_version"] = f"1.0.{p.processed:%Y%m%d}"
            p.properties["odc:producer"] = HARDCODED["producer"]
            p.properties["odc:product_family"] = HARDCODED["product_family"]
            p.properties["eo:sun_elevation"] = MTD_TL["sun_elevation"]
            p.properties["eo:sun_azimuth"] = MTD_TL["sun_azimuth"]
            p.properties["eo:gsd"] = MTD_DS["resolution"]
            p.properties["eo:cloud_cover"] = MTD_MSIL1C["cloud_cover"]
            p.properties["odc:file_format"] = HARDCODED["file_format"]
            p.properties["odc:region_code"] = MTD_MSIL1C["region_code"]
            p.properties["sentinel:data_type"] = MTD_MSIL1C["data_type"]
            p.properties["sentinel:product_type"] = MTD_MSIL1C["product_type"]
            p.properties["sentinel:software_version"] = FORMAT_CORRECTNESS["software_version"]
            p.properties["sentinel:source_system"] = FORMAT_CORRECTNESS["source_system"]
            p.properties["sentinel:datastrip_id"] = MTD_MSIL1C["datastrip_id"]
            p.properties["sentinel:downlink_orbit_number"] = MTD_DS["downlink_orbit_number"]
            p.properties["sentinel:reception_station"] = MTD_DS["reception_station"]
            p.properties["sentinel:processing_center"] = MTD_DS["processing_center"]
            p.properties["sentinel:orbit"] = MTD_MSIL1C["orbit"]
            p.properties["sentinel:orbit_direction"] = MTD_MSIL1C["orbit_direction"]
            p.properties["sentinel:datatake_type"] = MTD_MSIL1C["datatake_type"]
            p.properties["sentinel:processing_baseline"] = MTD_MSIL1C["processing_baseline"]

            for file in z.namelist():
                # T55HFA_20201011T000249_B01.jp2
                if ".jp2" in file and "TCI" not in file:
                    band = file.split("_")[len(file.split("_"))-1].replace(".jp2", "").replace("B", "")
                    name = SENTINEL_MSI_BAND_ALIASES[band]
                    #path = 'zip:%s!%s' % (str(dataset), str(file))
                    p.note_measurement(path=file, name=name, relative_to_dataset_location=True)
   
            return p.done()


@click.command(help=__doc__)
@click.option(
    "--dataset",
    type=PathPath(),
    required=True,
    help="Path to ESA zipped dataset",
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
