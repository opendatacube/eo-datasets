from datetime import datetime
from pathlib import Path
from xml.dom import minidom
import json
from os import listdir
from os.path import isfile, join
import sys

path = '/g/data/up71/projects/index-testing-wagl/repo/eo-datasets/eodatasets3'
sys.append(path)

from eodatasets3 import DatasetAssembler

output_yaml_path = Path("/home/547/awo547/s2_delete_me.yaml")
ds_path = Path("/home/547/awo547/sample_syn_s2/")
metadata_xml_path = "/home/547/awo547/sample_syn_s2/metadata.xml"
path = Path("/home/547/awo547/sample_syn_s2/B08.jp2")
product_path = Path("/home/547/awo547/sample_syn_s2/productInfo.json")
format_correctness_path = "/home/547/awo547/sample_syn_s2/qi/FORMAT_CORRECTNESS.xml"


def extract_metadata_from_product_info(product_path):
    fp = open(product_info)
    product = json.loads(fp.read())
    
    synergise_product_name = product['name']
    synergise_product_id = product['id']
    timestamp = product['tiles'][0]['timestamp']
    utm_zone = product['tiles'][0]['utmZone']
    latitude_band = product['tiles'][0]['latitudeBand']
    grid_square = product['tiles'][0]['gridSquare']
    region_code = '%s%s%s' % (utm_zone, latitude_band, grid_square)

    return {
            'synergise_product_name': synergise_product_name,
            'synergise_product_id': synergise_product_id,
            'timestamp': timestamp,
            'utm_zone': utm_zone,
            'latitude_band': latitude_band,
            'grid_square': grid_square,
            'region_code': region_code,
            }


def extract_metadata_from_metadata_xml(metadata_xml_path):
    xmldoc = minidom.parse(metadata_xml_path)

    cloud = float(xmldoc.getElementsByTagName('CLOUDY_PIXEL_PERCENTAGE')[0].firstChild.data)
    downlink_priority = xmldoc.getElementsByTagName('DOWNLINK_PRIORITY')[0].firstChild.data
    datastrip_id = xmldoc.getElementsByTagName('DATASTRIP_ID')[0].firstChild.data
    solar_azimuth = float(xmldoc.getElementsByTagName("Mean_Sun_Angle")[0].getElementsByTagName("ZENITH_ANGLE")[0].firstChild.data)
    solar_zenith = float(xmldoc.getElementsByTagName("Mean_Sun_Angle")[0].getElementsByTagName("AZIMUTH_ANGLE")[0].firstChild.data)

    resolutions = xmldoc.getElementsByTagName('Size')
    r_list = []
    for i in resolutions: r_list.append(int(i.attributes['resolution'].value))
    resolution = min(r_list)

    return {
            'cloud': cloud,
            'downlink_priority': downlink_priority,
            'datastrip_id': datastrip_id,
            'solar_azimuth': solar_azimuth,
            'solar_zenith': solar_zenith,
            'resolution': resolution,
            }


def extract_metadata_from_format_correctness(format_correctness_path):
    xmldoc = minidom.parse(format_correctness_path)

    source_system = xmldoc.getElementsByTagName('System')[0].firstChild.data
    creator_version = xmldoc.getElementsByTagName('Creator_Version')[0].firstChild.data
    creation_date = xmldoc.getElementsByTagName('Creation_Date')[0].firstChild.data
    datastrip_metadata = xmldoc.getElementsByTagName('File_Name')[0].firstChild.data

    return {
            'source_system': source_system,
            'creator_version': creator_version,
            'creation_date': creation_date,
            'datastrip_metadata': datastrip_metadata,
           }


def prepare_and_write(output_yaml_path, ds_path, product_info, metadata_xml, format_correctness):
    with DatasetAssembler(
        metadata_path=output_yaml_path,
        dataset_location=ds_path,
    ) as p:
        p.datetime = timestamp # datetime(2017, 4, 5, 11, 17, 36) 2016-06-28 00:02:28.624635Z
        p.properties["eo:instrument"] = 'MSI'
        p.properties["eo:platform"] = 'sentinel-2a'
        p.properties["odc:dataset_version"] = "1.0.0"
        p.properties["odc:producer"] = 'ga.gov.au'
        p.properties["odc:product_family"] = "level1"
        p.properties["odc:processing_datetime"] = creation_date.split('=')[1].replace('T', ' ')
        p.properties["eo:sun_elevation"] = solar_zenith
        p.properties["eo:sun_azimuth"] = solar_azimuth
        p.properties["eo:gsd"] = resolution
        p.properties["eo:cloud_cover"] = cloud
        p.properties["sentinel:sinergise_product_name"] = synergise_product_name
        p.properties["sentinel:sinergise_product_id"] = synergise_product_id
        p.properties["odc:file_format"] = 'JPEG2000'
        p.properties["odc:region_code"] = region_code
        p.properties["sentinel:data_type"] = "Level-1C"
        p.properties["sentinel:product_type"] = "S2MSI1C"
        p.properties["sentinel:software_version"] = creator_version
        p.properties["sentinel:source_system"] = source_system
        p.properties["sentinel:datastrip_metadata"] = datastrip_metadata
        p.properties["sentinel:downlink_priority"] = downlink_priority
        p.properties["sentinel:datastrip_id"] = datastrip_id
   
        directory = [f for f in listdir(ds_path) if isfile(join(ds_path, f))]

        for dataset in directory:
            if ('.jp2' in dataset and 'preview' not in dataset and 'TCI' not in dataset):
                name = dataset.replace('.jp2', '').lower()
                path = ds_path / dataset
                print(name + ': ' + str(path))
                p.note_measurement(path=path, name=name)
   
        p.done()


main():
    product_info = extract_metadata_from_product_info(product_path)
    metadata_xml = extract_metadata_from_metadata_xml(metadata_xml_path)
    format_correctness = extract_metadata_from_format_correctness(format_correctness_path)

    prepare_and_write(output_yaml_path, ds_path, product_info, metadata_xml, format_correctness)

if __name__ == "__main__":
    main()
