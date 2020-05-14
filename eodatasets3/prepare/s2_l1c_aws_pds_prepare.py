#!/usr/bin/env python3
# coding=utf-8
"""
Preparation code for Sentinel-2 L1C AWS PDS
Generates metadata documents for the Sentinel-2 Level-1C dataset available on aws:

    http://sentinel-pds.s3-website.eu-central-1.amazonaws.com/

in a format to be read by datacube.

    https://www.opendatacube.org/

example usage:

    eo3-prepare s2-awspds\
 S2A_OPER_PRD_MSIL1C_PDMC_20161017T123606_R018_V20161016T034742_20161016T034739\
 --output . --no-checksum
"""
from __future__ import absolute_import

import hashlib
import json
import logging
import os
import sys
import uuid
from pathlib import Path
from typing import Tuple, List, Dict, Optional
from xml.etree import ElementTree

import click
import rasterio
import rasterio.features
import shapely.affinity
import shapely.geometry
import shapely.ops
from rasterio.errors import RasterioIOError
from ruamel import yaml

try:
    from checksumdir import dirhash
    from osgeo import osr, gdal
except ImportError:
    sys.stderr.write(
        "eodatasets3 has not been installed with the ancillary extras. \n"
        "    Try `pip install eodatasets3[ancillary]\n"
    )
    raise

# Before gdal 3, coordinate transforms were reversed.
# https://github.com/OSGeo/gdal/issues/1546
_NEWER_GDAL_CONVENTIONS = int(gdal.__version__.split(".")[0]) >= 3

os.environ["CPL_ZIP_ENCODING"] = "UTF-8"
SRC_BUCKET = "sentinel-s2-l1c"
SRC_REGION = "eu-central-1"

S3_WEBSITE_PATTERN = "http://{src_bucket}.s3-website.{src_region}.amazonaws.com"

PLATFORM_MAPPING = {"Sentinel-2A": "SENTINEL_2A", "Sentinel-2B": "SENTINEL_2B"}


def safe_valid_region(images, mask_value=None):
    """
    Safely return valid data region for input images based on mask value and input image path
    """
    try:
        return valid_region(images, mask_value)
    except (OSError, RasterioIOError):
        return None


def valid_region(images, mask_value=None):
    """
    Return valid data region for input images based on mask value and input image path
    """
    mask = None
    for fname in images:
        logging.info("Valid regions for %s", fname)
        # ensure formats match
        with rasterio.open(str(fname), "r") as dataset:
            transform = dataset.transform
            img = dataset.read(1)
            if mask_value is not None:
                new_mask = img & mask_value == mask_value
            else:
                new_mask = img != 0
            if mask is None:
                mask = new_mask
            else:
                mask |= new_mask
    shapes = rasterio.features.shapes(mask.astype("uint8"), mask=mask)
    shape = shapely.ops.unary_union(
        [shapely.geometry.shape(shape) for shape, val in shapes if val == 1]
    )

    # convex hull
    geom = shape.convex_hull
    # buffer by 1 pixel
    geom = geom.buffer(1, join_style=3, cap_style=3)
    # simplify with 1 pixel radius
    geom = geom.simplify(1)
    # intersect with image bounding box
    geom = geom.intersection(shapely.geometry.box(0, 0, mask.shape[1], mask.shape[0]))
    # transform from pixel space into CRS space
    geom = shapely.affinity.affine_transform(
        geom,
        (
            transform.a,
            transform.b,
            transform.d,
            transform.e,
            transform.xoff,
            transform.yoff,
        ),
    )
    return geom


def get_geo_ref_points(root):
    """
    Returns dictionary of bounding coordinates from given xml
    """
    nrows = int(root.findall('./*/Tile_Geocoding/Size[@resolution="10"]/NROWS')[0].text)
    ncols = int(root.findall('./*/Tile_Geocoding/Size[@resolution="10"]/NCOLS')[0].text)
    ulx = int(
        root.findall('./*/Tile_Geocoding/Geoposition[@resolution="10"]/ULX')[0].text
    )
    uly = int(
        root.findall('./*/Tile_Geocoding/Geoposition[@resolution="10"]/ULY')[0].text
    )
    xdim = int(
        root.findall('./*/Tile_Geocoding/Geoposition[@resolution="10"]/XDIM')[0].text
    )
    ydim = int(
        root.findall('./*/Tile_Geocoding/Geoposition[@resolution="10"]/YDIM')[0].text
    )
    return {
        "ul": {"x": ulx, "y": uly},
        "ur": {"x": ulx + ncols * abs(xdim), "y": uly},
        "ll": {"x": ulx, "y": uly - nrows * abs(ydim)},
        "lr": {"x": ulx + ncols * abs(xdim), "y": uly - nrows * abs(ydim)},
    }


def get_coords(geo_ref_points, spatial_ref: osr.SpatialReference):
    """
    Returns transformed coordinates in latitude and longitude from input
    reference points and spatial reference
    """
    if _NEWER_GDAL_CONVENTIONS:
        spatial_ref.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)

    t = osr.CoordinateTransformation(spatial_ref, spatial_ref.CloneGeogCS())

    def transform(p):
        lon, lat, z = t.TransformPoint(p["x"], p["y"])
        return {"lon": lon, "lat": lat}

    return {key: transform(p) for key, p in geo_ref_points.items()}


def get_datastrip_info(path):
    # type: (Path) -> Tuple[str, uuid.UUID]
    """get_datastrip_info returns information parsed from productInfo.json

    :param path: path to root of tile collection
    :returns: url to the datastrip metadata, a deterministic uuid for level-1 dataset
    """
    with (path / "productInfo.json").open() as fd:
        product_info = json.load(fd)
        datastrip_metadata = (S3_WEBSITE_PATTERN + "/#{datastrip_path}").format(
            src_bucket=SRC_BUCKET,
            src_region=SRC_REGION,
            datastrip_path=product_info["tiles"][0]["datastrip"]["path"],
        )
        # Deterministic uuid
        persisted_uuid = uuid.uuid5(
            uuid.NAMESPACE_URL, datastrip_metadata + "#" + product_info["id"]
        )
    return (datastrip_metadata, persisted_uuid)


def get_tile_info(path):
    # type: (Path) -> str
    """get_tile_info: returns information parsed from the tileInfo.json

    :param path: path to the root of the tile collection
    :returns: prefix to tile collection
    """
    with (path / "tileInfo.json").open() as fd:
        tile_info = json.load(fd)
        return tile_info.get("path")


def prepare_dataset(path, datastrip_path=None):
    # type: (Path, Optional[Path]) -> List[Dict]
    """
    :param path: Path to the root of the granule/tile data
    :param datastrip_path: Path to the root of the datastrip metadata

    Returns yaml content based on content found at input file path

    :param path: Path to the root of the granule
    :param datastrip_path: Path to the root of the datastrip metadata

    returns a list of dictionaries containing the metadata for a granule
    """
    root = ElementTree.parse(path / "metadata.xml").getroot()
    # Set the path to the datastrip metadata to be rooted at the granule
    # for backwards compatibility
    if not datastrip_path:
        datastrip_path = path / "datastrip"
    root_datastrip = ElementTree.parse(datastrip_path / "metadata.xml").getroot()
    size_bytes = sum(os.path.getsize(p) for p in os.scandir(path))
    checksum_sha1 = dirhash(path.parent, "sha1")

    # Get the datastrip metadata url and generated a deterministic src uuid
    datastrip_metadata, persisted_uuid = get_datastrip_info(path)

    # Get the tile path for src information
    tile_path = get_tile_info(path)

    datatake_id = root_datastrip.findall("./*/Datatake_Info")[0].attrib
    platform = root_datastrip.findall(".//*/SPACECRAFT_NAME")[0].text

    # Update to GA's platform naming convention
    if platform in PLATFORM_MAPPING:
        platform = PLATFORM_MAPPING[platform]

    datatake_type = root_datastrip.findall(".//*/DATATAKE_TYPE")[0].text
    datatake_sensing_start = root_datastrip.findall(".//*/DATATAKE_SENSING_START")[
        0
    ].text
    orbit = root_datastrip.findall(".//*/SENSING_ORBIT_NUMBER")[0].text
    orbit_direction = root_datastrip.findall(".//*/SENSING_ORBIT_DIRECTION")[0].text
    product_format = {"name": "s2_aws_pds"}

    reflectance_conversion = root_datastrip.findall(".//*/Reflectance_Conversion/U")[
        0
    ].text
    solar_irradiance = []
    for irradiance in root.iter("SOLAR_IRRADIANCE"):
        band_irradiance = irradiance.attrib
        band_irradiance["value"] = irradiance.text
        solar_irradiance.append(band_irradiance)
    cloud_coverage = float(root.findall(".//*/CLOUDY_PIXEL_PERCENTAGE")[0].text)
    degraded_anc_data_percentage = float(
        root_datastrip.findall(".//*/DEGRADED_ANC_DATA_PERCENTAGE")[0].text
    )
    degraded_msi_data_percentage = float(
        root.findall(".//*/DEGRADED_MSI_DATA_PERCENTAGE")[0].text
    )

    sensor_quality_flag = (
        ElementTree.parse(path / "qi" / "SENSOR_QUALITY.xml")
        .getroot()
        .find(".//*/{http://gs2.esa.int/DATA_STRUCTURE/olqcReport}report")
        .attrib.get("globalStatus")
    )

    geometric_quality_flag = (
        ElementTree.parse(path / "qi" / "GEOMETRIC_QUALITY.xml")
        .getroot()
        .find(".//*/{http://gs2.esa.int/DATA_STRUCTURE/olqcReport}report")
        .attrib.get("globalStatus")
    )

    general_quality_flag = (
        ElementTree.parse(path / "qi" / "GENERAL_QUALITY.xml")
        .getroot()
        .find(".//*/{http://gs2.esa.int/DATA_STRUCTURE/olqcReport}report")
        .attrib.get("globalStatus")
    )

    format_quality_flag = (
        ElementTree.parse(path / "qi" / "FORMAT_CORRECTNESS.xml")
        .getroot()
        .find(".//*/{http://gs2.esa.int/DATA_STRUCTURE/olqcReport}report")
        .attrib.get("globalStatus")
    )

    radiometric_quality_flag = (
        ElementTree.parse(path / "datastrip" / "qi" / "RADIOMETRIC_QUALITY_report.xml")
        .getroot()
        .find(".//*/{http://gs2.esa.int/DATA_STRUCTURE/olqcReport}report")
        .attrib.get("globalStatus")
    )

    _imgs_in_directory = [p.name for p in path.glob("B??.jp2")]
    _img_ids = [
        "B01.jp2",
        "B02.jp2",
        "B03.jp2",
        "B04.jp2",
        "B05.jp2",
        "B06.jp2",
        "B07.jp2",
        "B08.jp2",
        "B8A.jp2",
        "B09.jp2",
        "B10.jp2",
        "B11.jp2",
        "B12.jp2",
        "TCI.jp2",
    ]

    # Use the filename without extension
    images = [img_name for img_name in _img_ids if img_name in _imgs_in_directory]

    documents = []

    # Point where original packaging script diverges for multi-granule workflow
    # Not required for Zip method - uses granule metadata
    img_data_path = path

    sensing_time = root.findall("./*/SENSING_TIME")[0].text
    images_sixty_list = []
    sixty_list = ["B01.jp2", "B09.jp2", "B10.jp2"]
    for image in images:
        for item in sixty_list:
            if item in image:
                images_sixty_list.append(img_data_path / image)
    tile_id = root.findall("./*/TILE_ID")[0].text
    mgrs_reference = tile_id.split("_")[9]
    datastrip_id = root.findall("./*/DATASTRIP_ID")[0].text
    downlink_priority = root.findall("./*/DOWNLINK_PRIORITY")[0].text
    sensing_time = root.findall("./*/SENSING_TIME")[0].text
    station = root.findall("./*/Archiving_Info/ARCHIVING_CENTRE")[0].text
    archiving_time = root.findall("./*/Archiving_Info/ARCHIVING_TIME")[0].text
    sun_zenith_angle = float(
        root.findall("./*/Tile_Angles/Mean_Sun_Angle/ZENITH_ANGLE")[0].text
    )
    sun_azimuth_angle = float(
        root.findall("./*/Tile_Angles/Mean_Sun_Angle/AZIMUTH_ANGLE")[0].text
    )
    viewing_zenith_azimuth_angle = []
    for viewing_incidence in root.iter("Mean_Viewing_Incidence_Angle"):
        view_incidence = viewing_incidence.attrib
        zenith_value = float(viewing_incidence.find("ZENITH_ANGLE").text)
        azimuth_value = float(viewing_incidence.find("AZIMUTH_ANGLE").text)
        view_incidence.update(
            {
                "unit": "degree",
                "measurement": {
                    "zenith": {"value": zenith_value},
                    "azimuth": {"value": azimuth_value},
                },
            }
        )
        viewing_zenith_azimuth_angle.append(view_incidence)
    cs_code = root.findall("./*/Tile_Geocoding/HORIZONTAL_CS_CODE")[0].text
    spatial_ref = osr.SpatialReference()
    spatial_ref.SetFromUserInput(cs_code)
    geo_ref_points = get_geo_ref_points(root)
    img_dict = {}
    for image in images:
        img_path = "s3://{src_bucket}/{tile_path}/{image}".format(
            src_bucket=SRC_BUCKET, tile_path=tile_path, image=image
        )
        band_label = image[-7:-4]
        img_dict[band_label] = {"path": str(img_path), "layer": 1}

    documents.append(
        {
            "id": str(persisted_uuid),
            "processing_level": "Level-1C",
            "product_type": "level1",
            "creation_dt": root_datastrip.findall("./*/Processing_Info/UTC_DATE_TIME")[
                0
            ].text,
            "datatake_id": datatake_id,
            "datatake_type": datatake_type,
            "datatake_sensing_start": datatake_sensing_start,
            "orbit": orbit,
            "orbit_direction": orbit_direction,
            "size_bytes": size_bytes,
            "checksum_sha1": checksum_sha1,
            "platform": {"code": platform},
            "instrument": {"name": "MSI"},
            "product_format": product_format,
            "format": {"name": "JPEG2000"},
            "tile_id": tile_id,
            "datastrip_id": datastrip_id,
            "datastrip_metadata": datastrip_metadata,
            "downlink_priority": downlink_priority,
            "archiving_time": archiving_time,
            "acquisition": {"groundstation": {"code": station}},
            "extent": {
                "center_dt": sensing_time,
                "coord": get_coords(geo_ref_points, spatial_ref),
            },
            "grid_spatial": {
                "projection": {
                    "geo_ref_points": geo_ref_points,
                    "spatial_reference": cs_code,
                    "valid_data": {
                        "coordinates": shapely.geometry.mapping(
                            shapely.ops.unary_union(
                                [safe_valid_region(images_sixty_list)]
                            )
                        )["coordinates"],
                        "type": "Polygon",
                    },
                }
            },
            "image": {
                "tile_reference": mgrs_reference,
                "cloud_cover_percentage": cloud_coverage,
                "sun_azimuth": sun_azimuth_angle,
                "sun_elevation": sun_zenith_angle,
                "viewing_angles": viewing_zenith_azimuth_angle,
                "degraded_anc_data_percentage": degraded_anc_data_percentage,
                "degraded_msi_data_percentage": degraded_msi_data_percentage,
                "sensor_quality_flag": sensor_quality_flag,
                "geometric_quality_flag": geometric_quality_flag,
                "general_quality_flag": general_quality_flag,
                "format_quality_flag": format_quality_flag,
                "radiometric_quality_flag": radiometric_quality_flag,
                "reflectance_conversion": reflectance_conversion,
                "solar_irradiance": solar_irradiance,
                "bands": img_dict,
            },
            "lineage": {"source_datasets": {}},
        }
    )
    return documents


@click.command(help=__doc__)
@click.option(
    "--output",
    help="Write datasets into this directory",
    type=click.Path(exists=False, writable=True, dir_okay=True),
)
@click.argument(
    "datasets", type=click.Path(exists=True, readable=True, writable=False), nargs=-1
)
@click.option(
    "--checksum/--no-checksum",
    help="Checksum the input dataset to confirm match",
    default=False,
)
def main(output, datasets, checksum):
    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(message)s", level=logging.INFO
    )

    for dataset in datasets:
        (mode, ino, dev, nlink, uid, gid, size, atime, mtime, ctime) = os.stat(dataset)
        path = Path(dataset)
        if path.is_dir():
            path = Path(path.joinpath(path))
        elif path.suffix not in [".xml", ".zip"]:
            raise RuntimeError("want xml or zipped archive")
        logging.info("Processing %s", path)
        output_path = Path(output)
        yaml_path = output_path.joinpath(path.name + ".yaml")
        logging.info("Output %s", yaml_path)
        if os.path.exists(yaml_path):
            logging.info("Output already exists %s", yaml_path)
            with open(yaml_path) as f:
                if checksum:
                    logging.info("Running checksum comparison")
                    datamap = yaml.safe_load_all(f)
                    for data in datamap:
                        yaml_sha1 = data["checksum_sha1"]
                        checksum_sha1 = hashlib.sha1(
                            open(path, "rb").read()
                        ).hexdigest()
                    if checksum_sha1 == yaml_sha1:
                        logging.info("Dataset preparation already done...SKIPPING")
                        continue
                else:
                    logging.info("Dataset preparation already done...SKIPPING")
                    continue

        documents = prepare_dataset(path)
        if documents:
            logging.info("Writing %s dataset(s) into %s", len(documents), yaml_path)
            with open(yaml_path, "w") as stream:
                yaml.safe_dump_all(documents, stream)
        else:
            logging.info("No datasets discovered. Bye!")


if __name__ == "__main__":
    main()
