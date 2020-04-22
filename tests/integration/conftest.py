import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, Callable

import pytest

from eodatasets3 import serialise
from eodatasets3.model import DatasetDoc
from eodatasets3.prepare.landsat_l1_prepare import normalise_nci_symlinks

L71GT_TARBALL_PATH: Path = Path(
    __file__
).parent / "data" / "LE07_L1TP_104078_20130429_20161124_01_T1.tar"

L5_TARBALL_PATH: Path = Path(
    __file__
).parent / "data" / "LT05_L1TP_090085_19970406_20161231_01_T1.tar.gz"

L8_INPUT_PATH: Path = Path(
    __file__
).parent / "data" / "LC08_L1TP_090084_20160121_20170405_01_T1"

LS8_TELEMETRY_PATH: Path = Path(
    __file__
).parent / "data" / "LS8_OLITIRS_STD-MD_P00_LC80840720742017365LGN00_084_072-074_20180101T004644Z20180101T004824_1"


def path_offset(base: Path, offset: str):
    return str(normalise_nci_symlinks(base.absolute().joinpath(offset)))


def tar_offset(tar: Path, offset: str):
    return "tar:" + str(normalise_nci_symlinks(tar.absolute())) + "!" + offset


def relative_offset(base, offset):
    return offset


@pytest.fixture
def l1_ls8_folder(tmp_path: Path) -> Path:
    return _make_copy(L8_INPUT_PATH, tmp_path)


@pytest.fixture
def l1_ls8_metadata_path(l1_ls8_folder: Path, l1_ls8_dataset: DatasetDoc) -> Path:
    path = l1_ls8_folder / f"{l1_ls8_dataset.label}.odc-metadata.yaml"
    serialise.dump_yaml(path, serialise.to_formatted_doc(l1_ls8_dataset))
    return path


@pytest.fixture
def l1_ls8_dataset_path(l1_ls8_folder: Path, l1_ls8_metadata_path: Path) -> Path:
    """
    A prepared L1 dataset with an EO3 metadata file.
    """
    return l1_ls8_folder


@pytest.fixture
def l1_ls7_tarball(tmp_path: Path) -> Path:
    return _make_copy(L71GT_TARBALL_PATH, tmp_path)


@pytest.fixture
def l1_ls5_tarball(tmp_path: Path) -> Path:
    return _make_copy(L5_TARBALL_PATH, tmp_path)


def _make_copy(input_path, tmp_path):
    our_input = tmp_path / input_path.name
    if input_path.is_file():
        shutil.copy(input_path, our_input)
    else:
        shutil.copytree(input_path, our_input)
    return our_input


@pytest.fixture
def l1_ls8_dataset(l1_ls8_folder_md_expected: Dict) -> DatasetDoc:
    return serialise.from_doc(l1_ls8_folder_md_expected)


@pytest.fixture
def l1_ls8_folder_md_expected(l1_ls8_folder) -> Dict:
    return expected_l1_ls8_folder(l1_ls8_folder, relative_offset)


@pytest.fixture
def l1_ls8_ga_expected(l1_ls8_folder) -> Dict:
    return expected_l1_ls8_folder(
        l1_ls8_folder,
        relative_offset,
        organisation="ga.gov.au",
        collection="3",
        # the id in the ls8_telemetry_path fixture
        lineage={"satellite_telemetry_data": ["30841328-89c2-4693-8802-a3560a6cf67a"]},
    )


@pytest.fixture
def l1_ls8_folder_md_expected_absolute(l1_ls8_folder) -> Dict:
    return expected_l1_ls8_folder(l1_ls8_folder, path_offset)


@pytest.fixture
def ls8_telemetry_path(tmp_path: Path) -> Path:
    """Telemetry data with old-style ODC metadata"""
    return _make_copy(LS8_TELEMETRY_PATH, tmp_path)


@pytest.fixture(params=("ls5", "ls7", "ls8"))
def example_metadata(
    request,
    l1_ls5_tarball_md_expected: Dict,
    l1_ls7_tarball_md_expected: Dict,
    l1_ls8_folder_md_expected: Dict,
):
    """
    Test against arbitrary valid eo3 documents.
    """
    which = request.param
    if which == "ls5":
        return l1_ls5_tarball_md_expected
    elif which == "ls7":
        return l1_ls7_tarball_md_expected
    elif which == "ls8":
        return l1_ls8_folder_md_expected
    assert False


def expected_l1_ls8_folder(
    l1_ls8_folder: Path,
    offset: Callable[[Path, str], str] = relative_offset,
    organisation="usgs.gov",
    collection="1",
    lineage=None,
):
    org_code = organisation.split(".")[0]
    product_name = f"{org_code}_ls8c_level1_{collection}"
    return {
        "$schema": "https://schemas.opendatacube.org/dataset",
        "id": "a780754e-a884-58a7-9ac0-df518a67f59d",
        "label": f"{product_name}-0-20170405_090084_2016-01-21",
        "product": {
            "name": product_name,
            "href": f"https://collections.dea.ga.gov.au/product/{product_name}",
        },
        "properties": {
            "datetime": datetime(2016, 1, 21, 23, 50, 23, 54435),
            # The minor version comes from the processing date (as used in filenames to distinguish reprocesses).
            "odc:dataset_version": f"{collection}.0.20170405",
            "odc:file_format": "GeoTIFF",
            "odc:processing_datetime": datetime(2017, 4, 5, 11, 17, 36),
            "odc:producer": organisation,
            "odc:product_family": "level1",
            "odc:region_code": "090084",
            "eo:cloud_cover": 93.22,
            "eo:gsd": 15.0,
            "eo:instrument": "OLI_TIRS",
            "eo:platform": "landsat-8",
            "eo:sun_azimuth": 74.007_443_8,
            "eo:sun_elevation": 55.486_483,
            "landsat:collection_category": "T1",
            "landsat:collection_number": 1,
            "landsat:data_type": "L1TP",
            "landsat:geometric_rmse_model_x": 4.593,
            "landsat:geometric_rmse_model_y": 5.817,
            "landsat:ground_control_points_model": 66,
            "landsat:ground_control_points_version": 4,
            "landsat:landsat_product_id": "LC08_L1TP_090084_20160121_20170405_01_T1",
            "landsat:landsat_scene_id": "LC80900842016021LGN02",
            "landsat:processing_software_version": "LPGS_2.7.0",
            "landsat:station_id": "LGN",
            "landsat:wrs_path": 90,
            "landsat:wrs_row": 84,
        },
        "crs": "epsg:32655",
        "geometry": {
            "coordinates": (
                (
                    (879_315.0, -3_714_585.0),
                    (879_315.0, -3_953_115.0),
                    (641_985.0, -3_953_115.0),
                    (641_985.0, -3_714_585.0),
                    (879_315.0, -3_714_585.0),
                ),
            ),
            "type": "Polygon",
        },
        "grids": {
            "default": {
                "shape": (60, 60),
                "transform": (
                    3955.5,
                    0.0,
                    641_985.0,
                    0.0,
                    -3975.500_000_000_000_5,
                    -3_714_585.0,
                    0.0,
                    0.0,
                    1.0,
                ),
            },
            "panchromatic": {
                "shape": (60, 60),
                "transform": (
                    3955.25,
                    0.0,
                    641_992.5,
                    0.0,
                    -3975.25,
                    -3_714_592.5,
                    0.0,
                    0.0,
                    1.0,
                ),
            },
        },
        "measurements": {
            "coastal_aerosol": {
                "path": offset(
                    l1_ls8_folder, "LC08_L1TP_090084_20160121_20170405_01_T1_B1.TIF"
                )
            },
            "blue": {
                "path": offset(
                    l1_ls8_folder, "LC08_L1TP_090084_20160121_20170405_01_T1_B2.TIF"
                )
            },
            "green": {
                "path": offset(
                    l1_ls8_folder, "LC08_L1TP_090084_20160121_20170405_01_T1_B3.TIF"
                )
            },
            "red": {
                "path": offset(
                    l1_ls8_folder, "LC08_L1TP_090084_20160121_20170405_01_T1_B4.TIF"
                )
            },
            "nir": {
                "path": offset(
                    l1_ls8_folder, "LC08_L1TP_090084_20160121_20170405_01_T1_B5.TIF"
                )
            },
            "swir_1": {
                "path": offset(
                    l1_ls8_folder, "LC08_L1TP_090084_20160121_20170405_01_T1_B6.TIF"
                )
            },
            "swir_2": {
                "path": offset(
                    l1_ls8_folder, "LC08_L1TP_090084_20160121_20170405_01_T1_B7.TIF"
                )
            },
            "panchromatic": {
                "grid": "panchromatic",
                "path": offset(
                    l1_ls8_folder, "LC08_L1TP_090084_20160121_20170405_01_T1_B8.TIF"
                ),
            },
            "cirrus": {
                "path": offset(
                    l1_ls8_folder, "LC08_L1TP_090084_20160121_20170405_01_T1_B9.TIF"
                )
            },
            "lwir_1": {
                "path": offset(
                    l1_ls8_folder, "LC08_L1TP_090084_20160121_20170405_01_T1_B10.TIF"
                )
            },
            "lwir_2": {
                "path": offset(
                    l1_ls8_folder, "LC08_L1TP_090084_20160121_20170405_01_T1_B11.TIF"
                )
            },
            "quality": {
                "path": offset(
                    l1_ls8_folder, "LC08_L1TP_090084_20160121_20170405_01_T1_BQA.TIF"
                )
            },
        },
        "accessories": {
            "metadata:landsat_mtl": {
                "path": "LC08_L1TP_090084_20160121_20170405_01_T1_MTL.txt"
            }
        },
        "lineage": lineage or {},
    }


@pytest.fixture
def l1_ls7_tarball_md_expected(
    l1_ls7_tarball, offset: Callable[[Path, str], str] = relative_offset
) -> Dict:
    return {
        "$schema": "https://schemas.opendatacube.org/dataset",
        "id": "f23c5fa2-3321-5be9-9872-2be73fee12a6",
        "label": "usgs_ls7e_level1_1-0-20161124_104078_2013-04-29",
        "product": {
            "name": "usgs_ls7e_level1_1",
            "href": "https://collections.dea.ga.gov.au/product/usgs_ls7e_level1_1",
        },
        "crs": "epsg:32652",
        "properties": {
            "datetime": datetime(2013, 4, 29, 1, 10, 20, 336_104),
            "odc:dataset_version": "1.0.20161124",
            "odc:file_format": "GeoTIFF",
            "odc:processing_datetime": datetime(2016, 11, 24, 8, 26, 33),
            "odc:producer": "usgs.gov",
            "odc:product_family": "level1",
            "odc:region_code": "104078",
            "eo:cloud_cover": 0.0,
            "eo:gsd": 15.0,
            "eo:instrument": "ETM",
            "eo:platform": "landsat-7",
            "eo:sun_azimuth": 40.562_981_98,
            "eo:sun_elevation": 39.374_408_72,
            "landsat:collection_category": "T1",
            "landsat:collection_number": 1,
            "landsat:data_type": "L1TP",
            "landsat:ephemeris_type": "DEFINITIVE",
            "landsat:geometric_rmse_model_x": 2.752,
            "landsat:geometric_rmse_model_y": 3.115,
            "landsat:ground_control_points_model": 179,
            "landsat:ground_control_points_version": 4,
            "landsat:landsat_product_id": "LE07_L1TP_104078_20130429_20161124_01_T1",
            "landsat:landsat_scene_id": "LE71040782013119ASA00",
            "landsat:processing_software_version": "LPGS_12.8.2",
            "landsat:station_id": "ASA",
            "landsat:scan_gap_interpolation": 2.0,
            "landsat:wrs_path": 104,
            "landsat:wrs_row": 78,
        },
        "geometry": {
            "coordinates": (
                (
                    (770_115.0, -2_768_985.0),
                    (770_115.0, -2_981_715.0),
                    (525_285.0, -2_981_715.0),
                    (525_285.0, -2_768_985.0),
                    (770_115.0, -2_768_985.0),
                ),
            ),
            "type": "Polygon",
        },
        "grids": {
            "default": {
                "shape": (60, 60),
                "transform": (
                    4080.500_000_000_000_5,
                    0.0,
                    525_285.0,
                    0.0,
                    -3545.5,
                    -2_768_985.0,
                    0.0,
                    0.0,
                    1.0,
                ),
            },
            "panchromatic": {
                "shape": [60, 60],
                "transform": [
                    4080.25,
                    0.0,
                    525_292.5,
                    0.0,
                    -3545.25,
                    -2_768_992.5,
                    0.0,
                    0.0,
                    1.0,
                ],
            },
        },
        "measurements": {
            "blue": {
                "path": offset(
                    l1_ls7_tarball, "LE07_L1TP_104078_20130429_20161124_01_T1_B1.TIF"
                )
            },
            "green": {
                "path": offset(
                    l1_ls7_tarball, "LE07_L1TP_104078_20130429_20161124_01_T1_B2.TIF"
                )
            },
            "nir": {
                "path": offset(
                    l1_ls7_tarball, "LE07_L1TP_104078_20130429_20161124_01_T1_B4.TIF"
                )
            },
            "quality": {
                "path": offset(
                    l1_ls7_tarball, "LE07_L1TP_104078_20130429_20161124_01_T1_BQA.TIF"
                )
            },
            "red": {
                "path": offset(
                    l1_ls7_tarball, "LE07_L1TP_104078_20130429_20161124_01_T1_B3.TIF"
                )
            },
            "swir_1": {
                "path": offset(
                    l1_ls7_tarball, "LE07_L1TP_104078_20130429_20161124_01_T1_B5.TIF"
                )
            },
            "swir_2": {
                "path": offset(
                    l1_ls7_tarball, "LE07_L1TP_104078_20130429_20161124_01_T1_B7.TIF"
                )
            },
            "tir_1": {"path": "LE07_L1TP_104078_20130429_20161124_01_T1_B6_VCID_1.TIF"},
            "tir_2": {"path": "LE07_L1TP_104078_20130429_20161124_01_T1_B6_VCID_2.TIF"},
            "panchromatic": {
                "path": offset(
                    l1_ls7_tarball, "LE07_L1TP_104078_20130429_20161124_01_T1_B8.TIF"
                ),
                "grid": "panchromatic",
            },
        },
        "accessories": {
            "metadata:landsat_mtl": {
                "path": "LE07_L1TP_104078_20130429_20161124_01_T1_MTL.txt"
            }
        },
        "lineage": {},
    }


@pytest.fixture
def l1_ls5_tarball_md_expected(
    l1_ls5_tarball, offset: Callable[[Path, str], str] = relative_offset
) -> Dict:
    return {
        "$schema": "https://schemas.opendatacube.org/dataset",
        "id": "b0d31709-dda4-5a67-9fdf-3ae026a99a72",
        "label": "usgs_ls5t_level1_1-0-20161231_090085_1997-04-06",
        "product": {
            "name": "usgs_ls5t_level1_1",
            "href": "https://collections.dea.ga.gov.au/product/usgs_ls5t_level1_1",
        },
        "crs": "epsg:32655",
        "properties": {
            "datetime": datetime(1997, 4, 6, 23, 17, 43, 102_000),
            "odc:dataset_version": "1.0.20161231",
            "odc:file_format": "GeoTIFF",
            "odc:processing_datetime": datetime(2016, 12, 31, 15, 54, 58),
            "odc:producer": "usgs.gov",
            "odc:product_family": "level1",
            "odc:region_code": "090085",
            "eo:cloud_cover": 27.0,
            "eo:gsd": 30.0,
            "eo:instrument": "TM",
            "eo:platform": "landsat-5",
            "eo:sun_azimuth": 51.254_542_23,
            "eo:sun_elevation": 31.987_632_19,
            "landsat:collection_category": "T1",
            "landsat:collection_number": 1,
            "landsat:data_type": "L1TP",
            "landsat:ephemeris_type": "PREDICTIVE",
            "landsat:geometric_rmse_model_x": 3.036,
            "landsat:geometric_rmse_model_y": 3.025,
            "landsat:geometric_rmse_verify": 0.163,
            "landsat:ground_control_points_model": 161,
            "landsat:ground_control_points_verify": 1679,
            "landsat:ground_control_points_version": 4,
            "landsat:landsat_product_id": "LT05_L1TP_090085_19970406_20161231_01_T1",
            "landsat:landsat_scene_id": "LT50900851997096ASA00",
            "landsat:processing_software_version": "LPGS_12.8.2",
            "landsat:station_id": "ASA",
            "landsat:wrs_path": 90,
            "landsat:wrs_row": 85,
        },
        "geometry": {
            "coordinates": (
                (
                    (835_815.0, -3_881_685.0),
                    (593_385.0, -3_881_685.0),
                    (593_385.0, -4_101_015.0),
                    (835_815.0, -4_101_015.0),
                    (835_815.0, -3_881_685.0),
                ),
            ),
            "type": "Polygon",
        },
        "grids": {
            "default": {
                "shape": (60, 60),
                "transform": (
                    4040.5,
                    0.0,
                    593_385.0,
                    0.0,
                    -3655.5,
                    -3_881_685.0,
                    0.0,
                    0.0,
                    1.0,
                ),
            }
        },
        "measurements": {
            "blue": {
                "path": offset(
                    l1_ls5_tarball, "LT05_L1TP_090085_19970406_20161231_01_T1_B1.TIF"
                )
            },
            "green": {
                "path": offset(
                    l1_ls5_tarball, "LT05_L1TP_090085_19970406_20161231_01_T1_B2.TIF"
                )
            },
            "red": {
                "path": offset(
                    l1_ls5_tarball, "LT05_L1TP_090085_19970406_20161231_01_T1_B3.TIF"
                )
            },
            "nir": {
                "path": offset(
                    l1_ls5_tarball, "LT05_L1TP_090085_19970406_20161231_01_T1_B4.TIF"
                )
            },
            "swir_1": {
                "path": offset(
                    l1_ls5_tarball, "LT05_L1TP_090085_19970406_20161231_01_T1_B5.TIF"
                )
            },
            "swir_2": {
                "path": offset(
                    l1_ls5_tarball, "LT05_L1TP_090085_19970406_20161231_01_T1_B7.TIF"
                )
            },
            "tir": {
                "path": offset(
                    l1_ls5_tarball, "LT05_L1TP_090085_19970406_20161231_01_T1_B6.TIF"
                )
            },
            "quality": {
                "path": offset(
                    l1_ls5_tarball, "LT05_L1TP_090085_19970406_20161231_01_T1_BQA.TIF"
                )
            },
        },
        "accessories": {
            "metadata:landsat_mtl": {
                "path": "LT05_L1TP_090085_19970406_20161231_01_T1_MTL.txt"
            }
        },
        "lineage": {},
    }
