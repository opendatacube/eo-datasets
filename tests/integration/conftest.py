import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, Callable

import pytest

from eodatasets3 import serialise
from eodatasets3.model import DatasetDoc
from eodatasets3.prepare.landsat_l1_prepare import normalise_nci_symlinks

L71GT_TARBALL_PATH: Path = (
    Path(__file__).parent / "data" / "LE07_L1TP_104078_20130429_20161124_01_T1.tar"
)

L5_TARBALL_PATH: Path = (
    Path(__file__).parent / "data" / "LT05_L1TP_090085_19970406_20161231_01_T1.tar.gz"
)

L8_INPUT_PATH: Path = (
    Path(__file__).parent / "data" / "LC08_L1TP_090084_20160121_20170405_01_T1"
)

L8_C2_INPUT_PATH: Path = (
    Path(__file__).parent / "data" / "LC08_L1TP_090084_20160121_20200907_02_T1"
)

LS8_TELEMETRY_PATH: Path = (
    Path(__file__).parent
    / "data"
    / "LS8_OLITIRS_STD-MD_P00_LC80840720742017365LGN00_084_072-074_20180101T004644Z20180101T004824_1"
)


WOFS_PATH: Path = Path(__file__).parent / "data" / "wofs"


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
def l1_c2_ls8_folder(tmp_path: Path) -> Path:
    return _make_copy(L8_C2_INPUT_PATH, tmp_path)


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
def l1_c2_ls8_usgs_expected(l1_ls8_folder) -> Dict:
    return expected_l1_ls8_folder(
        l1_ls8_folder,
        relative_offset,
        organisation="usgs.gov",
        collection="2",
        l1_collection="2",
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
    raise AssertionError


def expected_l1_ls8_folder(
    l1_ls8_folder: Path,
    offset: Callable[[Path, str], str] = relative_offset,
    organisation="usgs.gov",
    collection="1",
    l1_collection="1",
    lineage=None,
):
    """
    :param collection: The collection of the current scene
    :param l1_collection: The collection of the original landsat l1 scene
    :return:
    """
    org_code = organisation.split(".")[0]
    product_name = f"{org_code}_ls8c_level1_{collection}"
    if collection == "2":
        processing_datetime = datetime(2020, 9, 7, 19, 30, 5)
        cloud_cover = 93.28
        points_model = 125
        points_version = 5
        rmse_model_x = 4.525
        rmse_model_y = 5.917
        software_version = "LPGS_15.3.1c"
        uuid = "d9221c40-24c3-5356-ab22-4dcac2bf2d70"
        quality_tag = "QA_PIXEL"
    else:
        processing_datetime = datetime(2017, 4, 5, 11, 17, 36)
        cloud_cover = 93.22
        points_model = 66
        points_version = 4
        rmse_model_x = 4.593
        rmse_model_y = 5.817
        software_version = "LPGS_2.7.0"
        uuid = "a780754e-a884-58a7-9ac0-df518a67f59d"
        quality_tag = "BQA"
    processing_date = processing_datetime.strftime("%Y%m%d")
    return {
        "$schema": "https://schemas.opendatacube.org/dataset",
        "id": uuid,
        "label": f"{product_name}-0-{processing_date}_090084_2016-01-21",
        "product": {
            "name": product_name,
            "href": f"https://collections.dea.ga.gov.au/product/{product_name}",
        },
        "properties": {
            "datetime": datetime(2016, 1, 21, 23, 50, 23, 54435),
            # The minor version comes from the processing date (as used in filenames to distinguish reprocesses).
            "odc:dataset_version": f"{collection}.0.{processing_date}",
            "odc:file_format": "GeoTIFF",
            "odc:processing_datetime": processing_datetime,
            "odc:producer": organisation,
            "odc:product_family": "level1",
            "odc:region_code": "090084",
            "eo:cloud_cover": cloud_cover,
            "eo:gsd": 15.0,
            "eo:instrument": "OLI_TIRS",
            "eo:platform": "landsat-8",
            "eo:sun_azimuth": 74.007_443_8,
            "eo:sun_elevation": 55.486_483,
            "landsat:collection_category": "T1",
            "landsat:collection_number": int(l1_collection),
            "landsat:data_type": "L1TP",
            "landsat:geometric_rmse_model_x": rmse_model_x,
            "landsat:geometric_rmse_model_y": rmse_model_y,
            "landsat:ground_control_points_model": points_model,
            "landsat:ground_control_points_version": points_version,
            "landsat:landsat_product_id": f"LC08_L1TP_090084_20160121_{processing_date}_0{l1_collection}_T1",
            "landsat:landsat_scene_id": "LC80900842016021LGN02",
            "landsat:processing_software_version": software_version,
            "landsat:station_id": "LGN",
            "landsat:wrs_path": 90,
            "landsat:wrs_row": 84,
        },
        "crs": "epsg:32655",
        "geometry": {
            "coordinates": [
                [
                    [879307.5, -3776885.4340469087],
                    [879307.5, -3778240.713151076],
                    [839623.3108524992, -3938223.736900397],
                    [832105.7835592609, -3953107.5],
                    [831455.8296215904, -3953107.5],
                    [831453.7930575205, -3953115.0],
                    [819969.5411349908, -3953115.0],
                    [641985.0, -3906446.160824098],
                    [641985.0, -3889797.3351159613],
                    [685647.6920251067, -3717468.346156044],
                    [688909.3673333039, -3714585.0],
                    [708011.4230769231, -3714585.0],
                    [879315.0, -3761214.3020833335],
                    [879315.0, -3776857.8139976147],
                    [879307.5, -3776885.4340469087],
                ]
            ],
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
                    l1_ls8_folder,
                    f"LC08_L1TP_090084_20160121_{processing_date}_0{l1_collection}_T1_B1.TIF",
                )
            },
            "blue": {
                "path": offset(
                    l1_ls8_folder,
                    f"LC08_L1TP_090084_20160121_{processing_date}_0{l1_collection}_T1_B2.TIF",
                )
            },
            "green": {
                "path": offset(
                    l1_ls8_folder,
                    f"LC08_L1TP_090084_20160121_{processing_date}_0{l1_collection}_T1_B3.TIF",
                )
            },
            "red": {
                "path": offset(
                    l1_ls8_folder,
                    f"LC08_L1TP_090084_20160121_{processing_date}_0{l1_collection}_T1_B4.TIF",
                )
            },
            "nir": {
                "path": offset(
                    l1_ls8_folder,
                    f"LC08_L1TP_090084_20160121_{processing_date}_0{l1_collection}_T1_B5.TIF",
                )
            },
            "swir_1": {
                "path": offset(
                    l1_ls8_folder,
                    f"LC08_L1TP_090084_20160121_{processing_date}_0{l1_collection}_T1_B6.TIF",
                )
            },
            "swir_2": {
                "path": offset(
                    l1_ls8_folder,
                    f"LC08_L1TP_090084_20160121_{processing_date}_0{l1_collection}_T1_B7.TIF",
                )
            },
            "panchromatic": {
                "grid": "panchromatic",
                "path": offset(
                    l1_ls8_folder,
                    f"LC08_L1TP_090084_20160121_{processing_date}_0{l1_collection}_T1_B8.TIF",
                ),
            },
            "cirrus": {
                "path": offset(
                    l1_ls8_folder,
                    f"LC08_L1TP_090084_20160121_{processing_date}_0{l1_collection}_T1_B9.TIF",
                )
            },
            "lwir_1": {
                "path": offset(
                    l1_ls8_folder,
                    f"LC08_L1TP_090084_20160121_{processing_date}_0{l1_collection}_T1_B10.TIF",
                )
            },
            "lwir_2": {
                "path": offset(
                    l1_ls8_folder,
                    f"LC08_L1TP_090084_20160121_{processing_date}_0{l1_collection}_T1_B11.TIF",
                )
            },
            "quality": {
                "path": offset(
                    l1_ls8_folder,
                    f"LC08_L1TP_090084_20160121_{processing_date}_0{l1_collection}_T1_{quality_tag}.TIF",
                )
            },
        },
        "accessories": {
            "metadata:landsat_mtl": {
                "path": f"LC08_L1TP_090084_20160121_{processing_date}_0{l1_collection}_T1_MTL.txt"
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
            "coordinates": [
                [
                    [563899.8055973209, -2773901.091688032],
                    [565174.4110839436, -2768992.5],
                    [570160.9334151996, -2768992.5],
                    [570170.5, -2768985.0],
                    [570223.8384207273, -2768992.5],
                    [588295.85185022, -2768992.5],
                    [721491.3496511441, -2790262.4648538055],
                    [721949.2518894314, -2790326.8512143376],
                    [758758.6847331291, -2797433.428778118],
                    [770115.0, -2803848.537800015],
                    [770115.0, -2819232.438669062],
                    [745548.1744650088, -2936338.4604302375],
                    [730486.820264895, -2981715.0],
                    [707958.3856497289, -2981715.0],
                    [707665.2711912903, -2981666.327459585],
                    [695862.7971396025, -2981638.6536404933],
                    [593801.8189357058, -2963902.554008508],
                    [537007.2875722996, -2953328.054250119],
                    [536671.5165534337, -2953272.2984591112],
                    [526480.1507793682, -2945221.547092697],
                    [525364.2405528432, -2927837.1702428674],
                    [529047.5112406499, -2911836.1482165447],
                    [529451.9856980122, -2906561.9692719015],
                    [536583.4124976253, -2879098.363725102],
                    [545784.6687009194, -2839125.873061804],
                    [562106.6687009194, -2775306.873061804],
                    [563899.8055973209, -2773901.091688032],
                ]
            ],
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
            "coordinates": [
                [
                    [636860.78, -3881685.0],
                    [593385.0, -4045573.25],
                    [593385.0, -4062038.8525309917],
                    [636850.5348070407, -4075317.3559092814],
                    [768589.1325495897, -4101015.0],
                    [790215.8408397632, -4101015.0],
                    [795289.3607718372, -4094590.58897732],
                    [835815.0, -3937552.555313688],
                    [835815.0, -3920661.1474690083],
                    [792349.4651929593, -3907382.6440907186],
                    [657073.5519714399, -3881685.0],
                    [636860.78, -3881685.0],
                ]
            ],
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


@pytest.fixture
def input_uint8_tif() -> Path:
    return Path(WOFS_PATH / "ga_ls_wofs_3_099081_2020-07-26_interim_water_clipped.tif")


@pytest.fixture
def input_uint8_tif_2() -> Path:
    return Path(WOFS_PATH / "ga_ls_wofs_3_090081_1993_01_05_interim_water_clipped.tif")
