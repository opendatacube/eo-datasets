import datetime
import shutil
from pathlib import Path

import pytest

from eodatasets3.prepare import sentinel_l1c_prepare

from tests.common import check_prepare_outputs

DATASET_PATH: Path = Path(__file__).parent.parent / (
    "data/esa_s2_l1c/S2B_MSIL1C_20201011T000249_N0209_R030_T55HFA_20201011T011446.zip"
)


@pytest.fixture()
def expected_dataset_document():
    return {
        "$schema": "https://schemas.opendatacube.org/dataset",
        "id": "7c1df12c-e580-5fa2-b51b-c30a59e73bbf",
        "crs": "epsg:32755",
        "geometry": {
            "coordinates": [
                [
                    [600300.0, 6100000.0],
                    [709800.0, 6100000.0],
                    [709800.0, 5990200.0],
                    [600000.0, 5990200.0],
                    [600000.0, 6099700.0],
                    [600000.0, 6100000.0],
                    [600300.0, 6100000.0],
                ]
            ],
            "type": "Polygon",
        },
        "grids": {
            "300": {
                "shape": [366, 366],
                "transform": [
                    300.0,
                    0.0,
                    600000.0,
                    0.0,
                    -300.0,
                    6100000.0,
                    0.0,
                    0.0,
                    1.0,
                ],
            },
            "default": {
                "shape": [1098, 1098],
                "transform": [
                    100.0,
                    0.0,
                    600000.0,
                    0.0,
                    -100.0,
                    6100000.0,
                    0.0,
                    0.0,
                    1.0,
                ],
            },
            "50": {
                "shape": [2196, 2196],
                "transform": [
                    50.0,
                    0.0,
                    600000.0,
                    0.0,
                    -50.0,
                    6100000.0,
                    0.0,
                    0.0,
                    1.0,
                ],
            },
        },
        "label": "esa_s2bm_level1_1-0-20201011_55HFA_2020-10-11",
        "lineage": {},
        "measurements": {
            "blue": {
                "grid": "50",
                "path": "S2B_MSIL1C_20201011T000249_N0209_R030_T55HFA_20201011T011446.SAFE/GRANULE/"
                "L1C_T55HFA_A018789_20201011T000244/IMG_DATA/T55HFA_20201011T000249_B02.jp2",
            },
            "coastal_aerosol": {
                "grid": "300",
                "path": "S2B_MSIL1C_20201011T000249_N0209_R030_T55HFA_20201011T011446.SAFE/GRANULE/"
                "L1C_T55HFA_A018789_20201011T000244/IMG_DATA/T55HFA_20201011T000249_B01.jp2",
            },
            "green": {
                "grid": "50",
                "path": "S2B_MSIL1C_20201011T000249_N0209_R030_T55HFA_20201011T011446.SAFE/GRANULE/"
                "L1C_T55HFA_A018789_20201011T000244/IMG_DATA/T55HFA_20201011T000249_B03.jp2",
            },
            "nir_1": {
                "grid": "50",
                "path": "S2B_MSIL1C_20201011T000249_N0209_R030_T55HFA_20201011T011446.SAFE/GRANULE/"
                "L1C_T55HFA_A018789_20201011T000244/IMG_DATA/T55HFA_20201011T000249_B08.jp2",
            },
            "red": {
                "grid": "50",
                "path": "S2B_MSIL1C_20201011T000249_N0209_R030_T55HFA_20201011T011446.SAFE/GRANULE/"
                "L1C_T55HFA_A018789_20201011T000244/IMG_DATA/T55HFA_20201011T000249_B04.jp2",
            },
            "red_edge_1": {
                "path": "S2B_MSIL1C_20201011T000249_N0209_R030_T55HFA_20201011T011446.SAFE/GRANULE/"
                "L1C_T55HFA_A018789_20201011T000244/IMG_DATA/T55HFA_20201011T000249_B05.jp2"
            },
            "red_edge_2": {
                "path": "S2B_MSIL1C_20201011T000249_N0209_R030_T55HFA_20201011T011446.SAFE/GRANULE/"
                "L1C_T55HFA_A018789_20201011T000244/IMG_DATA/T55HFA_20201011T000249_B06.jp2"
            },
            "red_edge_3": {
                "path": "S2B_MSIL1C_20201011T000249_N0209_R030_T55HFA_20201011T011446.SAFE/GRANULE/"
                "L1C_T55HFA_A018789_20201011T000244/IMG_DATA/T55HFA_20201011T000249_B07.jp2"
            },
            "swir_1_cirrus": {
                "grid": "300",
                "path": "S2B_MSIL1C_20201011T000249_N0209_R030_T55HFA_20201011T011446.SAFE/GRANULE/"
                "L1C_T55HFA_A018789_20201011T000244/IMG_DATA/T55HFA_20201011T000249_B10.jp2",
            },
            "swir_2": {
                "path": "S2B_MSIL1C_20201011T000249_N0209_R030_T55HFA_20201011T011446.SAFE/GRANULE/"
                "L1C_T55HFA_A018789_20201011T000244/IMG_DATA/T55HFA_20201011T000249_B11.jp2"
            },
            "swir_3": {
                "path": "S2B_MSIL1C_20201011T000249_N0209_R030_T55HFA_20201011T011446.SAFE/GRANULE/"
                "L1C_T55HFA_A018789_20201011T000244/IMG_DATA/T55HFA_20201011T000249_B12.jp2"
            },
            "water_vapour": {
                "grid": "300",
                "path": "S2B_MSIL1C_20201011T000249_N0209_R030_T55HFA_20201011T011446.SAFE/GRANULE/"
                "L1C_T55HFA_A018789_20201011T000244/IMG_DATA/T55HFA_20201011T000249_B09.jp2",
            },
        },
        "product": {"name": "esa_s2bm_level1_1"},
        "properties": {
            "datetime": datetime.datetime(2020, 10, 11, 0, 6, 49, 882566),
            "eo:cloud_cover": 24.9912,
            "eo:gsd": 10,
            "eo:instrument": "MSI",
            "eo:platform": "sentinel-2b",
            "eo:constellation": "sentinel-2",
            "eo:sun_azimuth": 46.3307328858312,
            "eo:sun_elevation": 37.3713908882192,
            "odc:dataset_version": "1.0.20201011",
            "odc:file_format": "JPEG2000",
            "odc:processing_datetime": datetime.datetime(
                2020, 10, 11, 1, 47, 4, 112949
            ),
            "odc:producer": "esa.int",
            "odc:product_family": "level1",
            "odc:region_code": "55HFA",
            "sentinel:datastrip_id": "S2B_OPER_MSI_L1C_DS_EPAE_20201011T011446_S20201011T000244_N02.09",
            "sentinel:sentinel_tile_id": "S2B_OPER_MSI_L1C_TL_EPAE_20201011T011446_A018789_T55HFA_N02.09",
            "sentinel:product_name": "S2B_MSIL1C_20201011T000249_N0209_R030_T55HFA_20201011T011446",
            "sentinel:datatake_type": "INS-NOBS",
            "sat:orbit_state": "descending",
            "sat:relative_orbit": 30,
            "sentinel:datatake_start_datetime": datetime.datetime(
                2020, 10, 11, 1, 14, 46
            ),
            "sentinel:processing_baseline": "02.09",
            "sentinel:processing_center": "EPAE",
            "sentinel:reception_station": "EDRS",
        },
        "accessories": {
            "metadata:s2_datastrip": {
                "path": "S2B_MSIL1C_20201011T000249_N0209_R030_T55HFA_20201011T011446.SAFE/DATASTRIP/"
                "DS_EPAE_20201011T011446_S20201011T000244/MTD_DS.xml"
            },
            "metadata:s2_user_product": {
                "path": "S2B_MSIL1C_20201011T000249_N0209_R030_T55HFA_20201011T011446.SAFE/MTD_MSIL1C.xml"
            },
            "metadata:s2_tile": {
                "path": "S2B_MSIL1C_20201011T000249_N0209_R030_T55HFA_20201011T011446.SAFE/GRANULE/"
                "L1C_T55HFA_A018789_20201011T000244/MTD_TL.xml"
            },
        },
    }


def test_run(tmp_path, expected_dataset_document):
    """
    Run prepare on our test input scene, and check the created metadata matches expected.
    """
    shutil.copy(DATASET_PATH, tmp_path)
    expected_metadata_path = tmp_path / (
        "S2B_MSIL1C_20201011T000249_N0209_R030_T55HFA_20201011T011446.odc-metadata.yaml"
    )
    check_prepare_outputs(
        invoke_script=sentinel_l1c_prepare.main,
        run_args=[
            tmp_path / DATASET_PATH.name,
        ],
        expected_doc=expected_dataset_document,
        expected_metadata_path=expected_metadata_path,
    )
