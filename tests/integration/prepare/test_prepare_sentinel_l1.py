import copy
import datetime
import shutil
from pathlib import Path
from typing import Dict, Tuple

import pytest

from eodatasets3.prepare import sentinel_l1_prepare
from eodatasets3.prepare.sentinel_l1_prepare import FolderInfo

from tests.common import check_prepare_outputs, run_prepare_cli


def test_subfolder_info_extraction():
    info = FolderInfo.for_path(
        Path(
            "2019/2019-01/25S125E-30S130/S2A_MSIL1C_20190101T000000_N0206_R065_T32UJ_20190101T002651.zip"
        )
    )
    assert info is not None
    assert info == FolderInfo(2019, 1, "32UJ")

    info = FolderInfo.for_path(
        Path(
            "/dea/test-data/L1C/2022/2022-03/25S125E-30S130E/"
            "S2B_MSIL1C_20210719T010729_N0301_R045_T53LQC_20210719T021248.zip"
        )
    )
    assert info == FolderInfo(2022, 3, "53LQC")

    info = FolderInfo.for_path(
        Path(
            "/g/data/fj7/Copernicus/Sentinel-2/MSI/L1C/2021/2021-07/20N095E-15N100E/"
            "S2B_MSIL1C_20210716T035539_N0301_R004_T47QMB_20210716T063913.zip"
        )
    )
    assert info == FolderInfo(2021, 7, "47QMB")

    # Older dataset structure had no region code
    info = FolderInfo.for_path(
        Path(
            "/g/data/fj7/Copernicus/Sentinel-2/MSI/L1C/2015/2015-12/30S170E-35S175E/"
            "S2A_OPER_PRD_MSIL1C_PDMC_20151225T022834_R072_V20151224T223838_20151224T223838.zip"
        )
    )
    assert info == FolderInfo(2015, 12, None)

    # A sinergise-like input path.
    info = FolderInfo.for_path(
        Path(
            "/test_filter_folder_structure_i1/L1C/2019/2019-01/25S125E-30S130/"
            "S2B_MSIL1C_20190111T000249_N0209_R030_T55HFA_20190111T011446/tileInfo.json"
        )
    )
    assert info == FolderInfo(2019, 1, "55HFA")

    # A folder that doesn't follow standard layout will return no info
    info = FolderInfo.for_path(
        Path(
            "ARD/2019/04/S2A_MSIL1C_20190101T000000_N0206_R065_T32UJ_20190101T002651.zip"
        )
    )
    assert info is None


ESA_INPUT_DATASET: Path = Path(__file__).parent.parent / (
    "data/esa_s2_l1c/S2B_MSIL1C_20201011T000249_N0209_R030_T55HFA_20201011T011446.zip"
)
SINERGISE_INPUT_DATASET: Path = Path(__file__).parent.parent / (
    "data/sinergise_s2_l1c/S2B_MSIL1C_20201011T000249_N0209_R030_T55HFA_20201011T011446"
)


ESA_EXPECTED_METADATA = {
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
    "label": "esa_s2bm_level1_0-0-20201011_55HFA_2020-10-11",
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
    "product": {"name": "esa_s2bm_level1_0"},
    "properties": {
        "datetime": datetime.datetime(2020, 10, 11, 0, 6, 49, 882566),
        "eo:cloud_cover": 24.9912,
        "eo:gsd": 10,
        "eo:instrument": "MSI",
        "eo:platform": "sentinel-2b",
        "eo:constellation": "sentinel-2",
        "eo:sun_azimuth": 46.3307328858312,
        "eo:sun_elevation": 37.3713908882192,
        "odc:dataset_version": "0.0.20201011",
        "odc:file_format": "JPEG2000",
        "odc:processing_datetime": datetime.datetime(2020, 10, 11, 1, 47, 4, 112949),
        "odc:producer": "esa.int",
        "odc:product_family": "level1",
        "odc:region_code": "55HFA",
        "sentinel:datastrip_id": "S2B_OPER_MSI_L1C_DS_EPAE_20201011T011446_S20201011T000244_N02.09",
        "sentinel:sentinel_tile_id": "S2B_OPER_MSI_L1C_TL_EPAE_20201011T011446_A018789_T55HFA_N02.09",
        "sentinel:product_name": "S2B_MSIL1C_20201011T000249_N0209_R030_T55HFA_20201011T011446",
        "sentinel:datatake_type": "INS-NOBS",
        "sat:orbit_state": "descending",
        "sat:relative_orbit": 30,
        "sentinel:datatake_start_datetime": datetime.datetime(2020, 10, 11, 1, 14, 46),
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


SINERGISE_EXPECTED_METADATA = {
    "$schema": "https://schemas.opendatacube.org/dataset",
    "id": "f3e0eee1-573c-5035-870e-8d8392df8e33",
    "crs": "epsg:32755",
    "product": {
        "name": "sinergise_s2bm_level1_0",
    },
    "geometry": {
        "coordinates": [
            [
                [600000.0, 5990200.0],
                [600000.0, 6099500.909090909],
                [600000.0, 6100000.0],
                [600332.7272727273, 6100000.0],
                [709800.0, 6100000.0],
                [709800.0, 5990200.0],
                [600000.0, 5990200.0],
            ]
        ],
        "type": "Polygon",
    },
    "grids": {
        "default": {
            "shape": [55, 55],
            "transform": [
                1996.3636363636363,
                0.0,
                600000.0,
                0.0,
                -1996.3636363636363,
                6100000.0,
                0.0,
                0.0,
                1.0,
            ],
        },
        "998": {
            "shape": [110, 110],
            "transform": [
                998.1818181818181,
                0.0,
                600000.0,
                0.0,
                -998.1818181818181,
                6100000.0,
                0.0,
                0.0,
                1.0,
            ],
        },
        "5778": {
            "shape": [19, 19],
            "transform": [
                5778.9473684210525,
                0.0,
                600000.0,
                0.0,
                -5778.9473684210525,
                6100000.0,
                0.0,
                0.0,
                1.0,
            ],
        },
    },
    "label": "sinergise_s2bm_level1_0-0-20201011_55HFA_2020-10-11",
    "lineage": {},
    "measurements": {
        "blue": {"grid": "998", "path": "B02.jp2"},
        "coastal_aerosol": {
            "grid": "5778",
            "path": "B01.jp2",
        },
        "green": {"grid": "998", "path": "B03.jp2"},
        "nir_1": {"grid": "998", "path": "B08.jp2"},
        "nir_2": {"path": "B8A.jp2"},
        "red": {"grid": "998", "path": "B04.jp2"},
        "red_edge_1": {"path": "B05.jp2"},
        "red_edge_2": {"path": "B06.jp2"},
        "red_edge_3": {"path": "B07.jp2"},
        "swir_1_cirrus": {
            "grid": "5778",
            "path": "B10.jp2",
        },
        "swir_2": {"path": "B11.jp2"},
        "swir_3": {"path": "B12.jp2"},
        "water_vapour": {
            "grid": "5778",
            "path": "B09.jp2",
        },
    },
    "properties": {
        "datetime": datetime.datetime(2020, 10, 11, 0, 6, 49, 882566),
        "eo:cloud_cover": 24.9912,
        "eo:gsd": 10,
        "eo:instrument": "MSI",
        "eo:platform": "sentinel-2b",
        "eo:constellation": "sentinel-2",
        "eo:sun_azimuth": 46.3307328858312,
        "eo:sun_elevation": 37.3713908882192,
        "odc:dataset_version": "0.0.20201011",
        "odc:file_format": "JPEG2000",
        "odc:processing_datetime": datetime.datetime(2020, 10, 11, 1, 47, 4, 112949),
        "odc:producer": "sinergise.com",
        "odc:product_family": "level1",
        "odc:region_code": "55HFA",
        "sentinel:grid_square": "FA",
        "sentinel:latitude_band": "H",
        "sentinel:utm_zone": 55,
        "sinergise_product_id": "73e1a409-595d-4fbf-8fe0-01e0ee26bf00",
        "sentinel:product_name": "S2B_MSIL1C_20201011T000249_N0209_R030_T55HFA_20201011T011446",
        "sentinel:datastrip_id": "S2B_OPER_MSI_L1C_DS_EPAE_20201011T011446_S20201011T000244_N02.09",
        "sentinel:datatake_start_datetime": datetime.datetime(2020, 10, 11, 1, 14, 46),
        "sentinel:sentinel_tile_id": "S2B_OPER_MSI_L1C_TL_EPAE_20201011T011446_A018789_T55HFA_N02.09",
    },
    "accessories": {
        "metadata:sinergise_product_info": {"path": "productInfo.json"},
        "metadata:s2_tile": {"path": "metadata.xml"},
    },
}


@pytest.fixture(params=["esa", "sinergise"])
def dataset_input_output(request, tmp_path):
    datasets = dict(
        esa=(
            ESA_INPUT_DATASET,
            ESA_EXPECTED_METADATA,
            "S2B_MSIL1C_20201011T000249_N0209_R030_T55HFA_20201011T011446.odc-metadata.yaml",
        ),
        sinergise=(
            SINERGISE_INPUT_DATASET,
            SINERGISE_EXPECTED_METADATA,
            "S2B_MSIL1C_20201011T000249_N0209_R030_T55HFA_20201011T011446.odc-metadata.yaml",
        ),
    )
    input_dataset_path, expected_metadata_doc, expected_metadata_name = datasets[
        request.param
    ]
    unique_in_path = tmp_path / input_dataset_path.name

    if input_dataset_path.is_dir():
        shutil.copytree(input_dataset_path, unique_in_path)
        # A folder input expects a metadata file inside.
        expected_metadata_path = (
            tmp_path / input_dataset_path.name / expected_metadata_name
        )
    else:
        shutil.copy(input_dataset_path, unique_in_path)
        # A file expects a sibling metadata path.
        expected_metadata_path = tmp_path / expected_metadata_name

    return (
        unique_in_path,
        copy.deepcopy(expected_metadata_doc),
        expected_metadata_path,
    )


def test_filter_folder_structure_info(
    tmp_path: Path, dataset_input_output: Tuple[Path, Dict, Path]
):
    (
        input_dataset_path,
        expected_metadata_doc,
        expected_metadata_path,
    ) = dataset_input_output

    metadata_offset = expected_metadata_path.relative_to(
        input_dataset_path if input_dataset_path.is_dir() else input_dataset_path.parent
    )

    input_folder = tmp_path / "inputs"

    subfolders = "2019/2019-01/25S125E-30S130"

    # Move input data into subfolder hierarchy.
    dataset_folder = input_folder / "L1C" / subfolders
    dataset_folder.mkdir(parents=True)
    new_input_dataset_path = dataset_folder / input_dataset_path.name
    input_dataset_path.rename(new_input_dataset_path)
    input_dataset_path = new_input_dataset_path

    # Expect metadata files to be stored in an identical hierarchy in a different output folder
    output_folder = tmp_path / "output"
    output_folder.mkdir(parents=True)
    expected_metadata_path = output_folder / subfolders / metadata_offset

    # Our output metadata is in a different place than the data, so we expect it to
    # embed the true location in the metadata (by default)
    if input_dataset_path.is_dir():
        expected_metadata_doc[
            "location"
        ] = f"file://{input_dataset_path.as_posix()}/tileInfo.json"
    else:
        expected_metadata_doc["location"] = f"zip:{input_dataset_path}!/"

    # A file with the correct region
    regions_file = tmp_path / "our-regions.txt"
    regions_file.write_text("\n".join(["55HFA", "55HFB"]))
    # A file that doesn't have our region.
    non_regions_file = tmp_path / "our-non-regions.txt"
    non_regions_file.write_text("\n".join(["55HFB", "55HFC"]))

    # Sanity check: no output exists yet.
    assert not expected_metadata_path.exists()

    # Run with filters that skips this dataset:
    # (it should do nothing)

    # Whitelist including the correct region
    res = run_prepare_cli(
        sentinel_l1_prepare.main,
        # It contains our region, so it should filter!
        "--only-regions-in-file",
        regions_file,
        # "Put the output in a different location":
        "--output-base",
        output_folder,
        input_dataset_path,
    )
    assert (
        expected_metadata_path.exists()
    ), f"Expected dataset to be processed (it's within the region file)! {res.output}"
    expected_metadata_path.unlink()

    # Run with a region list that doesn't include our dataset region.
    res = run_prepare_cli(
        sentinel_l1_prepare.main,
        # It contains our region, so it should filter!
        "--only-regions-in-file",
        non_regions_file,
        # "Put the output in a different location":
        "--output-base",
        output_folder,
        input_dataset_path,
    )
    assert (
        not expected_metadata_path.exists()
    ), f"Expected dataset to be filtered out! {res.output}"

    # Filter the time period
    res = run_prepare_cli(
        sentinel_l1_prepare.main,
        # After our time period, so it should filter!
        "--after-month",
        "2019-03",
        # "Put the output in a different location":
        "--output-base",
        output_folder,
        input_dataset_path,
    )
    assert (
        not expected_metadata_path.exists()
    ), f"Expected dataset to be filtered out! {res.output}"

    # Filter the time period
    res = run_prepare_cli(
        sentinel_l1_prepare.main,
        # Before our time period, so it should filter!
        "--before-month",
        "2018-03",
        # "Put the output in a different location":
        "--output-base",
        output_folder,
        input_dataset_path,
    )
    assert (
        not expected_metadata_path.exists()
    ), f"Expected dataset to be filtered out! {res.output}"

    # Now run for real, expect an output.
    check_prepare_outputs(
        invoke_script=sentinel_l1_prepare.main,
        run_args=[
            # Before our time period, so it should be fine!
            "--after-month",
            "2018-12",
            # "Put the output in a different location":
            "--output-base",
            output_folder,
            input_dataset_path,
        ],
        expected_doc=expected_metadata_doc,
        expected_metadata_path=expected_metadata_path,
    )

    # Now run again using a folder input, not an exact zip input.
    # (so it has to scan for datasets)
    shutil.rmtree(output_folder)
    output_folder.mkdir()

    # Now run for real, expect an output.
    check_prepare_outputs(
        invoke_script=sentinel_l1_prepare.main,
        run_args=[
            # "Put the output in a different location":
            "--output-base",
            output_folder,
            # The base folder, so it has to scan for zip files itself!
            input_folder,
        ],
        expected_doc=expected_metadata_doc,
        expected_metadata_path=expected_metadata_path,
    )


def test_generate_expected_outputs(
    tmp_path: Path, dataset_input_output: Tuple[Path, Dict, Path]
):
    """
    Run prepare on our test input scenes, and check the created metadata matches expected.
    """
    (
        input_dataset_path,
        expected_metadata_doc,
        expected_metadata_path,
    ) = dataset_input_output
    check_prepare_outputs(
        invoke_script=sentinel_l1_prepare.main,
        run_args=[
            input_dataset_path,
        ],
        expected_doc=expected_metadata_doc,
        expected_metadata_path=expected_metadata_path,
    )
