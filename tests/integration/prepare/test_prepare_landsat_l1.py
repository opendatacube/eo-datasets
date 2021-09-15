import shutil
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict

import pytest

from eodatasets3.prepare import landsat_l1_prepare

from tests.common import check_prepare_outputs, run_prepare_cli

LC08_L2_C2_POST_20210507_INPUT_PATH: Path = (
    Path(__file__).parent.parent / "data" / "LC08_L2SP_098084_20210503_20210508_02_T1"
)

LT05_L2_C2_INPUT_PATH: Path = (
    Path(__file__).parent.parent / "data" / "LT05_L2SP_090084_19980308_20200909_02_T1"
)

LE07_L2_C2_INPUT_PATH: Path = (
    Path(__file__).parent.parent / "data" / "LE07_L2SP_090084_20210331_20210426_02_T1"
)


@pytest.fixture
def lc08_l2_c2_post_20210507_folder(tmp_path: Path) -> Path:
    return _make_copy(LC08_L2_C2_POST_20210507_INPUT_PATH, tmp_path)


@pytest.fixture
def lt05_l2_c2_folder(tmp_path: Path) -> Path:
    return _make_copy(LT05_L2_C2_INPUT_PATH, tmp_path)


@pytest.fixture
def le07_l2_c2_folder(tmp_path: Path) -> Path:
    return _make_copy(LE07_L2_C2_INPUT_PATH, tmp_path)


def relative_offset(base, offset):
    return offset


def _make_copy(input_path, tmp_path):
    our_input = tmp_path / input_path.name
    if input_path.is_file():
        shutil.copy(input_path, our_input)
    else:
        shutil.copytree(input_path, our_input)
    return our_input


def test_prepare_l5_l1_usgs_tarball(
    tmp_path: Path, l1_ls5_tarball_md_expected: Dict, l1_ls5_tarball: Path
):
    assert l1_ls5_tarball.exists(), "Test data missing(?)"
    output_path: Path = tmp_path / "out"
    output_path.mkdir()

    # When specifying an output base path it will create path/row subfolders within it.
    expected_metadata_path = (
        output_path
        / "090"
        / "085"
        / "LT05_L1TP_090085_19970406_20161231_01_T1.odc-metadata.yaml"
    )

    check_prepare_outputs(
        invoke_script=landsat_l1_prepare.main,
        run_args=["--output-base", str(output_path), str(l1_ls5_tarball)],
        expected_doc=l1_ls5_tarball_md_expected,
        expected_metadata_path=expected_metadata_path,
    )


def test_prepare_l8_l1_usgs_tarball(l1_ls8_folder, l1_ls8_folder_md_expected):
    assert l1_ls8_folder.exists(), "Test data missing(?)"

    # No output path defined,so it will default to being a sibling to the input.
    expected_metadata_path = (
        l1_ls8_folder.parent
        / "LC08_L1TP_090084_20160121_20170405_01_T1.odc-metadata.yaml"
    )
    assert not expected_metadata_path.exists()

    check_prepare_outputs(
        invoke_script=landsat_l1_prepare.main,
        run_args=[str(l1_ls8_folder)],
        expected_doc=l1_ls8_folder_md_expected,
        expected_metadata_path=expected_metadata_path,
    )


def test_prepare_l8_l1_c2(
    tmp_path: Path, l1_c2_ls8_folder: Path, l1_c2_ls8_usgs_expected: Dict
):
    """Run prepare script with a source telemetry data and unique producer."""
    assert l1_c2_ls8_folder.exists(), "Test data missing(?)"

    output_path = tmp_path
    expected_metadata_path = (
        output_path
        / "090"
        / "084"
        / "LC08_L1TP_090084_20160121_20200907_02_T1.odc-metadata.yaml"
    )
    check_prepare_outputs(
        invoke_script=landsat_l1_prepare.main,
        run_args=[
            "--output-base",
            output_path,
            "--producer",
            "usgs.gov",
            l1_c2_ls8_folder,
        ],
        expected_doc=l1_c2_ls8_usgs_expected,
        expected_metadata_path=expected_metadata_path,
    )


def test_prepare_lc08_l2_c2_post_20210507(
    tmp_path: Path,
    lc08_l2_c2_post_20210507_folder: Path,
):
    """Support a functionality baseline for the enhancements to expand landsat
    prepare (YAML) logic to support USGS level 2 - PR#159:
     LC08 C2 L2 post 7th May 2021."""
    assert lc08_l2_c2_post_20210507_folder.exists(), "Test data missing(?)"

    output_path = tmp_path
    expected_metadata_path = (
        output_path
        / "098"
        / "084"
        / "LC08_L2SP_098084_20210503_20210508_02_T1.odc-metadata.yaml"
    )
    check_prepare_outputs(
        invoke_script=landsat_l1_prepare.main,
        run_args=[
            "--output-base",
            output_path,
            "--producer",
            "usgs.gov",
            lc08_l2_c2_post_20210507_folder,
        ],
        expected_doc=expected_lc08_l2_c2_post_20210507_folder(),
        expected_metadata_path=expected_metadata_path,
    )


def test_prepare_lt05_l2_c2(
    tmp_path: Path,
    lt05_l2_c2_folder: Path,
):
    """Support a functionality baseline for the enhancements to expand landsat
    prepare (YAML) logic to support USGS level 2 - PR#159:
     LT05 C2 L2."""
    assert lt05_l2_c2_folder.exists(), "Test data missing(?)"

    output_path = tmp_path
    expected_metadata_path = (
        output_path
        / "090"
        / "084"
        / "LT05_L2SP_090084_19980308_20200909_02_T1.odc-metadata.yaml"
    )
    check_prepare_outputs(
        invoke_script=landsat_l1_prepare.main,
        run_args=[
            "--output-base",
            output_path,
            "--producer",
            "usgs.gov",
            lt05_l2_c2_folder,
        ],
        expected_doc=expected_lt05_l2_c2_folder(),
        expected_metadata_path=expected_metadata_path,
    )


def test_prepare_le07_l2_c2(
    tmp_path: Path,
    le07_l2_c2_folder: Path,
):
    """Support a functionality baseline for the enhancements to expand landsat
    prepare (YAML) logic to support USGS level 2 - PR#159:
     LE07 C2 L2."""
    assert le07_l2_c2_folder.exists(), "Test data missing(?)"

    output_path = tmp_path
    expected_metadata_path = (
        output_path
        / "090"
        / "084"
        / "LE07_L2SP_090084_20210331_20210426_02_T1.odc-metadata.yaml"
    )
    check_prepare_outputs(
        invoke_script=landsat_l1_prepare.main,
        run_args=[
            "--output-base",
            output_path,
            "--producer",
            "usgs.gov",
            le07_l2_c2_folder,
        ],
        expected_doc=expected_le07_l2_c2_folder(),
        expected_metadata_path=expected_metadata_path,
    )


def test_prepare_l8_l1_tarball_with_source(
    tmp_path: Path, l1_ls8_folder: Path, ls8_telemetry_path, l1_ls8_ga_expected: Dict
):
    """Run prepare script with a source telemetry data and unique producer."""
    assert l1_ls8_folder.exists(), "Test data missing(?)"

    output_path = tmp_path
    expected_metadata_path = (
        output_path
        / "090"
        / "084"
        / "LC08_L1TP_090084_20160121_20170405_01_T1.odc-metadata.yaml"
    )
    check_prepare_outputs(
        invoke_script=landsat_l1_prepare.main,
        run_args=[
            "--output-base",
            output_path,
            "--producer",
            "ga.gov.au",
            "--source",
            ls8_telemetry_path,
            l1_ls8_folder,
        ],
        expected_doc=l1_ls8_ga_expected,
        expected_metadata_path=expected_metadata_path,
    )


def test_prepare_l7_l1_usgs_tarball(
    l1_ls7_tarball: Path, l1_ls7_tarball_md_expected: Dict
):
    assert l1_ls7_tarball.exists(), "Test data missing(?)"

    expected_metadata_path = (
        l1_ls7_tarball.parent
        / "LE07_L1TP_104078_20130429_20161124_01_T1.odc-metadata.yaml"
    )

    check_prepare_outputs(
        invoke_script=landsat_l1_prepare.main,
        run_args=[str(l1_ls7_tarball)],
        expected_doc=l1_ls7_tarball_md_expected,
        expected_metadata_path=expected_metadata_path,
    )


def test_skips_old_datasets(l1_ls7_tarball):
    """Prepare should skip datasets older than the given date"""
    expected_metadata_path = (
        l1_ls7_tarball.parent
        / "LE07_L1TP_104078_20130429_20161124_01_T1.odc-metadata.yaml"
    )

    run_prepare_cli(
        landsat_l1_prepare.main,
        # Can't be newer than right now.
        "--newer-than",
        datetime.now().isoformat(),
        str(l1_ls7_tarball),
    )
    assert (
        not expected_metadata_path.exists()
    ), "Dataset should have been skipped due to age"

    # It should work with an old date.
    run_prepare_cli(
        landsat_l1_prepare.main,
        # Some old date, from before the test data was created.
        "--newer-than",
        "2014-05-04",
        str(l1_ls7_tarball),
    )
    assert (
        expected_metadata_path.exists()
    ), "Dataset should have been packaged when using an ancient date cutoff"


def expected_lc08_l2_c2_post_20210507_folder(
    l2_c2_ls8_folder: Path = None,
    offset: Callable[[Path, str], str] = relative_offset,
    organisation="usgs.gov",
    collection="2",
    leveln_collection="2",
    lineage=None,
):
    """ """
    org_code = organisation.split(".")[0]
    product_name = f"{org_code}_ls8c_level{leveln_collection}_{collection}"
    processing_datetime = datetime(2021, 5, 8, 11, 5, 47)
    cloud_cover = 72.57
    points_model = 35
    points_version = 5
    rmse_model_x = 5.974
    rmse_model_y = 4.276
    software_version = "LPGS_15.4.0"
    uuid = "d4ff459f-67e0-57df-af73-dfef14ad9c47"
    quality_tag = "QA_PIXEL"
    processing_date = processing_datetime.strftime("%Y%m%d")
    return {
        "$schema": "https://schemas.opendatacube.org/dataset",
        "id": uuid,
        "label": f"{product_name}-0-{processing_date}_098084_2021-05-03",
        "product": {
            "name": product_name,
            "href": f"https://collections.dea.ga.gov.au/product/{product_name}",
        },
        "properties": {
            "datetime": datetime(2021, 5, 3, 0, 39, 15, 718295),
            # The minor version comes from the processing date,
            # as used in filenames to distinguish reprocesses.
            "odc:dataset_version": f"{collection}.0.{processing_date}",
            "odc:file_format": "GeoTIFF",
            "odc:processing_datetime": processing_datetime,
            "odc:producer": organisation,
            "odc:product_family": "level2",
            "odc:region_code": "098084",
            "eo:cloud_cover": cloud_cover,
            "eo:gsd": 30.0,
            "eo:instrument": "OLI_TIRS",
            "eo:platform": "landsat-8",
            "eo:sun_azimuth": 36.555_149_01,
            "eo:sun_elevation": 31.263_730_68,
            "landsat:algorithm_source_surface_reflectance": "LaSRC_1.5.0",
            "landsat:collection_category": "T1",
            "landsat:collection_number": int(leveln_collection),
            "landsat:data_type": "L2SP",
            "landsat:geometric_rmse_model_x": rmse_model_x,
            "landsat:geometric_rmse_model_y": rmse_model_y,
            "landsat:ground_control_points_model": points_model,
            "landsat:ground_control_points_version": points_version,
            "landsat:landsat_product_id": f"LC08_L2SP_098084_20210503_{processing_date}_0{leveln_collection}_T1",
            "landsat:landsat_scene_id": "LC80980842021123LGN00",
            "landsat:processing_software_version": software_version,
            "landsat:station_id": "LGN",
            "landsat:wrs_path": 98,
            "landsat:wrs_row": 84,
        },
        "crs": "epsg:32653",
        "geometry": {
            "coordinates": [
                [
                    [656390.7366486033, -3713985.0],
                    [653137.3072645832, -3716864.7197616836],
                    [609585.0, -3888976.9699604893],
                    [609585.0, -3905841.19690461],
                    [691402.3702754473, -3932222.5947522246],
                    [777854.1421397077, -3952215.0],
                    [796350.2575455576, -3952215.0],
                    [818563.2799541968, -3885739.54955405],
                    [846315.0, -3761631.0],
                    [823680.1297245529, -3749859.4052477754],
                    [677099.698619789, -3713985.0],
                    [656390.7366486033, -3713985.0],
                ],
            ],
            "type": "Polygon",
        },
        "grids": {
            "default": {
                "shape": (60, 60),
                "transform": (
                    3945.5000000000005,
                    0.0,
                    609585.0,
                    0.0,
                    -3970.5,
                    -3713985.0,
                    0.0,
                    0.0,
                    1.0,
                ),
            },
        },
        "measurements": {
            "blue": {
                "path": offset(
                    l2_c2_ls8_folder,
                    f"LC08_L2SP_098084_20210503_{processing_date}_0{leveln_collection}_T1_SR_B2.TIF",
                )
            },
            "coastal_aerosol": {
                "path": offset(
                    l2_c2_ls8_folder,
                    f"LC08_L2SP_098084_20210503_{processing_date}_0{leveln_collection}_T1_SR_B1.TIF",
                )
            },
            "green": {
                "path": offset(
                    l2_c2_ls8_folder,
                    f"LC08_L2SP_098084_20210503_{processing_date}_0{leveln_collection}_T1_SR_B3.TIF",
                )
            },
            "lwir": {
                "path": offset(
                    l2_c2_ls8_folder,
                    f"LC08_L2SP_098084_20210503_{processing_date}_0{leveln_collection}_T1_ST_B10.TIF",
                )
            },
            "nir": {
                "path": offset(
                    l2_c2_ls8_folder,
                    f"LC08_L2SP_098084_20210503_{processing_date}_0{leveln_collection}_T1_SR_B5.TIF",
                )
            },
            "qa_aerosol": {
                "path": offset(
                    l2_c2_ls8_folder,
                    f"LC08_L2SP_098084_20210503_{processing_date}_0{leveln_collection}_T1_SR_QA_AEROSOL.TIF",
                )
            },
            "quality": {
                "path": offset(
                    l2_c2_ls8_folder,
                    f"LC08_L2SP_098084_20210503_{processing_date}_0{leveln_collection}_T1_{quality_tag}.TIF",
                )
            },
            "red": {
                "path": offset(
                    l2_c2_ls8_folder,
                    f"LC08_L2SP_098084_20210503_{processing_date}_0{leveln_collection}_T1_SR_B4.TIF",
                )
            },
            "swir_1": {
                "path": offset(
                    l2_c2_ls8_folder,
                    f"LC08_L2SP_098084_20210503_{processing_date}_0{leveln_collection}_T1_SR_B6.TIF",
                )
            },
            "swir_2": {
                "path": offset(
                    l2_c2_ls8_folder,
                    f"LC08_L2SP_098084_20210503_{processing_date}_0{leveln_collection}_T1_SR_B7.TIF",
                )
            },
        },
        "accessories": {
            "metadata:landsat_mtl": {
                "path": f"LC08_L2SP_098084_20210503_{processing_date}_0{leveln_collection}_T1_MTL.txt"
            }
        },
        "lineage": lineage or {},
    }


def expected_lt05_l2_c2_folder(
    l2_c2_ls8_folder: Path = None,
    offset: Callable[[Path, str], str] = relative_offset,
    organisation="usgs.gov",
    collection="2",
    leveln_collection="2",
    lineage=None,
):
    """ """
    org_code = organisation.split(".")[0]
    product_name = f"{org_code}_ls5t_level{leveln_collection}_{collection}"
    processing_datetime = datetime(2020, 9, 9, 10, 36, 59)
    cloud_cover = 12.0
    points_model = 965
    points_version = 5
    rmse_model_x = 3.085
    rmse_model_y = 2.977
    software_version = "LPGS_15.3.1c"
    uuid = "b08b3f9e-b00c-5a67-88d8-a889f0e79d00"
    quality_tag = "QA_PIXEL"
    processing_date = processing_datetime.strftime("%Y%m%d")
    return {
        "$schema": "https://schemas.opendatacube.org/dataset",
        "id": uuid,
        "label": f"{product_name}-0-{processing_date}_090084_1998-03-08",
        "product": {
            "name": product_name,
            "href": f"https://collections.dea.ga.gov.au/product/{product_name}",
        },
        "properties": {
            "datetime": datetime(1998, 3, 8, 23, 26, 47, 294081),
            # The minor version comes from the processing date,
            # as used in filenames to distinguish reprocesses.
            "odc:dataset_version": f"{collection}.0.{processing_date}",
            "odc:file_format": "GeoTIFF",
            "odc:processing_datetime": processing_datetime,
            "odc:producer": organisation,
            "odc:product_family": "level2",
            "odc:region_code": "090084",
            "eo:cloud_cover": cloud_cover,
            "eo:gsd": 30.0,
            "eo:instrument": "TM",
            "eo:platform": "landsat-5",
            "eo:sun_azimuth": 61.298_799_16,
            "eo:sun_elevation": 41.583_263_99,
            "landsat:algorithm_source_surface_reflectance": "LEDAPS_3.4.0",
            "landsat:collection_category": "T1",
            "landsat:collection_number": int(leveln_collection),
            "landsat:data_type": "L2SP",
            "landsat:geometric_rmse_model_x": rmse_model_x,
            "landsat:geometric_rmse_model_y": rmse_model_y,
            "landsat:ground_control_points_model": points_model,
            "landsat:ground_control_points_version": points_version,
            "landsat:landsat_product_id": f"LT05_L2SP_090084_19980308_{processing_date}_0{leveln_collection}_T1",
            "landsat:landsat_scene_id": "LT50900841998067ASA00",
            "landsat:processing_software_version": software_version,
            "landsat:station_id": "ASA",
            "landsat:wrs_path": 90,
            "landsat:wrs_row": 84,
        },
        "crs": "epsg:32655",
        "geometry": {
            "coordinates": [
                [
                    [686022.6472422444, -3724785.0],
                    [682595.9899406636, -3727553.91102268],
                    [638085.0, -3907560.0],
                    [790642.5733366499, -3940388.5126599884],
                    [830607.0294154783, -3943044.3288386273],
                    [880215.0, -3761340.0],
                    [707789.0909090909, -3724785.0],
                    [686022.6472422444, -3724785.0],
                ],
            ],
            "type": "Polygon",
        },
        "grids": {
            "default": {
                "shape": (60, 60),
                "transform": (
                    4035.5000000000005,
                    0.0,
                    638085.0,
                    0.0,
                    -3655.5,
                    -3724785.0,
                    0.0,
                    0.0,
                    1.0,
                ),
            },
        },
        "measurements": {
            "atmos_opacity": {
                "path": offset(
                    l2_c2_ls8_folder,
                    f"LT05_L2SP_090084_19980308_{processing_date}_0{leveln_collection}_T1_SR_ATMOS_OPACITY.TIF",
                )
            },
            "blue": {
                "path": offset(
                    l2_c2_ls8_folder,
                    f"LT05_L2SP_090084_19980308_{processing_date}_0{leveln_collection}_T1_SR_B1.TIF",
                )
            },
            "green": {
                "path": offset(
                    l2_c2_ls8_folder,
                    f"LT05_L2SP_090084_19980308_{processing_date}_0{leveln_collection}_T1_SR_B2.TIF",
                )
            },
            "lwir": {
                "path": offset(
                    l2_c2_ls8_folder,
                    f"LT05_L2SP_090084_19980308_{processing_date}_0{leveln_collection}_T1_ST_B6.TIF",
                )
            },
            "nir": {
                "path": offset(
                    l2_c2_ls8_folder,
                    f"LT05_L2SP_090084_19980308_{processing_date}_0{leveln_collection}_T1_SR_B4.TIF",
                )
            },
            "qa_cloud": {
                "path": offset(
                    l2_c2_ls8_folder,
                    f"LT05_L2SP_090084_19980308_{processing_date}_0{leveln_collection}_T1_SR_CLOUD_QA.TIF",
                )
            },
            "quality": {
                "path": offset(
                    l2_c2_ls8_folder,
                    f"LT05_L2SP_090084_19980308_{processing_date}_0{leveln_collection}_T1_{quality_tag}.TIF",
                )
            },
            "red": {
                "path": offset(
                    l2_c2_ls8_folder,
                    f"LT05_L2SP_090084_19980308_{processing_date}_0{leveln_collection}_T1_SR_B3.TIF",
                )
            },
            "swir_1": {
                "path": offset(
                    l2_c2_ls8_folder,
                    f"LT05_L2SP_090084_19980308_{processing_date}_0{leveln_collection}_T1_SR_B5.TIF",
                )
            },
            "swir_2": {
                "path": offset(
                    l2_c2_ls8_folder,
                    f"LT05_L2SP_090084_19980308_{processing_date}_0{leveln_collection}_T1_SR_B7.TIF",
                )
            },
        },
        "accessories": {
            "metadata:landsat_mtl": {
                "path": f"LT05_L2SP_090084_19980308_{processing_date}_0{leveln_collection}_T1_MTL.txt"
            }
        },
        "lineage": lineage or {},
    }


def expected_le07_l2_c2_folder(
    l2_c2_ls8_folder: Path = None,
    offset: Callable[[Path, str], str] = relative_offset,
    organisation="usgs.gov",
    collection="2",
    leveln_collection="2",
    lineage=None,
):
    """ """
    org_code = organisation.split(".")[0]
    product_name = f"{org_code}_ls7e_level{leveln_collection}_{collection}"
    processing_datetime = datetime(2021, 4, 26, 10, 52, 29)
    cloud_cover = 6.0
    points_model = 1240
    points_version = 5
    rmse_model_x = 3.08
    rmse_model_y = 3.663
    software_version = "LPGS_15.4.0"
    uuid = "2184a390-3bfc-5393-91fa-e9dae7c3fe39"
    quality_tag = "QA_PIXEL"
    processing_date = processing_datetime.strftime("%Y%m%d")
    return {
        "$schema": "https://schemas.opendatacube.org/dataset",
        "id": uuid,
        "label": f"{product_name}-0-{processing_date}_090084_2021-03-31",
        "product": {
            "name": product_name,
            "href": f"https://collections.dea.ga.gov.au/product/{product_name}",
        },
        "properties": {
            "datetime": datetime(2021, 3, 31, 23, 1, 59, 738020),
            # The minor version comes from the processing date,
            # as used in filenames to distinguish reprocesses.
            "odc:dataset_version": f"{collection}.0.{processing_date}",
            "odc:file_format": "GeoTIFF",
            "odc:processing_datetime": processing_datetime,
            "odc:producer": organisation,
            "odc:product_family": "level2",
            "odc:region_code": "090084",
            "eo:cloud_cover": cloud_cover,
            "eo:gsd": 30.0,
            "eo:instrument": "ETM",
            "eo:platform": "landsat-7",
            "eo:sun_azimuth": 57.028_331_7,
            "eo:sun_elevation": 31.970_873_9,
            "landsat:algorithm_source_surface_reflectance": "LEDAPS_3.4.0",
            "landsat:collection_category": "T1",
            "landsat:collection_number": int(leveln_collection),
            "landsat:data_type": "L2SP",
            "landsat:geometric_rmse_model_x": rmse_model_x,
            "landsat:geometric_rmse_model_y": rmse_model_y,
            "landsat:ground_control_points_model": points_model,
            "landsat:ground_control_points_version": points_version,
            "landsat:landsat_product_id": f"LE07_L2SP_090084_20210331_{processing_date}_0{leveln_collection}_T1",
            "landsat:landsat_scene_id": "LE70900842021090ASA00",
            "landsat:processing_software_version": software_version,
            "landsat:station_id": "ASA",
            "landsat:wrs_path": 90,
            "landsat:wrs_row": 84,
        },
        "crs": "epsg:32655",
        "geometry": {
            "coordinates": [
                [
                    [691984.3611711152, -3725685.0],
                    [681093.954439248, -3750395.5802668664],
                    [643185.0, -3904314.5],
                    [671569.7291070349, -3915142.15448428],
                    [825623.838894661, -3944415.0],
                    [846655.2670137166, -3944415.0],
                    [857285.0835718605, -3923426.16362107],
                    [895215.0, -3765785.5],
                    [870923.2163880672, -3754935.8100720993],
                    [713242.5838228005, -3725685.0],
                    [691984.3611711152, -3725685.0],
                ],
            ],
            "type": "Polygon",
        },
        "grids": {
            "default": {
                "shape": (60, 60),
                "transform": (
                    4200.5,
                    0.0,
                    643185.0,
                    0.0,
                    -3645.5,
                    -3725685.0,
                    0.0,
                    0.0,
                    1.0,
                ),
            },
        },
        "measurements": {
            "atmos_opacity": {
                "path": offset(
                    l2_c2_ls8_folder,
                    f"LE07_L2SP_090084_20210331_{processing_date}_0{leveln_collection}_T1_SR_ATMOS_OPACITY.TIF",
                )
            },
            "blue": {
                "path": offset(
                    l2_c2_ls8_folder,
                    f"LE07_L2SP_090084_20210331_{processing_date}_0{leveln_collection}_T1_SR_B1.TIF",
                )
            },
            "green": {
                "path": offset(
                    l2_c2_ls8_folder,
                    f"LE07_L2SP_090084_20210331_{processing_date}_0{leveln_collection}_T1_SR_B2.TIF",
                )
            },
            "lwir": {
                "path": offset(
                    l2_c2_ls8_folder,
                    f"LE07_L2SP_090084_20210331_{processing_date}_0{leveln_collection}_T1_ST_B6.TIF",
                )
            },
            "nir": {
                "path": offset(
                    l2_c2_ls8_folder,
                    f"LE07_L2SP_090084_20210331_{processing_date}_0{leveln_collection}_T1_SR_B4.TIF",
                )
            },
            "qa_cloud": {
                "path": offset(
                    l2_c2_ls8_folder,
                    f"LE07_L2SP_090084_20210331_{processing_date}_0{leveln_collection}_T1_SR_CLOUD_QA.TIF",
                )
            },
            "quality": {
                "path": offset(
                    l2_c2_ls8_folder,
                    f"LE07_L2SP_090084_20210331_{processing_date}_0{leveln_collection}_T1_{quality_tag}.TIF",
                )
            },
            "red": {
                "path": offset(
                    l2_c2_ls8_folder,
                    f"LE07_L2SP_090084_20210331_{processing_date}_0{leveln_collection}_T1_SR_B3.TIF",
                )
            },
            "swir_1": {
                "path": offset(
                    l2_c2_ls8_folder,
                    f"LE07_L2SP_090084_20210331_{processing_date}_0{leveln_collection}_T1_SR_B5.TIF",
                )
            },
            "swir_2": {
                "path": offset(
                    l2_c2_ls8_folder,
                    f"LE07_L2SP_090084_20210331_{processing_date}_0{leveln_collection}_T1_SR_B7.TIF",
                )
            },
        },
        "accessories": {
            "metadata:landsat_mtl": {
                "path": f"LE07_L2SP_090084_20210331_{processing_date}_0{leveln_collection}_T1_MTL.txt"
            }
        },
        "lineage": lineage or {},
    }
