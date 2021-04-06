from datetime import datetime
from pathlib import Path
from typing import Dict

from eodatasets3.prepare import landsat_l1_prepare
from tests.common import check_prepare_outputs
from tests.common import run_prepare_cli


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
