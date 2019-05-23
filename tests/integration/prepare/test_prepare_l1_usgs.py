from datetime import datetime
from pathlib import Path
from textwrap import dedent
from typing import Dict

from eodatasets.prepare import ls_usgs_l1_prepare
from .common import check_prepare_outputs
from .common import run_prepare_cli


def test_prepare_l5_l1_usgs_tarball(tmpdir, l1_ls5_tarball_md_expected, l1_ls5_tarball):
    assert l1_ls5_tarball.exists(), "Test data missing(?)"

    output_path = Path(tmpdir)
    expected_metadata_path = (
        output_path / "LT05_L1TP_090085_19970406_20161231_01_T1.yaml"
    )

    check_prepare_outputs(
        invoke_script=ls_usgs_l1_prepare.main,
        run_args=[
            "--absolute-paths",
            "--output",
            str(output_path),
            str(l1_ls5_tarball),
        ],
        expected_doc=l1_ls5_tarball_md_expected,
        expected_metadata_path=expected_metadata_path,
    )


def test_prepare_l8_l1_usgs_tarball(tmpdir, l1_ls8_folder, l1_ls8_folder_md_expected):
    assert l1_ls8_folder.exists(), "Test data missing(?)"

    output_path = Path(tmpdir)
    expected_metadata_path = (
        output_path / "LC08_L1TP_090084_20160121_20170405_01_T1.yaml"
    )

    check_prepare_outputs(
        invoke_script=ls_usgs_l1_prepare.main,
        run_args=["--absolute-paths", "--output", str(output_path), str(l1_ls8_folder)],
        expected_doc=l1_ls8_folder_md_expected,
        expected_metadata_path=expected_metadata_path,
    )

    checksum_file = l1_ls8_folder / "package.sha1"
    assert checksum_file.read_text() == dedent(
        """\
        921a20d85696d0267533d2810ba0d9d39a7cbd56	LC08_L1TP_090084_20160121_20170405_01_T1_ANG.txt
        eae60de697ddefd83171d2ecf7e9d7a87d782b05	LC08_L1TP_090084_20160121_20170405_01_T1_B1.TIF
        e86c475d6d8aa0224fc5239b1264533377b71140	LC08_L1TP_090084_20160121_20170405_01_T1_B10.TIF
        8c2ba78c8ba2a0c37638d01148a49e47fd890f66	LC08_L1TP_090084_20160121_20170405_01_T1_B11.TIF
        ca0247b270ee166bdd88e40f3c611c192d52b14b	LC08_L1TP_090084_20160121_20170405_01_T1_B2.TIF
        00e2cb5f0ba666758c9710cb794f5123456ab1f6	LC08_L1TP_090084_20160121_20170405_01_T1_B3.TIF
        7ba3952d33272d78ff21d6db4b964e954f21741b	LC08_L1TP_090084_20160121_20170405_01_T1_B4.TIF
        790e58ca6198342a6da695ad1bb04343ab5de745	LC08_L1TP_090084_20160121_20170405_01_T1_B5.TIF
        b1305bb8c03dd0865e7b8fced505e47144a07319	LC08_L1TP_090084_20160121_20170405_01_T1_B6.TIF
        9858a25a8ce343a8b8c39076048311ca101aeb85	LC08_L1TP_090084_20160121_20170405_01_T1_B7.TIF
        91a953ab1aec86d2676da973628948fd4843bad0	LC08_L1TP_090084_20160121_20170405_01_T1_B8.TIF
        fa56fdd77be655cc4e4e7b4db5333c2260c1c922	LC08_L1TP_090084_20160121_20170405_01_T1_B9.TIF
        2bd7a30e6cd0e17870ef05d128379296d8babf7e	LC08_L1TP_090084_20160121_20170405_01_T1_BQA.TIF
        2d1878ba89840d1942bc3ff273fb09bbf4917af3	LC08_L1TP_090084_20160121_20170405_01_T1_MTL.txt
    """
    )


def test_prepare_l7_l1_usgs_tarball(
    tmpdir, l1_ls7_tarball: Path, l1_ls7_tarball_md_expected: Dict
):
    assert l1_ls7_tarball.exists(), "Test data missing(?)"

    output_path = Path(tmpdir)
    expected_metadata_path = (
        output_path / "LE07_L1TP_104078_20130429_20161124_01_T1.yaml"
    )

    check_prepare_outputs(
        invoke_script=ls_usgs_l1_prepare.main,
        run_args=[
            "--absolute-paths",
            "--output",
            str(output_path),
            str(l1_ls7_tarball),
        ],
        expected_doc=l1_ls7_tarball_md_expected,
        expected_metadata_path=expected_metadata_path,
    )


def test_skips_old_datasets(tmpdir, l1_ls7_tarball):
    """Prepare should skip datasets older than the given date"""
    output_path = Path(tmpdir)
    expected_metadata_path = (
        output_path / "LE07_L1TP_104078_20130429_20161124_01_T1.yaml"
    )

    run_prepare_cli(
        ls_usgs_l1_prepare.main,
        "--output",
        str(output_path),
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
        ls_usgs_l1_prepare.main,
        "--output",
        str(output_path),
        # Some old date, from before the test data was created.
        "--newer-than",
        "2014-05-04",
        str(l1_ls7_tarball),
    )
    assert (
        expected_metadata_path.exists()
    ), "Dataset should have been packaged when using an ancient date cutoff"
