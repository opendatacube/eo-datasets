from datetime import datetime
from pathlib import Path

from .common import run_prepare_cli, check_prepare_outputs
from eodatasets.prepare import ls_usgs_l1_prepare
from eodatasets.prepare.ls_usgs_l1_prepare import normalise_nci_symlinks

L71GT_TARBALL_PATH: Path = Path(
    __file__
).parent / "data" / "LE07_L1TP_104078_20130429_20161124_01_T1.tar"


def test_prepare_l7_l1_usgs_tarball(tmpdir):
    assert L71GT_TARBALL_PATH.exists(), "Test data missing(?)"

    output_path = Path(tmpdir)
    expected_metadata_path = (
        output_path / "LE07_L1TP_104078_20130429_20161124_01_T1.yaml"
    )

    def path_offset(offset: str):
        return (
            "tar:"
            + str(normalise_nci_symlinks(L71GT_TARBALL_PATH.absolute()))
            + "!"
            + offset
        )

    expected_doc = {
        "id": "f23c5fa2-3321-5be9-9872-2be73fee12a6",
        "crs": "epsg:32652",
        "datetime": datetime(2013, 4, 29, 1, 10, 20, 336104),
        'creation_datetime': datetime(2016, 11, 24, 8, 26, 33),
        "file_format": "GeoTIFF",
        "product": {"name": "usgs_ls7-e_level1_3"},
        "properties": {
            "eo:cloud_cover": 0.0,
            "eo:gsd": 30.0,
            "eo:instrument": "ETM",
            "eo:platform": "landsat-7",
            "eo:sun_azimuth": 40.56298198,
            "eo:sun_elevation": 39.37440872,
            'landsat:collection_category': 'T1',
            'landsat:collection_number': 1,
            'landsat:geometric_rmse_model_x': 2.752,
            'landsat:geometric_rmse_model_y': 3.115,
            'landsat:ground_control_points_model': 179,
            'landsat:ground_control_points_version': 4,
            'landsat:wrs_path': 104,
            'landsat:wrs_row': 78,
        },
        "user_data": {
            "data_type": "L1TP",
            "ephemeris_type": "DEFINITIVE",
            "landsat_product_id": "LE07_L1TP_104078_20130429_20161124_01_T1",
            "landsat_scene_id": "LE71040782013119ASA00",
            "processing_software_version": "LPGS_12.8.2",
            "station_id": "ASA",
        },
        "geometry": {
            "coordinates": [
                [
                    [770115.0, -2768985.0],
                    [525285.0, -2768985.0],
                    [525285.0, -2981715.0],
                    [770115.0, -2981715.0],
                    [770115.0, -2768985.0],
                ]
            ],
            "type": "Polygon",
        },
        "grids": {
            "default": {
                "shape": [60, 60],
                "transform": [
                    4080.5000000000005,
                    0.0,
                    525285.0,
                    0.0,
                    -3545.5,
                    -2768985.0,
                    0.0,
                    0.0,
                    1.0,
                ],
            }
        },
        "measurements": {
            "blue": {
                "band": "blue",
                "grid": "default",
                "layer": "1",
                "path": path_offset("LE07_L1TP_104078_20130429_20161124_01_T1_B1.TIF"),
            },
            "green": {
                "band": "green",
                "grid": "default",
                "layer": "1",
                "path": path_offset("LE07_L1TP_104078_20130429_20161124_01_T1_B2.TIF"),
            },
            "nir": {
                "band": "nir",
                "grid": "default",
                "layer": "1",
                "path": path_offset("LE07_L1TP_104078_20130429_20161124_01_T1_B4.TIF"),
            },
            "quality": {
                "band": "quality",
                "grid": "default",
                "layer": "1",
                "path": path_offset("LE07_L1TP_104078_20130429_20161124_01_T1_BQA.TIF"),
            },
            "red": {
                "band": "red",
                "grid": "default",
                "layer": "1",
                "path": path_offset("LE07_L1TP_104078_20130429_20161124_01_T1_B3.TIF"),
            },
            "swir1": {
                "band": "swir1",
                "grid": "default",
                "layer": "1",
                "path": path_offset("LE07_L1TP_104078_20130429_20161124_01_T1_B5.TIF"),
            },
            "swir2": {
                "band": "swir2",
                "grid": "default",
                "layer": "1",
                "path": path_offset("LE07_L1TP_104078_20130429_20161124_01_T1_B7.TIF"),
            },
        },
        "lineage": {},
    }
    print(expected_doc)
    check_prepare_outputs(
        invoke_script=ls_usgs_l1_prepare.main,
        run_args=[
            "--absolute-paths",
            "--output",
            str(output_path),
            str(L71GT_TARBALL_PATH),
        ],
        expected_doc=expected_doc,
        expected_metadata_path=expected_metadata_path,
    )


def test_skips_old_datasets(tmpdir):
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
        str(L71GT_TARBALL_PATH),
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
        str(L71GT_TARBALL_PATH),
    )
    assert (
        expected_metadata_path.exists()
    ), "Dataset should have been packaged when using an ancient date cutoff"
