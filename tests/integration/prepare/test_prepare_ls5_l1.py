from datetime import datetime
from pathlib import Path

from .common import check_prepare_outputs
from eodatasets.prepare import ls_usgs_l1_prepare
from eodatasets.prepare.ls_usgs_l1_prepare import normalise_nci_symlinks

L1_TARBALL_PATH: Path = Path(
    __file__
).parent / "data" / "LT05_L1TP_090085_19970406_20161231_01_T1.tar.gz"


def test_prepare_l5_l1_usgs_tarball(tmpdir):
    assert L1_TARBALL_PATH.exists(), "Test data missing(?)"

    output_path = Path(tmpdir)
    expected_metadata_path = (
        output_path / "LT05_L1TP_090085_19970406_20161231_01_T1.yaml"
    )

    def path_offset(offset: str):
        return (
            "tar:"
            + str(normalise_nci_symlinks(L1_TARBALL_PATH.absolute()))
            + "!"
            + offset
        )

    expected_doc = {
        "id": "b0d31709-dda4-5a67-9fdf-3ae026a99a72",
        "product": {"name": "usgs_ls5-t_level1_3"},
        "crs": "epsg:32655",
        "datetime": datetime(1997, 4, 6, 23, 17, 43, 102000),
        'creation_datetime': datetime(2016, 12, 31, 15, 54, 58),
        "file_format": "GeoTIFF",
        "properties": {
            "eo:cloud_cover": 27.0,
            "eo:gsd": 30.0,
            "eo:instrument": "TM",
            "eo:platform": "landsat-5",
            "eo:sun_azimuth": 51.25454223,
            "eo:sun_elevation": 31.98763219,
            'landsat:collection_category': 'T1',
            'landsat:collection_number': 1,
            'landsat:geometric_rmse_model_x': 3.036,
            'landsat:geometric_rmse_model_y': 3.025,
            'landsat:geometric_rmse_verify': 0.163,
            'landsat:ground_control_points_model': 161,
            'landsat:ground_control_points_verify': 1679,
            'landsat:ground_control_points_version': 4,
            'landsat:wrs_path': 90,
            'landsat:wrs_row': 85,
        },
        "user_data": {
            "data_type": "L1TP",
            "ephemeris_type": "PREDICTIVE",
            "landsat_product_id": "LT05_L1TP_090085_19970406_20161231_01_T1",
            "landsat_scene_id": "LT50900851997096ASA00",
            "processing_software_version": "LPGS_12.8.2",
            "station_id": "ASA",
        },
        "geometry": {
            "coordinates": [
                [
                    [835815.0, -3881685.0],
                    [593385.0, -3881685.0],
                    [593385.0, -4101015.0],
                    [835815.0, -4101015.0],
                    [835815.0, -3881685.0],
                ]
            ],
            "type": "Polygon",
        },
        "grids": {
            "default": {
                "shape": [60, 60],
                "transform": [
                    4040.5,
                    0.0,
                    593385.0,
                    0.0,
                    -3655.5,
                    -3881685.0,
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
                "path": path_offset("LT05_L1TP_090085_19970406_20161231_01_T1_B1.TIF"),
            },
            "green": {
                "band": "green",
                "grid": "default",
                "layer": "1",
                "path": path_offset("LT05_L1TP_090085_19970406_20161231_01_T1_B2.TIF"),
            },
            "nir": {
                "band": "nir",
                "grid": "default",
                "layer": "1",
                "path": path_offset("LT05_L1TP_090085_19970406_20161231_01_T1_B4.TIF"),
            },
            "quality": {
                "band": "quality",
                "grid": "default",
                "layer": "1",
                "path": path_offset("LT05_L1TP_090085_19970406_20161231_01_T1_BQA.TIF"),
            },
            "red": {
                "band": "red",
                "grid": "default",
                "layer": "1",
                "path": path_offset("LT05_L1TP_090085_19970406_20161231_01_T1_B3.TIF"),
            },
            "swir1": {
                "band": "swir1",
                "grid": "default",
                "layer": "1",
                "path": path_offset("LT05_L1TP_090085_19970406_20161231_01_T1_B5.TIF"),
            },
            "swir2": {
                "band": "swir2",
                "grid": "default",
                "layer": "1",
                "path": path_offset("LT05_L1TP_090085_19970406_20161231_01_T1_B7.TIF"),
            },
        },
        "lineage": {},
    }
    check_prepare_outputs(
        invoke_script=ls_usgs_l1_prepare.main,
        run_args=[
            "--absolute-paths",
            "--output",
            str(output_path),
            str(L1_TARBALL_PATH),
        ],
        expected_doc=expected_doc,
        expected_metadata_path=expected_metadata_path,
    )
