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
        "product": {"href": "https://collections.dea.ga.gov.au/usgs_ls5t_level1_1"},
        "bbox": [
            148.02427805478845,
            -37.05072319440303,
            150.77322846827445,
            -35.022069880628756,
        ],
        "crs": "epsg:32655",
        "properties": {
            "datetime": datetime(1997, 4, 6, 23, 17, 43, 102000),
            "odc:creation_datetime": datetime(2016, 12, 31, 15, 54, 58),
            "odc:file_format": "GeoTIFF",
            "eo:cloud_cover": 27.0,
            "eo:gsd": 30.0,
            "eo:instrument": "TM",
            "eo:platform": "landsat-5",
            "eo:sun_azimuth": 51.25454223,
            "eo:sun_elevation": 31.98763219,
            "landsat:collection_category": "T1",
            "landsat:collection_number": 1,
            "landsat:geometric_rmse_model_x": 3.036,
            "landsat:geometric_rmse_model_y": 3.025,
            "landsat:geometric_rmse_verify": 0.163,
            "landsat:ground_control_points_model": 161,
            "landsat:ground_control_points_verify": 1679,
            "landsat:ground_control_points_version": 4,
            "landsat:wrs_path": 90,
            "landsat:wrs_row": 85,
            "landsat:data_type": "L1TP",
            "landsat:ephemeris_type": "PREDICTIVE",
            "landsat:landsat_product_id": "LT05_L1TP_090085_19970406_20161231_01_T1",
            "landsat:landsat_scene_id": "LT50900851997096ASA00",
            "landsat:processing_software_version": "LPGS_12.8.2",
            "landsat:station_id": "ASA",
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
                "path": path_offset("LT05_L1TP_090085_19970406_20161231_01_T1_B1.TIF")
            },
            "green": {
                "path": path_offset("LT05_L1TP_090085_19970406_20161231_01_T1_B2.TIF")
            },
            "red": {
                "path": path_offset("LT05_L1TP_090085_19970406_20161231_01_T1_B3.TIF")
            },
            "nir": {
                "path": path_offset("LT05_L1TP_090085_19970406_20161231_01_T1_B4.TIF")
            },
            "swir1": {
                "path": path_offset("LT05_L1TP_090085_19970406_20161231_01_T1_B5.TIF")
            },
            "swir2": {
                "path": path_offset("LT05_L1TP_090085_19970406_20161231_01_T1_B7.TIF")
            },
            "quality": {
                "path": path_offset("LT05_L1TP_090085_19970406_20161231_01_T1_BQA.TIF")
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
