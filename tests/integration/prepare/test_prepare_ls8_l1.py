from datetime import datetime
from pathlib import Path
from textwrap import dedent

from .common import check_prepare_outputs
from eodatasets.prepare import ls_usgs_l1_prepare
from eodatasets.prepare.ls_usgs_l1_prepare import normalise_nci_symlinks

L1_INPUT_PATH: Path = Path(
    __file__
).parent / "data" / "LC08_L1TP_090084_20160121_20170405_01_T1"


def test_prepare_l8_l1_usgs_tarball(tmpdir):
    assert L1_INPUT_PATH.exists(), "Test data missing(?)"

    output_path = Path(tmpdir)
    expected_metadata_path = (
        output_path / "LC08_L1TP_090084_20160121_20170405_01_T1.yaml"
    )

    def path_offset(offset: str):
        return str(normalise_nci_symlinks(L1_INPUT_PATH.absolute().joinpath(offset)))

    expected_doc = {
        "id": "a780754e-a884-58a7-9ac0-df518a67f59d",
        "datetime": datetime(2016, 1, 21, 23, 50, 23, 54435),
        'creation_datetime': datetime(2017, 4, 5, 11, 17, 36),
        "file_format": "GeoTIFF",
        "product": {"href": "https://collections.dea.ga.gov.au/usgs_ls8o_level1_1"},
        "properties": {
            "eo:cloud_cover": 93.22,
            "eo:gsd": 30.0,
            "eo:instrument": "OLI_TIRS",
            "eo:platform": "landsat-8",
            "eo:sun_azimuth": 74.0074438,
            "eo:sun_elevation": 55.486483,
            'landsat:collection_category': 'T1',
            'landsat:collection_number': 1,
            'landsat:geometric_rmse_model_x': 4.593,
            'landsat:geometric_rmse_model_y': 5.817,
            'landsat:ground_control_points_model': 66,
            'landsat:ground_control_points_version': 4,
            'landsat:wrs_path': 90,
            'landsat:wrs_row': 84,
        },
        "user_data": {
            "data_type": "L1TP",
            "landsat_product_id": "LC08_L1TP_090084_20160121_20170405_01_T1",
            "landsat_scene_id": "LC80900842016021LGN02",
            "processing_software_version": "LPGS_2.7.0",
            "station_id": "LGN",
        },
        "crs": "epsg:32655",
        "geometry": {
            "coordinates": [
                [
                    [879315.0, -3714585.0],
                    [641985.0, -3714585.0],
                    [641985.0, -3953115.0],
                    [879315.0, -3953115.0],
                    [879315.0, -3714585.0],
                ]
            ],
            "type": "Polygon",
        },
        "grids": {
            "default": {
                "shape": [60, 60],
                "transform": [
                    3955.5,
                    0.0,
                    641985.0,
                    0.0,
                    -3975.5000000000005,
                    -3714585.0,
                    0.0,
                    0.0,
                    1.0,
                ],
            },
            "panchromatic": {
                "shape": [60, 60],
                "transform": [
                    3955.25,
                    0.0,
                    641992.5,
                    0.0,
                    -3975.25,
                    -3714592.5,
                    0.0,
                    0.0,
                    1.0,
                ],
            },
        },
        "measurements": {
            "blue": {
                "path": path_offset("LC08_L1TP_090084_20160121_20170405_01_T1_B2.TIF"),
            },
            "cirrus": {

                "path": path_offset("LC08_L1TP_090084_20160121_20170405_01_T1_B9.TIF"),
            },
            "coastal_aerosol": {

                "path": path_offset("LC08_L1TP_090084_20160121_20170405_01_T1_B1.TIF"),
            },
            "green": {

                "path": path_offset("LC08_L1TP_090084_20160121_20170405_01_T1_B3.TIF"),
            },
            "lwir1": {

                "path": path_offset("LC08_L1TP_090084_20160121_20170405_01_T1_B10.TIF"),
            },
            "lwir2": {

                "path": path_offset("LC08_L1TP_090084_20160121_20170405_01_T1_B11.TIF"),
            },
            "nir": {

                "path": path_offset("LC08_L1TP_090084_20160121_20170405_01_T1_B5.TIF"),
            },
            "panchromatic": {

                "grid": "panchromatic",

                "path": path_offset("LC08_L1TP_090084_20160121_20170405_01_T1_B8.TIF"),
            },
            "quality": {

                "path": path_offset("LC08_L1TP_090084_20160121_20170405_01_T1_BQA.TIF"),
            },
            "red": {

                "path": path_offset("LC08_L1TP_090084_20160121_20170405_01_T1_B4.TIF"),
            },
            "swir1": {

                "path": path_offset("LC08_L1TP_090084_20160121_20170405_01_T1_B6.TIF"),
            },
            "swir2": {

                "path": path_offset("LC08_L1TP_090084_20160121_20170405_01_T1_B7.TIF"),
            },
        },
        "lineage": {},
    }

    check_prepare_outputs(
        invoke_script=ls_usgs_l1_prepare.main,
        run_args=["--absolute-paths", "--output", str(output_path), str(L1_INPUT_PATH)],
        expected_doc=expected_doc,
        expected_metadata_path=expected_metadata_path,
    )

    checksum_file = L1_INPUT_PATH / "package.sha1"
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
