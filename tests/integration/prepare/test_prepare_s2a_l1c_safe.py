from pathlib import Path

from eodatasets3.prepare import s2_prepare_cophub_zip
from tests.integration.common import check_prepare_outputs

L1_ZIPFILE_PATH: Path = (
    Path(__file__).parent.parent
    / "data"
    / "S2A_MSIL1C_20180629T000241_N0206_R030_T56JMM_20180629T012042.zip"
)


def test_prepare_s2a_l1c_safe(tmpdir):
    assert L1_ZIPFILE_PATH.exists(), "Test data missing(?)"

    output_path = Path(tmpdir)
    expected_metadata_path = (
        output_path
        / "S2A_MSIL1C_20180629T000241_N0206_R030_T56JMM_20180629T012042.zip.yaml"
    )
    granule_path = (
        "/S2A_MSIL1C_20180629T000241_N0206_R030_T56JMM_20180629T012042.SAFE"
        "/GRANULE/L1C_T56JMM_A015757_20180629T000241/"
    )

    def path_offset(offset: str, granule_path=granule_path):
        return "zip://" + str(L1_ZIPFILE_PATH.absolute()) + "!" + granule_path + offset

    expected_doc = {
        "acquisition": {"groundstation": {"code": "EPA_"}},
        "archiving_time": "2018-06-29T03:44:20.252114Z",
        "checksum_sha1": "908571e2cdd54174fb051272be34fe73f6efb9a5",
        "creation_dt": "2018-06-29T01:20:42.000000Z",
        "datastrip_id": "S2A_OPER_MSI_L1C_DS_EPAE_20180629T012042_S20180629T000241_N02.06",
        "datastrip_metadata": (
            "S2A_MSIL1C_20180629T000241_N0206_R030_T56JMM_20180629T012042.SAFE"
            "/DATASTRIP/DS_EPAE_20180629T012042_S20180629T000241/MTD_DS.xml"
        ),
        "datatake_id": {"datatakeIdentifier": "GS2A_20180629T000241_015757_N02.06"},
        "datatake_sensing_start": "2018-06-29T00:02:41.024Z",
        "datatake_type": "INS-NOBS",
        "downlink_priority": "NOMINAL",
        "extent": {
            "center_dt": "2018-06-29T00:02:41.461Z",
            "coord": {
                "ll": {"lat": -30.81709901611161, "lon": 151.95410707081012},
                "lr": {"lat": -30.82128003446065, "lon": 153.10204544208213},
                "ul": {"lat": -29.82640952246035, "lon": 151.9645797990478},
                "ur": {"lat": -29.83042869279505, "lon": 153.10102341983713},
            },
            "from_dt": "2018-06-29T00:02:41.024Z",
            "to_dt": "2018-06-29T00:02:41.024Z",
        },
        "format": {"name": "JPEG2000"},
        "grid_spatial": {
            "projection": {
                "geo_ref_points": {
                    "ll": {"x": 399960, "y": 6590200},
                    "lr": {"x": 509760, "y": 6590200},
                    "ul": {"x": 399960, "y": 6700000},
                    "ur": {"x": 509760, "y": 6700000},
                },
                "spatial_reference": "EPSG:32756",
                "valid_data": {
                    "coordinates": [
                        [
                            [470826.97121060337, 6590200.0],
                            [475130.9086383948, 6598904.030892345],
                            [493180.2237068879, 6681630.058289605],
                            [493214.79452054796, 6700000.0],
                            [399960.0, 6700000.0],
                            [399960.0, 6590200.0],
                            [470826.97121060337, 6590200.0],
                        ]
                    ],
                    "type": "Polygon",
                },
            }
        },
        "id": "d6004c21-171f-5a76-8e4f-2de359af506a",
        "image": {
            "bands": {
                "B01": {
                    "layer": 1,
                    "path": path_offset("IMG_DATA/T56JMM_20180629T000241_B01.jp2"),
                },
                "B02": {
                    "layer": 1,
                    "path": path_offset("IMG_DATA/T56JMM_20180629T000241_B02.jp2"),
                },
                "B03": {
                    "layer": 1,
                    "path": path_offset("IMG_DATA/T56JMM_20180629T000241_B03.jp2"),
                },
                "B04": {
                    "layer": 1,
                    "path": path_offset("IMG_DATA/T56JMM_20180629T000241_B04.jp2"),
                },
                "B05": {
                    "layer": 1,
                    "path": path_offset("IMG_DATA/T56JMM_20180629T000241_B05.jp2"),
                },
                "B06": {
                    "layer": 1,
                    "path": path_offset("IMG_DATA/T56JMM_20180629T000241_B06.jp2"),
                },
                "B07": {
                    "layer": 1,
                    "path": path_offset("IMG_DATA/T56JMM_20180629T000241_B07.jp2"),
                },
                "B08": {
                    "layer": 1,
                    "path": path_offset("IMG_DATA/T56JMM_20180629T000241_B08.jp2"),
                },
                "B09": {
                    "layer": 1,
                    "path": path_offset("IMG_DATA/T56JMM_20180629T000241_B09.jp2"),
                },
                "B10": {
                    "layer": 1,
                    "path": path_offset("IMG_DATA/T56JMM_20180629T000241_B10.jp2"),
                },
                "B11": {
                    "layer": 1,
                    "path": path_offset("IMG_DATA/T56JMM_20180629T000241_B11.jp2"),
                },
                "B12": {
                    "layer": 1,
                    "path": path_offset("IMG_DATA/T56JMM_20180629T000241_B12.jp2"),
                },
                "B8A": {
                    "layer": 1,
                    "path": path_offset("IMG_DATA/T56JMM_20180629T000241_B8A.jp2"),
                },
                "PVI": {
                    "layer": 1,
                    "path": path_offset("QI_DATA/T56JMM_20180629T000241_PVI.jp2"),
                },
                "TCI": {
                    "layer": 1,
                    "path": path_offset("IMG_DATA/T56JMM_20180629T000241_TCI.jp2"),
                },
            },
            "cloud_cover_percentage": 0.0,
            "degraded_anc_data_percentage": 0.0,
            "degraded_msi_data_percentage": 0.0,
            "format_quality_flag": "",
            "general_quality_flag": "",
            "geometric_quality_flag": "",
            "null_value": "0",
            "radiometric_quality_flag": "",
            "reflectance_conversion": "0.967798898595979",
            "saturated": "65535",
            "sensor_quality_flag": "",
            "solar_irradiance": [
                {"bandId": "0", "unit": "W/m²/µm", "value": "1884.69"},
                {"bandId": "1", "unit": "W/m²/µm", "value": "1959.72"},
                {"bandId": "2", "unit": "W/m²/µm", "value": "1823.24"},
                {"bandId": "3", "unit": "W/m²/µm", "value": "1512.06"},
                {"bandId": "4", "unit": "W/m²/µm", "value": "1424.64"},
                {"bandId": "5", "unit": "W/m²/µm", "value": "1287.61"},
                {"bandId": "6", "unit": "W/m²/µm", "value": "1162.08"},
                {"bandId": "7", "unit": "W/m²/µm", "value": "1041.63"},
                {"bandId": "8", "unit": "W/m²/µm", "value": "955.32"},
                {"bandId": "9", "unit": "W/m²/µm", "value": "812.92"},
                {"bandId": "10", "unit": "W/m²/µm", "value": "367.15"},
                {"bandId": "11", "unit": "W/m²/µm", "value": "245.59"},
                {"bandId": "12", "unit": "W/m²/µm", "value": "85.25"},
            ],
            "sun_azimuth": 28.9784209875988,
            "sun_elevation": 59.5161129280706,
            "tile_reference": "N02.06",
            "viewing_angles": [
                {
                    "bandId": "0",
                    "measurement": {
                        "azimuth": {"value": 287.743300570742},
                        "zenith": {"value": 8.90682426360539},
                    },
                    "unit": "degree",
                },
                {
                    "bandId": "9",
                    "measurement": {
                        "azimuth": {"value": 287.913358161721},
                        "zenith": {"value": 8.93763578589198},
                    },
                    "unit": "degree",
                },
                {
                    "bandId": "10",
                    "measurement": {
                        "azimuth": {"value": 286.62269660653},
                        "zenith": {"value": 8.77437299338626},
                    },
                    "unit": "degree",
                },
                {
                    "bandId": "1",
                    "measurement": {
                        "azimuth": {"value": 286.085976995274},
                        "zenith": {"value": 8.70023786261715},
                    },
                    "unit": "degree",
                },
                {
                    "bandId": "2",
                    "measurement": {
                        "azimuth": {"value": 286.471239179063},
                        "zenith": {"value": 8.72476018739389},
                    },
                    "unit": "degree",
                },
                {
                    "bandId": "3",
                    "measurement": {
                        "azimuth": {"value": 286.832768592131},
                        "zenith": {"value": 8.75574267151839},
                    },
                    "unit": "degree",
                },
                {
                    "bandId": "4",
                    "measurement": {
                        "azimuth": {"value": 287.013584765891},
                        "zenith": {"value": 8.77685536841587},
                    },
                    "unit": "degree",
                },
                {
                    "bandId": "5",
                    "measurement": {
                        "azimuth": {"value": 287.207324001684},
                        "zenith": {"value": 8.80050900146543},
                    },
                    "unit": "degree",
                },
                {
                    "bandId": "6",
                    "measurement": {
                        "azimuth": {"value": 287.400419388821},
                        "zenith": {"value": 8.8344918122903},
                    },
                    "unit": "degree",
                },
                {
                    "bandId": "7",
                    "measurement": {
                        "azimuth": {"value": 286.272771889735},
                        "zenith": {"value": 8.71220887022109},
                    },
                    "unit": "degree",
                },
                {
                    "bandId": "8",
                    "measurement": {
                        "azimuth": {"value": 287.592709087249},
                        "zenith": {"value": 8.86277189674952},
                    },
                    "unit": "degree",
                },
                {
                    "bandId": "11",
                    "measurement": {
                        "azimuth": {"value": 287.100489102096},
                        "zenith": {"value": 8.84204098088515},
                    },
                    "unit": "degree",
                },
                {
                    "bandId": "12",
                    "measurement": {
                        "azimuth": {"value": 287.498739900102},
                        "zenith": {"value": 8.90079212851159},
                    },
                    "unit": "degree",
                },
            ],
        },
        "instrument": {"name": "MSI"},
        "lineage": {"source_datasets": {}},
        "orbit": "30",
        "orbit_direction": "DESCENDING",
        "platform": {"code": "Sentinel-2A"},
        "processing_baseline": "02.06",
        "processing_level": "Level-1C",
        "product_format": {"name": "SAFE_COMPACT"},
        "product_type": "S2MSI1C",
        "product_uri": "S2A_MSIL1C_20180629T000241_N0206_R030_T56JMM_20180629T012042.SAFE",
        "size_bytes": 1837225,
        "tile_id": "S2A_OPER_MSI_L1C_TL_EPAE_20180629T012042_A015757_T56JMM_N02.06",
    }

    check_prepare_outputs(
        invoke_script=s2_prepare_cophub_zip.main,
        run_args=[
            "--newer-than",
            "1970-01-01",
            "--output",
            str(output_path),
            str(L1_ZIPFILE_PATH),
        ],
        expected_doc=expected_doc,
        expected_metadata_path=expected_metadata_path,
    )
