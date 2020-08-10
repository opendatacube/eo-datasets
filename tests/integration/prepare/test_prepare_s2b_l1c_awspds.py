import zipfile
from pathlib import Path

from eodatasets3.prepare import s2_l1c_aws_pds_prepare
from tests.integration.common import check_prepare_outputs

L1_ZIPFILE_PATH: Path = (
    Path(__file__).parent.parent
    / "data"
    / "S2B_OPER_MSI_L1C_TL_EPAE_20180617T013729_A006677_T55JGF_N02.06.AWSPDS.zip"
)


def test_prepare_s2a_l1c_safe(tmpdir):
    assert L1_ZIPFILE_PATH.exists(), "Test data missing(?)"

    output_path = Path(tmpdir)

    # make a folder and extract contents
    output_path.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(L1_ZIPFILE_PATH, "r") as zipped_data:
        zipped_data.extractall(output_path)

    expected_metadata_path = (
        output_path
        / "S2B_OPER_MSI_L1C_TL_EPAE_20180617T013729_A006677_T55JGF_N02.06.yaml"
    )

    def path_offset(offset: str):
        return "s3://sentinel-s2-l1c/tiles/55/J/GF/2018/6/17/0/" + offset

    expected_doc = {
        "acquisition": {"groundstation": {"code": "EPA_"}},
        "archiving_time": "2018-06-17T02:06:21.973943Z",
        "creation_dt": "2018-06-17T01:27:55Z",
        "checksum_sha1": "bfac5eea3ec0c6c70816ddb97592dfd4d8a389e2",
        "datastrip_id": "S2B_OPER_MSI_L1C_DS_EPAE_20180617T013729_S20180617T001107_N02.06",
        "datastrip_metadata": (
            "http://sentinel-s2-l1c.s3-website.eu-central-1.amazonaws.com"
            "/#products/2018/6/17/S2B_MSIL1C_20180617T001109_N0206_R073_"
            "T55JGF_20180617T013729/datastrip/0"
        ),
        "datatake_id": {
            "datatakeIdentifier": "GS2B_20180617T001109_006677_N02.06",
            "metadataLevel": "Brief",
        },
        "datatake_sensing_start": "2018-06-17T00:11:09.024Z",
        "datatake_type": "INS-NOBS",
        "downlink_priority": "NOMINAL",
        "extent": {
            "center_dt": "2018-06-17T00:11:07.458Z",
            "coord": {
                "ll": {"lat": -31.705779763545088, "lon": 149.11005323208184},
                "lr": {"lat": -31.681364358256744, "lon": 150.26738162306842},
                "ul": {"lat": -30.715729325189965, "lon": 149.08817422152293},
                "ur": {"lat": -30.6922425616759, "lon": 150.23354450086148},
            },
        },
        "format": {"name": "JPEG2000"},
        "grid_spatial": {
            "projection": {
                "geo_ref_points": {
                    "ll": {"x": 699960, "y": 6490240},
                    "lr": {"x": 809760, "y": 6490240},
                    "ul": {"x": 699960, "y": 6600040},
                    "ur": {"x": 809760, "y": 6600040},
                },
                "spatial_reference": "EPSG:32755",
                "valid_data": {
                    "coordinates": [
                        [
                            [781030.5157393045, 6490240.0],
                            [802194.5431029584, 6568088.898470836],
                            [807815.3469119765, 6599599.456501017],
                            [807233.663302242, 6600040.0],
                            [699960.0, 6600040.0],
                            [699960.0, 6490240.0],
                            [781030.5157393045, 6490240.0],
                        ]
                    ],
                    "type": "Polygon",
                },
            }
        },
        "id": "e6e14ee5-c431-559d-8b07-e387648d06c6",
        "image": {
            "bands": {
                "B01": {"layer": 1, "path": path_offset("B01.jp2")},
                "B02": {"layer": 1, "path": path_offset("B02.jp2")},
                "B03": {"layer": 1, "path": path_offset("B03.jp2")},
                "B04": {"layer": 1, "path": path_offset("B04.jp2")},
                "B05": {"layer": 1, "path": path_offset("B05.jp2")},
                "B06": {"layer": 1, "path": path_offset("B06.jp2")},
                "B07": {"layer": 1, "path": path_offset("B07.jp2")},
                "B08": {"layer": 1, "path": path_offset("B08.jp2")},
                "B09": {"layer": 1, "path": path_offset("B09.jp2")},
                "B10": {"layer": 1, "path": path_offset("B10.jp2")},
                "B11": {"layer": 1, "path": path_offset("B11.jp2")},
                "B12": {"layer": 1, "path": path_offset("B12.jp2")},
                "B8A": {"layer": 1, "path": path_offset("B8A.jp2")},
            },
            "cloud_cover_percentage": 41.1953,
            "degraded_anc_data_percentage": 0.0,
            "degraded_msi_data_percentage": 0.0,
            "format_quality_flag": "PASSED",
            "general_quality_flag": "PASSED",
            "geometric_quality_flag": "PASSED",
            "radiometric_quality_flag": "PASSED",
            "reflectance_conversion": "0.969545500936321",
            "sensor_quality_flag": "PASSED",
            "solar_irradiance": [],
            "sun_azimuth": 28.329529500752,
            "sun_elevation": 60.25415978281,
            "tile_reference": "N02.06",
            "viewing_angles": [
                {
                    "bandId": "0",
                    "measurement": {
                        "azimuth": {"value": 285.033757307864},
                        "zenith": {"value": 8.48756458553399},
                    },
                    "unit": "degree",
                },
                {
                    "bandId": "9",
                    "measurement": {
                        "azimuth": {"value": 285.075607281345},
                        "zenith": {"value": 8.52241368394389},
                    },
                    "unit": "degree",
                },
                {
                    "bandId": "10",
                    "measurement": {
                        "azimuth": {"value": 284.793804242023},
                        "zenith": {"value": 8.34345092249542},
                    },
                    "unit": "degree",
                },
                {
                    "bandId": "1",
                    "measurement": {
                        "azimuth": {"value": 284.793733737481},
                        "zenith": {"value": 8.29509641932038},
                    },
                    "unit": "degree",
                },
                {
                    "bandId": "2",
                    "measurement": {
                        "azimuth": {"value": 284.851677995258},
                        "zenith": {"value": 8.32126064250559},
                    },
                    "unit": "degree",
                },
                {
                    "bandId": "3",
                    "measurement": {
                        "azimuth": {"value": 284.909295311577},
                        "zenith": {"value": 8.3544279324201},
                    },
                    "unit": "degree",
                },
                {
                    "bandId": "4",
                    "measurement": {
                        "azimuth": {"value": 284.933467772596},
                        "zenith": {"value": 8.37600138776781},
                    },
                    "unit": "degree",
                },
                {
                    "bandId": "5",
                    "measurement": {
                        "azimuth": {"value": 284.973394067973},
                        "zenith": {"value": 8.40018017679447},
                    },
                    "unit": "degree",
                },
                {
                    "bandId": "6",
                    "measurement": {
                        "azimuth": {"value": 285.002316322296},
                        "zenith": {"value": 8.42684047592471},
                    },
                    "unit": "degree",
                },
                {
                    "bandId": "7",
                    "measurement": {
                        "azimuth": {"value": 284.82134822338},
                        "zenith": {"value": 8.30688902336585},
                    },
                    "unit": "degree",
                },
                {
                    "bandId": "8",
                    "measurement": {
                        "azimuth": {"value": 285.068419119347},
                        "zenith": {"value": 8.45522377449714},
                    },
                    "unit": "degree",
                },
                {
                    "bandId": "11",
                    "measurement": {
                        "azimuth": {"value": 284.889987613982},
                        "zenith": {"value": 8.39599518380445},
                    },
                    "unit": "degree",
                },
                {
                    "bandId": "12",
                    "measurement": {
                        "azimuth": {"value": 284.999750610317},
                        "zenith": {"value": 8.46178406516545},
                    },
                    "unit": "degree",
                },
            ],
        },
        "instrument": {"name": "MSI"},
        "lineage": {"source_datasets": {}},
        "orbit": "73",
        "orbit_direction": "DESCENDING",
        "platform": {"code": "SENTINEL_2B"},
        "processing_level": "Level-1C",
        "product_format": {"name": "s2_aws_pds"},
        "product_type": "level1",
        "size_bytes": 1292575,
        "tile_id": "S2B_OPER_MSI_L1C_TL_EPAE_20180617T013729_A006677_T55JGF_N02.06",
    }

    check_prepare_outputs(
        invoke_script=s2_l1c_aws_pds_prepare.main,
        run_args=[
            "--output",
            str(output_path),
            str(
                output_path
                / "S2B_OPER_MSI_L1C_TL_EPAE_20180617T013729_A006677_T55JGF_N02.06"
            ),
        ],
        expected_doc=expected_doc,
        expected_metadata_path=expected_metadata_path,
    )
