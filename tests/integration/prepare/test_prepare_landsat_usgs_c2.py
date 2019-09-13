import pytest
import shutil
from pathlib import Path

from eodatasets3.prepare import landsat_l2_prepare
from tests.integration.common import check_prepare_outputs


def _make_copy(input_path, tmp_path):
    our_input = tmp_path / input_path.name
    if input_path.is_file():
        shutil.copy(input_path, our_input)
    else:
        shutil.copytree(input_path, our_input)
    return our_input


SAMPLE_DATA = (
    Path(__file__).parent.parent
    / "recompress_unpackaged/USGS/L2/Landsat/LC08_L2SP_185052_20180104_20190821_02_T1"
)


@pytest.fixture
def l2_c2_sample(tmp_path: Path) -> Path:
    return _make_copy(SAMPLE_DATA, tmp_path)


def test_prepare_usgs_l2_c2(tmp_path: Path, l2_c2_sample: Path):
    assert l2_c2_sample.exists(), "Test data missing(?)"
    output_path: Path = tmp_path / "out"
    output_path.mkdir()

    # When specifying an output base path it will create path/row subfolders within it.
    expected_metadata_path = (
        output_path
        / "usgs_ls8c_level2_2/185/052/2018/01/04/usgs_ls8c_level2_2-0-20190821_185052_2018-01-04.odc-metadata.yaml"
    )

    check_prepare_outputs(
        invoke_script=landsat_l2_prepare.main,
        run_args=[
            "--output-base",
            str(output_path),
            str(l2_c2_sample) + "/LC08_L2SP_185052_20180104_20190821_02_T1_MTL.txt",
        ],
        expected_doc=USGS_L2_C2_EXPECTED,
        expected_metadata_path=expected_metadata_path,
    )


@pytest.fixture
def super_mock_s3():
    """
    Start a mock S3 server that can be used from within python and C code.

    To avoid any extra configuration, this requires hosts file entries for the default hostnames
    used by S3, *and* environment variables pointing to alternative SSL keys.

    This can be easily done with Docker, but might be better managed with docker-compose.

    We could potentially use https://github.com/adobe/S3Mock in docker instead of moto.
    """
    import socket
    import subprocess
    import os

    # Must run with patched S3 hosts
    hosts_redirected = (
        socket.gethostbyname("s3.amazonaws.com") == "127.0.0.1"
        and socket.gethostbyname("mybucket.s3.amazonaws.com") == "127.0.0.1"
    )
    environment_variables_set = (
        "AWS_CA_BUNDLE" in os.environ and "CURL_CA_BUNDLE" in os.environ
    )
    if not hosts_redirected or not environment_variables_set:
        pytest.skip(
            "super_mock_s3 requires hostnames and environment variables to be set"
        )

    p = subprocess.Popen(
        [
            "moto_server",
            "-p",
            "443",
            "-s",
            "--ssl-cert",
            "keys/server.pem",
            "--ssl-key",
            "keys/server-key.pem",
            "s3",
        ]
    )
    yield
    p.kill()


@pytest.usesfixture("super_mock_s3")
def test_prepare_usgs_l2_c2_on_aws(tmp_path: Path, l2_c2_sample: Path):
    import boto3
    import fsspec

    s3 = boto3.resource("s3")
    s3.create_bucket(Bucket="mybucket")
    fs = fsspec.filesystem("s3")
    fs.put(str(SAMPLE_DATA), "s3://mybucket/", recursive=True)
    assert l2_c2_sample.exists(), "Test data missing(?)"
    output_path: Path = tmp_path / "out"
    output_path.mkdir()

    # When specifying an output base path it will create path/row subfolders within it.
    expected_metadata_path = (
        output_path
        / "usgs_ls8c_level2_2/185/052/2018/01/04/usgs_ls8c_level2_2-0-20190821_185052_2018-01-04.odc-metadata.yaml"
    )

    check_prepare_outputs(
        invoke_script=landsat_l2_prepare.main,
        run_args=[
            "--output-base",
            str(output_path),
            "s3://mybucket/LC08_L2SP_185052_20180104_20190821_02_T1/LC08_L2SP_185052_20180104_20190821_02_T1_MTL.txt",
        ],
        expected_doc=USGS_L2_C2_EXPECTED,
        expected_metadata_path=expected_metadata_path,
    )


USGS_L2_C2_EXPECTED = {
    "$schema": "https://schemas.opendatacube.org/dataset",
    "accessories": {
        "checksum:sha1": {
            "path": "usgs_ls8c_level2_2-0-20190821_185052_2018-01-04.sha1"
        },
        "metadata:landsat_mtl": {
            "path": "LC08_L2SP_185052_20180104_20190821_02_T1_MTL.txt"
        },
        "metadata:processor": {
            "path": "usgs_ls8c_level2_2-0-20190821_185052_2018-01-04.proc-info.yaml"
        },
    },
    "crs": "epsg:32633",
    "geometry": {
        "coordinates": [
            [
                [461715.0, 1395315.0],
                [233085.0, 1395315.0],
                [233085.0, 1162185.0],
                [461715.0, 1162185.0],
                [461715.0, 1395315.0],
            ]
        ],
        "type": "Polygon",
    },
    "grids": {
        "default": {
            "shape": [60, 60],
            "transform": [
                3810.5,
                0.0,
                233085.0,
                0.0,
                -3885.5000000000005,
                1395315.0,
                0.0,
                0.0,
                1.0,
            ],
        }
    },
    "id": "b06249c5-7487-544f-92f5-815bf7c0b1cf",
    "label": "usgs_ls8c_level2_2-0-20190821_185052_2018-01-04",
    "lineage": {},
    "measurements": {
        "atmospheric_transmittance": {
            "path": "usgs_ls8c_level2_2-0-20190821_185052_2018-01-04_atmospheric-transmittance.tif"
        },
        "b1": {"path": "usgs_ls8c_level2_2-0-20190821_185052_2018-01-04_b1.tif"},
        "b10": {"path": "usgs_ls8c_level2_2-0-20190821_185052_2018-01-04_b10.tif"},
        "b2": {"path": "usgs_ls8c_level2_2-0-20190821_185052_2018-01-04_b2.tif"},
        "b3": {"path": "usgs_ls8c_level2_2-0-20190821_185052_2018-01-04_b3.tif"},
        "b4": {"path": "usgs_ls8c_level2_2-0-20190821_185052_2018-01-04_b4.tif"},
        "b5": {"path": "usgs_ls8c_level2_2-0-20190821_185052_2018-01-04_b5.tif"},
        "b6": {"path": "usgs_ls8c_level2_2-0-20190821_185052_2018-01-04_b6.tif"},
        "b7": {"path": "usgs_ls8c_level2_2-0-20190821_185052_2018-01-04_b7.tif"},
        "cloud_distance": {
            "path": "usgs_ls8c_level2_2-0-20190821_185052_2018-01-04_cloud-distance.tif"
        },
        "downwell_radiance": {
            "path": "usgs_ls8c_level2_2-0-20190821_185052_2018-01-04_downwell-radiance.tif"
        },
        "emissivity": {
            "path": "usgs_ls8c_level2_2-0-20190821_185052_2018-01-04_emissivity.tif"
        },
        "emissivity_stdev": {
            "path": "usgs_ls8c_level2_2-0-20190821_185052_2018-01-04_emissivity-stdev.tif"
        },
        "quality_l1_pixel": {
            "path": "usgs_ls8c_level2_2-0-20190821_185052_2018-01-04_quality-l1-pixel.tif"
        },
        "quality_l1_radiometric_saturation": {
            "path": "usgs_ls8c_level2_2-0-20190821_185052_2018-01-04_quality-l1-radiometric-saturation.tif"
        },
        "quality_l2_aerosol": {
            "path": "usgs_ls8c_level2_2-0-20190821_185052_2018-01-04_quality-l2-aerosol.tif"
        },
        "quality_l2_surface_temperature": {
            "path": "usgs_ls8c_level2_2-0-20190821_185052_2018-01-04_quality-l2-surface-temperature.tif"
        },
        "thermal_radiance": {
            "path": "usgs_ls8c_level2_2-0-20190821_185052_2018-01-04_thermal-radiance.tif"
        },
        "upwell_radiance": {
            "path": "usgs_ls8c_level2_2-0-20190821_185052_2018-01-04_upwell-radiance.tif"
        },
    },
    "product": {
        "href": "https://collections.dea.ga.gov.au/product/usgs_ls8c_level2_2",
        "name": "usgs_ls8c_level2_2",
    },
    "properties": {
        "datetime": "2018-01-04T09:24:47.733834",
        "eo:cloud_cover": 0.0,
        "eo:gsd": 30.0,
        "eo:instrument": "OLI_TIRS",
        "eo:platform": "landsat-8",
        "eo:sun_azimuth": 142.95066879,
        "eo:sun_elevation": 47.05081757,
        "landsat:collection_number": 2,
        "landsat:geometric_rmse_model_x": 4.992,
        "landsat:geometric_rmse_model_y": 4.284,
        "landsat:geometric_rmse_verify": 4.234,
        "landsat:ground_control_points_model": 576,
        "landsat:ground_control_points_verify": 165,
        "landsat:ground_control_points_version": 51,
        "landsat:landsat_product_id": "LC08_L1TP_185052_20180104_20190821_02_T1",
        "landsat:landsat_scene_id": "LC81850522018004LGN00",
        "landsat:processing_software_version": "LPGS_Unknown",
        "landsat:station_id": "LGN",
        "landsat:wrs_path": 185,
        "landsat:wrs_row": 52,
        "odc:dataset_version": "2.0.20190821",
        "odc:file_format": "GeoTIFF",
        "odc:processing_datetime": "2019-08-21T21:40:22",
        "odc:producer": "usgs.gov",
        "odc:product_family": "level2",
        "odc:region_code": "185052",
    },
}
