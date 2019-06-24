from datetime import datetime
from pathlib import Path

import eodatasets2
from eodatasets2.model import DatasetDoc
from eodatasets2.scripts.packagewagl import package
from tests import assert_file_structure
from tests.integration.common import assert_same_as_file

WAGL_INPUT_PATH: Path = Path(
    __file__
).parent / "data/wagl-input/LC80890802016062LGN01/LC80890802016062LGN01.wagl.h5"


def test_minimal_dea_package(
    l1_ls8_dataset: DatasetDoc, l1_ls8_folder: Path, tmp_path: Path
):
    out = tmp_path / "out"
    given_path = package(WAGL_INPUT_PATH, l1_ls8_dataset, out)

    assert_file_structure(
        out,
        {
            "LC80890802016062LGN01": {
                "ga_ls8c_ard_3-0-0_089080_2016-01-21_final.odc-metadata.yaml": "",
                "ga_ls8c_ard_3-0-0_089080_2016-01-21_final.proc-info.yaml": "",
                "ga_ls8c_ard_3-0-0_089080_2016-01-21_final.sha1": "",
                "ga_ls8c_nbar_3-0-0_089080_2016-01-21_final_band01.tif": "",
                "ga_ls8c_nbar_3-0-0_089080_2016-01-21_final_band02.tif": "",
                "ga_ls8c_nbar_3-0-0_089080_2016-01-21_final_band03.tif": "",
                "ga_ls8c_nbar_3-0-0_089080_2016-01-21_final_band04.tif": "",
                "ga_ls8c_nbar_3-0-0_089080_2016-01-21_final_band05.tif": "",
                "ga_ls8c_nbar_3-0-0_089080_2016-01-21_final_band06.tif": "",
                "ga_ls8c_nbar_3-0-0_089080_2016-01-21_final_band07.tif": "",
                "ga_ls8c_nbar_3-0-0_089080_2016-01-21_final_band08.tif": "",
                "ga_ls8c_nbart_3-0-0_089080_2016-01-21_final_band01.tif": "",
                "ga_ls8c_nbart_3-0-0_089080_2016-01-21_final_band02.tif": "",
                "ga_ls8c_nbart_3-0-0_089080_2016-01-21_final_band03.tif": "",
                "ga_ls8c_nbart_3-0-0_089080_2016-01-21_final_band04.tif": "",
                "ga_ls8c_nbart_3-0-0_089080_2016-01-21_final_band05.tif": "",
                "ga_ls8c_nbart_3-0-0_089080_2016-01-21_final_band06.tif": "",
                "ga_ls8c_nbart_3-0-0_089080_2016-01-21_final_band07.tif": "",
                "ga_ls8c_nbart_3-0-0_089080_2016-01-21_final_band08.tif": "",
                "ga_ls8c_oa_3-0-0_089080_2016-01-21_final_azimuthal-exiting.tif": "",
                "ga_ls8c_oa_3-0-0_089080_2016-01-21_final_azimuthal-incident.tif": "",
                "ga_ls8c_oa_3-0-0_089080_2016-01-21_final_combined-terrain-shadow.tif": "",
                "ga_ls8c_oa_3-0-0_089080_2016-01-21_final_exiting-angle.tif": "",
                "ga_ls8c_oa_3-0-0_089080_2016-01-21_final_fmask.tif": "",
                "ga_ls8c_oa_3-0-0_089080_2016-01-21_final_incident-angle.tif": "",
                "ga_ls8c_oa_3-0-0_089080_2016-01-21_final_nbar-contiguity.tif": "",
                "ga_ls8c_oa_3-0-0_089080_2016-01-21_final_nbart-contiguity.tif": "",
                "ga_ls8c_oa_3-0-0_089080_2016-01-21_final_relative-azimuth.tif": "",
                "ga_ls8c_oa_3-0-0_089080_2016-01-21_final_relative-slope.tif": "",
                "ga_ls8c_oa_3-0-0_089080_2016-01-21_final_satellite-azimuth.tif": "",
                "ga_ls8c_oa_3-0-0_089080_2016-01-21_final_satellite-view.tif": "",
                "ga_ls8c_oa_3-0-0_089080_2016-01-21_final_solar-azimuth.tif": "",
                "ga_ls8c_oa_3-0-0_089080_2016-01-21_final_solar-zenith.tif": "",
                "ga_ls8c_oa_3-0-0_089080_2016-01-21_final_timedelta.tif": "",
            }
        },
    )
    expected_folder = out / "LC80890802016062LGN01"
    assert given_path == expected_folder
    expected_metadata = (
        expected_folder / "ga_ls8c_ard_3-0-0_089080_2016-01-21_final.odc-metadata.yaml"
    )
    assert expected_metadata.exists()

    # Checksum should include all files other than itself.
    checksum = expected_folder / "ga_ls8c_ard_3-0-0_089080_2016-01-21_final.sha1"
    all_output_files = set(
        p.relative_to(checksum.parent)
        for p in expected_folder.rglob("*")
        if p != checksum
    )
    files_in_checksum = {
        Path(l.split("\t")[1]) for l in checksum.read_text().splitlines()
    }
    assert all_output_files == files_in_checksum

    nan = float("NaN")
    assert_same_as_file(
        {
            "$schema": "https://schemas.opendatacube.org/dataset",
            "product": {
                "href": "https://collections.dea.ga.gov.au/product/ga_ls8c_ard_3",
                "name": "ga_ls8c_ard_3",
            },
            "crs": "epsg:32656",
            "geometry": {
                "coordinates": [
                    [
                        [609615.0, -3077085.0],
                        [609615.0, -3310515.0],
                        [378285.0, -3310515.0],
                        [378285.0, -3077085.0],
                        [609615.0, -3077085.0],
                    ]
                ],
                "type": "Polygon",
            },
            "grids": {
                "default": {
                    "shape": [77, 77],
                    "transform": [
                        3004.285714285714,
                        0.0,
                        378285.0,
                        0.0,
                        -3031.5584415584412,
                        -3077085.0,
                        0.0,
                        0.0,
                        1.0,
                    ],
                },
                "nbar": {
                    "shape": [155, 154],
                    "transform": [
                        1502.0454545454545,
                        0.0,
                        378292.5,
                        0.0,
                        -1505.9032258064515,
                        -3077092.5,
                        0.0,
                        0.0,
                        1.0,
                    ],
                },
                "oa:fmask": {
                    "shape": [7781, 7711],
                    "transform": [
                        30.0,
                        0.0,
                        378285.0,
                        0.0,
                        -30.0,
                        -3077085.0,
                        0.0,
                        0.0,
                        1.0,
                    ],
                },
            },
            "properties": {
                "datetime": datetime(2016, 1, 21, 23, 50, 23, 54435),
                "dea:dataset_maturity": "final",
                "dea:processing_level": "level-2",
                "dtr:end_datetime": datetime(2016, 1, 21, 23, 50, 37, 778000),
                "dtr:start_datetime": datetime(2016, 1, 21, 23, 50, 8, 229000),
                "eo:cloud_cover": 93.22,
                "eo:gsd": 30.0,
                "eo:instrument": "OLI_TIRS",
                "eo:platform": "landsat-8",
                "eo:sun_azimuth": 74.0074438,
                "eo:sun_elevation": 55.486483,
                "fmask:clear": 4.869270279446922,
                "fmask:cloud": 83.93772700416639,
                "fmask:cloud_shadow": 3.1888579668711876,
                "fmask:snow": 6.610523895281075e-05,
                "fmask:water": 8.004078644276545,
                "gqa:abs_iterative_mean_x": nan,
                "gqa:abs_iterative_mean_xy": nan,
                "gqa:abs_iterative_mean_y": nan,
                "gqa:abs_x": nan,
                "gqa:abs_xy": nan,
                "gqa:abs_y": nan,
                "gqa:cep90": nan,
                "gqa:iterative_mean_x": nan,
                "gqa:iterative_mean_xy": nan,
                "gqa:iterative_mean_y": nan,
                "gqa:iterative_stddev_x": nan,
                "gqa:iterative_stddev_xy": nan,
                "gqa:iterative_stddev_y": nan,
                "gqa:mean_x": nan,
                "gqa:mean_xy": nan,
                "gqa:mean_y": nan,
                "gqa:stddev_x": nan,
                "gqa:stddev_xy": nan,
                "gqa:stddev_y": nan,
                "landsat:collection_category": "T1",
                "landsat:collection_number": 1,
                "landsat:landsat_product_id": "LC08_L1TP_090084_20160121_20170405_01_T1",
                "landsat:landsat_scene_id": "LC80900842016021LGN02",
                "landsat:wrs_path": 90,
                "landsat:wrs_row": 84,
                "odc:dataset_version": "3.0.0",
                "odc:processing_datetime": datetime(2019, 6, 14, 13, 33, 18, 526020),
                "odc:producer": "ga.gov.au",
                "odc:product_family": "ard",
                "odc:reference_code": "089080",
            },
            "measurements": {
                "nbar_band01": {
                    "path": "ga_ls8c_nbar_3-0-0_089080_2016-01-21_final_band01.tif"
                },
                "nbar_band02": {
                    "path": "ga_ls8c_nbar_3-0-0_089080_2016-01-21_final_band02.tif"
                },
                "nbar_band03": {
                    "path": "ga_ls8c_nbar_3-0-0_089080_2016-01-21_final_band03.tif"
                },
                "nbar_band04": {
                    "path": "ga_ls8c_nbar_3-0-0_089080_2016-01-21_final_band04.tif"
                },
                "nbar_band05": {
                    "path": "ga_ls8c_nbar_3-0-0_089080_2016-01-21_final_band05.tif"
                },
                "nbar_band06": {
                    "path": "ga_ls8c_nbar_3-0-0_089080_2016-01-21_final_band06.tif"
                },
                "nbar_band07": {
                    "path": "ga_ls8c_nbar_3-0-0_089080_2016-01-21_final_band07.tif"
                },
                "nbar_band08": {
                    "grid": "nbar",
                    "path": "ga_ls8c_nbar_3-0-0_089080_2016-01-21_final_band08.tif",
                },
                "nbart_band01": {
                    "path": "ga_ls8c_nbart_3-0-0_089080_2016-01-21_final_band01.tif"
                },
                "nbart_band02": {
                    "path": "ga_ls8c_nbart_3-0-0_089080_2016-01-21_final_band02.tif"
                },
                "nbart_band03": {
                    "path": "ga_ls8c_nbart_3-0-0_089080_2016-01-21_final_band03.tif"
                },
                "nbart_band04": {
                    "path": "ga_ls8c_nbart_3-0-0_089080_2016-01-21_final_band04.tif"
                },
                "nbart_band05": {
                    "path": "ga_ls8c_nbart_3-0-0_089080_2016-01-21_final_band05.tif"
                },
                "nbart_band06": {
                    "path": "ga_ls8c_nbart_3-0-0_089080_2016-01-21_final_band06.tif"
                },
                "nbart_band07": {
                    "path": "ga_ls8c_nbart_3-0-0_089080_2016-01-21_final_band07.tif"
                },
                "nbart_band08": {
                    "grid": "nbar",
                    "path": "ga_ls8c_nbart_3-0-0_089080_2016-01-21_final_band08.tif",
                },
                "oa_azimuthal_exiting": {
                    "path": "ga_ls8c_oa_3-0-0_089080_2016-01-21_final_azimuthal-exiting.tif"
                },
                "oa_azimuthal_incident": {
                    "path": "ga_ls8c_oa_3-0-0_089080_2016-01-21_final_azimuthal-incident.tif"
                },
                "oa_combined_terrain_shadow": {
                    "path": "ga_ls8c_oa_3-0-0_089080_2016-01-21_final_combined-terrain-shadow.tif"
                },
                "oa_exiting_angle": {
                    "path": "ga_ls8c_oa_3-0-0_089080_2016-01-21_final_exiting-angle.tif"
                },
                "oa_fmask": {
                    "grid": "oa:fmask",
                    "path": "ga_ls8c_oa_3-0-0_089080_2016-01-21_final_fmask.tif",
                },
                "oa_incident_angle": {
                    "path": "ga_ls8c_oa_3-0-0_089080_2016-01-21_final_incident-angle.tif"
                },
                "oa_nbar_contiguity": {
                    "path": "ga_ls8c_oa_3-0-0_089080_2016-01-21_final_nbar-contiguity.tif"
                },
                "oa_nbart_contiguity": {
                    "path": "ga_ls8c_oa_3-0-0_089080_2016-01-21_final_nbart-contiguity.tif"
                },
                "oa_relative_azimuth": {
                    "path": "ga_ls8c_oa_3-0-0_089080_2016-01-21_final_relative-azimuth.tif"
                },
                "oa_relative_slope": {
                    "path": "ga_ls8c_oa_3-0-0_089080_2016-01-21_final_relative-slope.tif"
                },
                "oa_satellite_azimuth": {
                    "path": "ga_ls8c_oa_3-0-0_089080_2016-01-21_final_satellite-azimuth.tif"
                },
                "oa_satellite_view": {
                    "path": "ga_ls8c_oa_3-0-0_089080_2016-01-21_final_satellite-view.tif"
                },
                "oa_solar_azimuth": {
                    "path": "ga_ls8c_oa_3-0-0_089080_2016-01-21_final_solar-azimuth.tif"
                },
                "oa_solar_zenith": {
                    "path": "ga_ls8c_oa_3-0-0_089080_2016-01-21_final_solar-zenith.tif"
                },
                "oa_timedelta": {
                    "path": "ga_ls8c_oa_3-0-0_089080_2016-01-21_final_timedelta.tif"
                },
            },
            "lineage": {"level1": ["a780754e-a884-58a7-9ac0-df518a67f59d"]},
        },
        expected_metadata,
        ignore_fields=["id"],
    )

    assert_same_as_file(
        {
            "fmask": {
                "parameters": {
                    "cloud_buffer_distance_metres": 150.0,
                    "cloud_shadow_buffer_distance_metres": 300.0,
                    "frantz_parallax_sentinel_2": False,
                },
                "percent_class_distribution": {
                    "clear": 4.869270279446922,
                    "cloud": 83.93772700416639,
                    "cloud_shadow": 3.1888579668711876,
                    "snow": 6.610523895281075e-05,
                    "water": 8.004078644276545,
                },
            },
            "software_versions": [
                {
                    "name": "eugl",
                    "url": "https://github.com/OpenDataCubePipelines/eugl.git",
                    "version": "0.1.0+35.g0203248",
                },
                {"name": "gverify", "url": None, "version": "v0.25c"},
                {
                    "name": "fmask",
                    "url": "https://bitbucket.org/chchrsc/python-fmask",
                    "version": "0.4.5",
                },
                {
                    "name": "eodatasets2",
                    "url": "https://github.com/GeoscienceAustralia/eo-datasets",
                    "version": eodatasets2.__version__,
                },
            ],
        },
        expected_folder / "ga_ls8c_ard_3-0-0_089080_2016-01-21_final.proc-info.yaml",
        ignore_fields=("gqa", "wagl"),
    )
