from datetime import datetime, timedelta
from pathlib import Path

import pytest
import rasterio
from binascii import crc32
from dateutil.tz import tzutc
from rasterio import DatasetReader
from rasterio.enums import Compression
from rio_cogeo import cogeo

import eodatasets2
from eodatasets2.model import DatasetDoc
from eodatasets2.scripts import packagewagl
from eodatasets2.scripts.packagewagl import package
from tests import assert_file_structure
from tests.integration.common import assert_same_as_file

WAGL_INPUT_PATH: Path = Path(
    __file__
).parent / "data/wagl-input/LC80890802016062LGN01/LC80890802016062LGN01.wagl.h5"
L1_METADATA_PATH: Path = Path(
    __file__
).parent / "data/LC08_L1TP_089080_20160302_20170328_01_T1.yaml"


def test_minimal_dea_package(
    l1_ls8_dataset: DatasetDoc, l1_ls8_folder: Path, tmp_path: Path
):
    out = tmp_path / "out"

    with pytest.warns(None) as warning_record:
        given_path = package(WAGL_INPUT_PATH, L1_METADATA_PATH, out)

    # No warnings should have been logged during package.
    # We could tighten this to specific warnings if it proves too noisy, but it's
    # useful for catching things like unclosed files.
    if warning_record:
        messages = "\n".join(f"- {w.message} ({w})\n" for w in warning_record)
        raise AssertionError(
            f"Warnings were produced during wagl package:\n {messages}"
        )

    assert_file_structure(
        out,
        {
            "LC80890802016062LGN01": {
                "ga_ls8c_ard_3-0-0_089080_2016-03-02_final.odc-metadata.yaml": "",
                "ga_ls8c_ard_3-0-0_089080_2016-03-02_final.proc-info.yaml": "",
                "ga_ls8c_ard_3-0-0_089080_2016-03-02_final.sha1": "",
                "ga_ls8c_nbar_3-0-0_089080_2016-03-02_final_band01.tif": "",
                "ga_ls8c_nbar_3-0-0_089080_2016-03-02_final_band02.tif": "",
                "ga_ls8c_nbar_3-0-0_089080_2016-03-02_final_band03.tif": "",
                "ga_ls8c_nbar_3-0-0_089080_2016-03-02_final_band04.tif": "",
                "ga_ls8c_nbar_3-0-0_089080_2016-03-02_final_band05.tif": "",
                "ga_ls8c_nbar_3-0-0_089080_2016-03-02_final_band06.tif": "",
                "ga_ls8c_nbar_3-0-0_089080_2016-03-02_final_band07.tif": "",
                "ga_ls8c_nbar_3-0-0_089080_2016-03-02_final_band08.tif": "",
                "ga_ls8c_nbar_3-0-0_089080_2016-03-02_final_thumbnail.jpg": "",
                "ga_ls8c_nbart_3-0-0_089080_2016-03-02_final_band01.tif": "",
                "ga_ls8c_nbart_3-0-0_089080_2016-03-02_final_band02.tif": "",
                "ga_ls8c_nbart_3-0-0_089080_2016-03-02_final_band03.tif": "",
                "ga_ls8c_nbart_3-0-0_089080_2016-03-02_final_band04.tif": "",
                "ga_ls8c_nbart_3-0-0_089080_2016-03-02_final_band05.tif": "",
                "ga_ls8c_nbart_3-0-0_089080_2016-03-02_final_band06.tif": "",
                "ga_ls8c_nbart_3-0-0_089080_2016-03-02_final_band07.tif": "",
                "ga_ls8c_nbart_3-0-0_089080_2016-03-02_final_band08.tif": "",
                "ga_ls8c_nbart_3-0-0_089080_2016-03-02_final_thumbnail.jpg": "",
                "ga_ls8c_oa_3-0-0_089080_2016-03-02_final_azimuthal-exiting.tif": "",
                "ga_ls8c_oa_3-0-0_089080_2016-03-02_final_azimuthal-incident.tif": "",
                "ga_ls8c_oa_3-0-0_089080_2016-03-02_final_combined-terrain-shadow.tif": "",
                "ga_ls8c_oa_3-0-0_089080_2016-03-02_final_exiting-angle.tif": "",
                "ga_ls8c_oa_3-0-0_089080_2016-03-02_final_fmask.tif": "",
                "ga_ls8c_oa_3-0-0_089080_2016-03-02_final_incident-angle.tif": "",
                "ga_ls8c_oa_3-0-0_089080_2016-03-02_final_nbar-contiguity.tif": "",
                "ga_ls8c_oa_3-0-0_089080_2016-03-02_final_nbart-contiguity.tif": "",
                "ga_ls8c_oa_3-0-0_089080_2016-03-02_final_relative-azimuth.tif": "",
                "ga_ls8c_oa_3-0-0_089080_2016-03-02_final_relative-slope.tif": "",
                "ga_ls8c_oa_3-0-0_089080_2016-03-02_final_satellite-azimuth.tif": "",
                "ga_ls8c_oa_3-0-0_089080_2016-03-02_final_satellite-view.tif": "",
                "ga_ls8c_oa_3-0-0_089080_2016-03-02_final_solar-azimuth.tif": "",
                "ga_ls8c_oa_3-0-0_089080_2016-03-02_final_solar-zenith.tif": "",
                "ga_ls8c_oa_3-0-0_089080_2016-03-02_final_time-delta.tif": "",
            }
        },
    )
    expected_folder = out / "LC80890802016062LGN01"
    assert given_path == expected_folder
    [output_metadata] = expected_folder.rglob("*.odc-metadata.yaml")

    # Checksum should include all files other than itself.
    [checksum_file] = expected_folder.rglob("*.sha1")
    all_output_files = set(
        p.relative_to(checksum_file.parent)
        for p in expected_folder.rglob("*")
        if p != checksum_file
    )
    files_in_checksum = {
        Path(l.split("\t")[1]) for l in checksum_file.read_text().splitlines()
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
                        [609_615.0, -3_077_085.0],
                        [609_615.0, -3_310_515.0],
                        [378_285.0, -3_310_515.0],
                        [378_285.0, -3_077_085.0],
                        [609_615.0, -3_077_085.0],
                    ]
                ],
                "type": "Polygon",
            },
            "grids": {
                "band08": {
                    "shape": [155, 154],
                    "transform": [
                        1502.045_454_545_454_5,
                        0.0,
                        378_292.5,
                        0.0,
                        -1505.903_225_806_451_5,
                        -3_077_092.5,
                        0.0,
                        0.0,
                        1.0,
                    ],
                },
                "default": {
                    "shape": [77, 77],
                    "transform": [
                        3004.285_714_285_714,
                        0.0,
                        378_285.0,
                        0.0,
                        -3031.558_441_558_441_2,
                        -3_077_085.0,
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
                        378_285.0,
                        0.0,
                        -30.0,
                        -3_077_085.0,
                        0.0,
                        0.0,
                        1.0,
                    ],
                },
            },
            "properties": {
                "datetime": datetime(2016, 3, 2, 23, 42, 24, 747_943),
                "dea:dataset_maturity": "final",
                "dea:processing_level": "level-2",
                "dtr:end_datetime": datetime(2016, 3, 2, 23, 42, 39, 472_060),
                "dtr:start_datetime": datetime(2016, 3, 2, 23, 42, 9, 922_933),
                "eo:cloud_cover": 74.31,
                "eo:gsd": 30.0,
                "eo:instrument": "OLI_TIRS",
                "eo:platform": "landsat-8",
                "eo:sun_azimuth": 62.480_213_79,
                "eo:sun_elevation": 50.707_283_49,
                "fmask:clear": 4.869_270_279_446_922,
                "fmask:cloud": 83.937_727_004_166_39,
                "fmask:cloud_shadow": 3.188_857_966_871_187_6,
                "fmask:snow": 6.610_523_895_281_075e-05,
                "fmask:water": 8.004_078_644_276_545,
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
                "landsat:landsat_product_id": "LC08_L1TP_089080_20160302_20170328_01_T1",
                "landsat:landsat_scene_id": "LC80890802016062LGN01",
                "landsat:wrs_path": 89,
                "landsat:wrs_row": 80,
                "odc:dataset_version": "3.0.0",
                "odc:processing_datetime": datetime(2019, 6, 14, 13, 33, 18, 526_020),
                "odc:producer": "ga.gov.au",
                "odc:product_family": "ard",
                "odc:reference_code": "089080",
            },
            "measurements": {
                "nbar_band01": {
                    "path": "ga_ls8c_nbar_3-0-0_089080_2016-03-02_final_band01.tif"
                },
                "nbar_band02": {
                    "path": "ga_ls8c_nbar_3-0-0_089080_2016-03-02_final_band02.tif"
                },
                "nbar_band03": {
                    "path": "ga_ls8c_nbar_3-0-0_089080_2016-03-02_final_band03.tif"
                },
                "nbar_band04": {
                    "path": "ga_ls8c_nbar_3-0-0_089080_2016-03-02_final_band04.tif"
                },
                "nbar_band05": {
                    "path": "ga_ls8c_nbar_3-0-0_089080_2016-03-02_final_band05.tif"
                },
                "nbar_band06": {
                    "path": "ga_ls8c_nbar_3-0-0_089080_2016-03-02_final_band06.tif"
                },
                "nbar_band07": {
                    "path": "ga_ls8c_nbar_3-0-0_089080_2016-03-02_final_band07.tif"
                },
                "nbar_band08": {
                    "grid": "band08",
                    "path": "ga_ls8c_nbar_3-0-0_089080_2016-03-02_final_band08.tif",
                },
                "nbart_band01": {
                    "path": "ga_ls8c_nbart_3-0-0_089080_2016-03-02_final_band01.tif"
                },
                "nbart_band02": {
                    "path": "ga_ls8c_nbart_3-0-0_089080_2016-03-02_final_band02.tif"
                },
                "nbart_band03": {
                    "path": "ga_ls8c_nbart_3-0-0_089080_2016-03-02_final_band03.tif"
                },
                "nbart_band04": {
                    "path": "ga_ls8c_nbart_3-0-0_089080_2016-03-02_final_band04.tif"
                },
                "nbart_band05": {
                    "path": "ga_ls8c_nbart_3-0-0_089080_2016-03-02_final_band05.tif"
                },
                "nbart_band06": {
                    "path": "ga_ls8c_nbart_3-0-0_089080_2016-03-02_final_band06.tif"
                },
                "nbart_band07": {
                    "path": "ga_ls8c_nbart_3-0-0_089080_2016-03-02_final_band07.tif"
                },
                "nbart_band08": {
                    "grid": "band08",
                    "path": "ga_ls8c_nbart_3-0-0_089080_2016-03-02_final_band08.tif",
                },
                "oa_azimuthal_exiting": {
                    "path": "ga_ls8c_oa_3-0-0_089080_2016-03-02_final_azimuthal-exiting.tif"
                },
                "oa_azimuthal_incident": {
                    "path": "ga_ls8c_oa_3-0-0_089080_2016-03-02_final_azimuthal-incident.tif"
                },
                "oa_combined_terrain_shadow": {
                    "path": "ga_ls8c_oa_3-0-0_089080_2016-03-02_final_combined-terrain-shadow.tif"
                },
                "oa_exiting_angle": {
                    "path": "ga_ls8c_oa_3-0-0_089080_2016-03-02_final_exiting-angle.tif"
                },
                "oa_fmask": {
                    "grid": "oa:fmask",
                    "path": "ga_ls8c_oa_3-0-0_089080_2016-03-02_final_fmask.tif",
                },
                "oa_incident_angle": {
                    "path": "ga_ls8c_oa_3-0-0_089080_2016-03-02_final_incident-angle.tif"
                },
                "oa_nbar_contiguity": {
                    "path": "ga_ls8c_oa_3-0-0_089080_2016-03-02_final_nbar-contiguity.tif"
                },
                "oa_nbart_contiguity": {
                    "path": "ga_ls8c_oa_3-0-0_089080_2016-03-02_final_nbart-contiguity.tif"
                },
                "oa_relative_azimuth": {
                    "path": "ga_ls8c_oa_3-0-0_089080_2016-03-02_final_relative-azimuth.tif"
                },
                "oa_relative_slope": {
                    "path": "ga_ls8c_oa_3-0-0_089080_2016-03-02_final_relative-slope.tif"
                },
                "oa_satellite_azimuth": {
                    "path": "ga_ls8c_oa_3-0-0_089080_2016-03-02_final_satellite-azimuth.tif"
                },
                "oa_satellite_view": {
                    "path": "ga_ls8c_oa_3-0-0_089080_2016-03-02_final_satellite-view.tif"
                },
                "oa_solar_azimuth": {
                    "path": "ga_ls8c_oa_3-0-0_089080_2016-03-02_final_solar-azimuth.tif"
                },
                "oa_solar_zenith": {
                    "path": "ga_ls8c_oa_3-0-0_089080_2016-03-02_final_solar-zenith.tif"
                },
                "oa_time_delta": {
                    "path": "ga_ls8c_oa_3-0-0_089080_2016-03-02_final_time-delta.tif"
                },
            },
            "lineage": {"level1": ["944b3a38-0c42-5a6d-b4e8-4c0fdb67fbfc"]},
        },
        output_metadata,
        ignore_fields=["id"],
    )

    [proc_info] = expected_folder.rglob("*.proc-info.yaml")
    assert_same_as_file(
        {
            "fmask": {
                "parameters": {
                    "cloud_buffer_distance_metres": 150.0,
                    "cloud_shadow_buffer_distance_metres": 300.0,
                    "frantz_parallax_sentinel_2": False,
                },
                "percent_class_distribution": {
                    "clear": 4.869_270_279_446_922,
                    "cloud": 83.937_727_004_166_39,
                    "cloud_shadow": 3.188_857_966_871_187_6,
                    "snow": 6.610_523_895_281_075e-05,
                    "water": 8.004_078_644_276_545,
                },
            },
            "software_versions": [
                {
                    "name": "modtran",
                    "url": "http://www.ontar.com/software/productdetails.aspx?item=modtran",
                    "version": "6.0.1",
                },
                {
                    "name": "wagl",
                    "url": "https://github.com/GeoscienceAustralia/wagl.git",
                    "version": "5.3.1+80.g8714c32.dirty",
                },
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
        proc_info,
        ignore_fields=("gqa", "wagl"),
    )

    # All produced tifs should be valid COGs
    for image in expected_folder.rglob("*.tif"):
        assert cogeo.cog_validate(image), f"Failed COG validation: {image}"

    # Check one of the images explicitly.
    [image] = expected_folder.rglob("*_nbar_*_band08.tif")
    with rasterio.open(image) as d:
        d: DatasetReader
        assert d.count == 1, "Expected one band"
        assert d.nodata == -999.0

        # Verify the pixel values haven't changed.
        assert crc32(d.read(1).tobytes()) == 3_593_128_880
        # (Rasterio's checksum is zero on this data for some reason?)
        assert d.checksum(1) == 0

        assert d.overviews(1) == [8, 15, 31]
        assert d.driver == "GTiff"
        assert d.dtypes == ("float64",)
        assert d.compression == Compression.deflate

        assert d.height == 155
        assert d.width == 154


def test_maturity_calculation():
    # Simplified. Only a few ancillary parts that matter to us.
    wagl_doc = {
        "ancillary": {
            "aerosol": {
                "CLASS": "SCALAR",
                "VERSION": "0.1",
                "id": ["281203f2-aeb2-573c-8207-5bdad109d03f"],
                "tier": "AATSR_CMP_MONTH",
                "value": 0.047_768_376_767_635_345,
            },
            "brdf_alpha_1_band_2": {
                "id": [
                    "71785790-5d66-59c8-beed-12d35d0811ac",
                    "6093891a-a242-5545-a666-662a2c29aead",
                ],
                "tier": "DEFINITIVE",
                "value": 0.354_201_642_899_772_8,
            },
            "brdf_alpha_2_band_2": {
                "id": [
                    "71785790-5d66-59c8-beed-12d35d0811ac",
                    "6093891a-a242-5545-a666-662a2c29aead",
                ],
                "tier": "DEFINITIVE",
                "value": 0.192_742_504_203_452_96,
            },
            "elevation": {
                "CLASS": "SCALAR",
                "VERSION": "0.1",
                "id": ["e75ac77d-1ed0-55a5-888b-9ae48080eae9"],
                "value": 0.549_334_594_726_562_5,
            },
            "ozone": {
                "CLASS": "SCALAR",
                "VERSION": "0.1",
                "id": ["c3953cf0-93a0-5217-9c2e-babc16fef3be"],
                "tier": "DEFINITIVE",
                "value": 0.263,
            },
            "water_vapour": {
                "CLASS": "SCALAR",
                "VERSION": "0.1",
                "id": ["71950fbf-8aeb-56fc-bbd7-41e2046f22f2"],
                "tier": "DEFINITIVE",
                "value": 1.010_000_038_146_972_7,
            },
        }
    }

    normal_acq_date = datetime(2002, 11, 20, tzinfo=tzutc())
    normal_proc_date = normal_acq_date + timedelta(days=7)
    acq_before_01 = datetime(2000, 11, 20, tzinfo=tzutc())

    # Normal, final dataset. Processed just outside of NRT window.
    assert (
        packagewagl._determine_maturity(
            normal_acq_date, normal_acq_date + timedelta(hours=49), wagl_doc
        )
        == "final"
    )

    # NRT when processed < 48 hours
    assert (
        packagewagl._determine_maturity(
            normal_acq_date, normal_acq_date + timedelta(hours=1), wagl_doc
        )
        == "nrt"
    )
    assert (
        packagewagl._determine_maturity(
            acq_before_01, acq_before_01 + timedelta(hours=47), wagl_doc
        )
        == "nrt"
    )

    # Before 2001: final if water vapour is definitive.
    assert (
        packagewagl._determine_maturity(
            acq_before_01, acq_before_01 + timedelta(days=3), wagl_doc
        )
        == "final"
    )

    # Interim whenever water vapour is fallback.
    wagl_doc["ancillary"]["water_vapour"]["tier"] = "FALLBACK_DATASET"
    assert (
        packagewagl._determine_maturity(normal_acq_date, normal_proc_date, wagl_doc)
        == "interim"
    )
    assert (
        packagewagl._determine_maturity(
            acq_before_01, acq_before_01 + timedelta(days=3), wagl_doc
        )
        == "interim"
    )
    wagl_doc["ancillary"]["water_vapour"]["tier"] = "DEFINITIVE"

    # Fallback BRDF (when at least one is fallback)
    wagl_doc["ancillary"]["brdf_alpha_2_band_2"]["tier"] = "FALLBACK_DEFAULT"
    assert (
        packagewagl._determine_maturity(normal_acq_date, normal_proc_date, wagl_doc)
        == "interim"
    )
