import pytest
import rasterio
import numpy

from binascii import crc32
from click.testing import CliRunner
from datetime import datetime, timedelta, timezone
from pathlib import Path
from rasterio import DatasetReader
from rasterio.enums import Compression
from rio_cogeo import cogeo

import eodatasets3
from eodatasets3.model import DatasetDoc
from tests import assert_file_structure
from tests.common import assert_same_as_file

from . import assert_image

h5py = pytest.importorskip(
    "h5py",
    reason="Extra dependencies needed to run wagl package test. "
    "Try pip install eodatasets3[wagl]",
)

# This test dataset comes from running `tests/integration/h5downsample.py` on a real
# wagl output.
WAGL_INPUT_PATH: Path = (
    Path(__file__).parent
    / "data/wagl-input/LC80910862014310LGN01/LC80910862014310LGN01.wagl.h5"
)
# The matching Level1 metadata (produced by landsat_l1_prepare.py)
L1_METADATA_PATH: Path = (
    Path(__file__).parent
    / "data/wagl-input/LC08_L1TP_091086_20141106_20170417_01_T1.odc-metadata.yaml"
)


def test_whole_wagl_package(
    l1_ls8_dataset: DatasetDoc, l1_ls8_folder: Path, tmp_path: Path
):
    out = tmp_path

    # packagewagl was changed, such that lambertian
    # is the default product to package.
    from eodatasets3.scripts import packagewagl

    with pytest.warns(None) as warning_record:
        res = CliRunner().invoke(
            packagewagl.run,
            map(str, (WAGL_INPUT_PATH, "--level1", L1_METADATA_PATH, "--output", out)),
            catch_exceptions=False,
        )
        # The last line of output ends with the dataset path.
        words, reported_metadata = res.output.splitlines()[-1].rsplit(" ", 1)

    # No warnings should have been logged during package.
    # We could tighten this to specific warnings if it proves too noisy, but it's
    # useful for catching things like unclosed files.
    if warning_record:
        messages = "\n".join(f"- {w.message} ({w})\n" for w in warning_record)
        raise AssertionError(
            f"Warnings were produced during wagl package:\n {messages}"
        )

    expected_folder = out / "ga_ls8c_aard_3/091/086/2014/11/06"
    assert_file_structure(
        expected_folder,
        {
            "ga_ls8c_aard_3-2-0_091086_2014-11-06_final.odc-metadata.yaml": "",
            "ga_ls8c_aard_3-2-0_091086_2014-11-06_final.proc-info.yaml": "",
            "ga_ls8c_aard_3-2-0_091086_2014-11-06_final.sha1": "",
            "ga_ls8c_lambertian_3-2-0_091086_2014-11-06_final_band01.tif": "",
            "ga_ls8c_lambertian_3-2-0_091086_2014-11-06_final_band02.tif": "",
            "ga_ls8c_lambertian_3-2-0_091086_2014-11-06_final_band03.tif": "",
            "ga_ls8c_lambertian_3-2-0_091086_2014-11-06_final_band04.tif": "",
            "ga_ls8c_lambertian_3-2-0_091086_2014-11-06_final_band05.tif": "",
            "ga_ls8c_lambertian_3-2-0_091086_2014-11-06_final_band06.tif": "",
            "ga_ls8c_lambertian_3-2-0_091086_2014-11-06_final_band07.tif": "",
            "ga_ls8c_lambertian_3-2-0_091086_2014-11-06_final_band08.tif": "",
            "ga_ls8c_oa_3-2-0_091086_2014-11-06_final_azimuthal-exiting.tif": "",
            "ga_ls8c_oa_3-2-0_091086_2014-11-06_final_azimuthal-incident.tif": "",
            "ga_ls8c_oa_3-2-0_091086_2014-11-06_final_combined-terrain-shadow.tif": "",
            "ga_ls8c_oa_3-2-0_091086_2014-11-06_final_exiting-angle.tif": "",
            "ga_ls8c_oa_3-2-0_091086_2014-11-06_final_fmask.tif": "",
            "ga_ls8c_oa_3-2-0_091086_2014-11-06_final_incident-angle.tif": "",
            "ga_ls8c_oa_3-2-0_091086_2014-11-06_final_lambertian-contiguity.tif": "",
            "ga_ls8c_oa_3-2-0_091086_2014-11-06_final_relative-azimuth.tif": "",
            "ga_ls8c_oa_3-2-0_091086_2014-11-06_final_relative-slope.tif": "",
            "ga_ls8c_oa_3-2-0_091086_2014-11-06_final_satellite-azimuth.tif": "",
            "ga_ls8c_oa_3-2-0_091086_2014-11-06_final_satellite-view.tif": "",
            "ga_ls8c_oa_3-2-0_091086_2014-11-06_final_solar-azimuth.tif": "",
            "ga_ls8c_oa_3-2-0_091086_2014-11-06_final_solar-zenith.tif": "",
            "ga_ls8c_oa_3-2-0_091086_2014-11-06_final_time-delta.tif": "",
        },
    )
    [output_metadata] = expected_folder.rglob("*.odc-metadata.yaml")
    from pprint import pprint

    pprint(output_metadata)

    assert reported_metadata == str(
        output_metadata
    ), "Cli didn't report the expected output path"

    # Checksum should include all files other than itself.
    [checksum_file] = expected_folder.rglob("*.sha1")
    all_output_files = set(
        p.relative_to(checksum_file.parent)
        for p in expected_folder.rglob("*")
        if p != checksum_file
    )
    files_in_checksum = {
        Path(line.split("\t")[1]) for line in checksum_file.read_text().splitlines()
    }
    assert all_output_files == files_in_checksum

    # Verify the computed contiguity looks the same. (metadata fields will depend on it)
    [image] = expected_folder.rglob("*_oa_*lambertian-contiguity.tif")
    assert_image(image, nodata=255, unique_pixel_counts={0: 5619, 1: 622})

    assert_same_as_file(
        {
            "$schema": "https://schemas.opendatacube.org/dataset",
            # A stable ID is taken from the WAGL doc.
            "id": "4c4177ef-fcb2-46af-9378-c444779fe997",
            "label": "ga_ls8c_aard_3-2-0_091086_2014-11-06_final",
            "product": {
                "href": "https://collections.dea.ga.gov.au/product/ga_ls8c_aard_3",
                "name": "ga_ls8c_aard_3",
            },
            "crs": "epsg:32655",
            "geometry": {
                "coordinates": [
                    [
                        [470_805.0, -4_029_885.0],
                        [464_953.676_774_568_4, -4_038_116.554_728_451_7],
                        [423_285.0, -4_220_937.151_898_734],
                        [440_403.0_358_099_319, -4_229_808.143_934_857],
                        [597_347.1_317_838_556, -4_265_715.0],
                        [611_397.7_376_167_143, -4_265_715.0],
                        [622_190.852_781_391, -4_233583.4_657_330_42],
                        [657_915.0, -4_074_662.848_101_265_7],
                        [524_966.964_190_068_1, -4_038_925.1_472_043_837],
                        [470_805.0, -4_029_885.0],
                    ]
                ],
                "type": "Polygon",
            },
            "grids": {
                "default": {
                    "shape": [79, 79],
                    "transform": [
                        2970.0,
                        0.0,
                        423_285.0,
                        0.0,
                        -2985.1_898_734_177_216,
                        -4_029_885.0,
                        0.0,
                        0.0,
                        1.0,
                    ],
                },
                "RES_1494m": {
                    "shape": [158, 157],
                    "transform": [
                        1494.363_057_324_840_8,
                        0.0,
                        423_292.5,
                        0.0,
                        -1492.5,
                        -4_029_892.5,
                        0.0,
                        0.0,
                        1.0,
                    ],
                },
            },
            "properties": {
                "datetime": datetime(2014, 11, 6, 23, 57, 30, 326_870),
                "dea:dataset_maturity": "final",
                "dtr:end_datetime": datetime(2014, 11, 6, 23, 57, 44, 708_057),
                "dtr:start_datetime": datetime(2014, 11, 6, 23, 57, 16, 421_152),
                "eo:cloud_cover": 5.8_808_932_221_195_4,
                "eo:gsd": 1492.5,
                "eo:instrument": "OLI_TIRS",
                "eo:platform": "landsat-8",
                "eo:sun_azimuth": 58.1_919_722_7,
                "eo:sun_elevation": 56.6_866_517,
                "fmask:clear": 79.067_734_968_526_51,
                "fmask:cloud": 5.880_893_222_119_54,
                "fmask:cloud_shadow": 0.684_885_048_789_727_6,
                "fmask:snow": 0.0,
                "fmask:water": 14.366_486_760_564_232,
                "gqa:abs_iterative_mean_x": 0.18,
                "gqa:abs_iterative_mean_xy": 0.26,
                "gqa:abs_iterative_mean_y": 0.19,
                "gqa:abs_x": 0.43,
                "gqa:abs_xy": 0.54,
                "gqa:abs_y": 0.32,
                "gqa:cep90": 0.52,
                "gqa:iterative_mean_x": -0.08,
                "gqa:iterative_mean_xy": 0.16,
                "gqa:iterative_mean_y": 0.14,
                "gqa:iterative_stddev_x": 0.28,
                "gqa:iterative_stddev_xy": 0.35,
                "gqa:iterative_stddev_y": 0.2,
                "gqa:mean_x": -0.2,
                "gqa:mean_xy": 0.22,
                "gqa:mean_y": 0.1,
                "gqa:stddev_x": 2.39,
                "gqa:stddev_xy": 2.57,
                "gqa:stddev_y": 0.95,
                "landsat:collection_category": "T1",
                "landsat:collection_number": 1,
                "landsat:landsat_product_id": "LC08_L1TP_091086_20141106_20170417_01_T1",
                "landsat:landsat_scene_id": "LC80910862014310LGN01",
                "landsat:wrs_path": 91,
                "landsat:wrs_row": 86,
                "odc:dataset_version": "3.2.0",
                "odc:file_format": "GeoTIFF",
                "odc:processing_datetime": datetime(2020, 11, 9, 5, 0, 19, 63_344),
                "odc:producer": "ga.gov.au",
                "odc:product_family": "aard",
                "odc:region_code": "091086",
            },
            "measurements": {
                "lambertian_blue": {
                    "path": "ga_ls8c_lambertian_3-2-0_091086_2014-11-06_final_band02.tif",
                },
                "lambertian_coastal_aerosol": {
                    "path": "ga_ls8c_lambertian_3-2-0_091086_2014-11-06_final_band01.tif",
                },
                "lambertian_green": {
                    "path": "ga_ls8c_lambertian_3-2-0_091086_2014-11-06_final_band03.tif",
                },
                "lambertian_nir": {
                    "path": "ga_ls8c_lambertian_3-2-0_091086_2014-11-06_final_band05.tif",
                },
                "lambertian_panchromatic": {
                    "grid": "RES_1494m",
                    "path": "ga_ls8c_lambertian_3-2-0_091086_2014-11-06_final_band08.tif",
                },
                "lambertian_red": {
                    "path": "ga_ls8c_lambertian_3-2-0_091086_2014-11-06_final_band04.tif",
                },
                "lambertian_swir_1": {
                    "path": "ga_ls8c_lambertian_3-2-0_091086_2014-11-06_final_band06.tif",
                },
                "lambertian_swir_2": {
                    "path": "ga_ls8c_lambertian_3-2-0_091086_2014-11-06_final_band07.tif",
                },
                "oa_azimuthal_exiting": {
                    "path": "ga_ls8c_oa_3-2-0_091086_2014-11-06_final_azimuthal-exiting.tif",
                },
                "oa_azimuthal_incident": {
                    "path": "ga_ls8c_oa_3-2-0_091086_2014-11-06_final_azimuthal-incident.tif",
                },
                "oa_combined_terrain_shadow": {
                    "path": "ga_ls8c_oa_3-2-0_091086_2014-11-06_final_combined-terrain-shadow.tif",
                },
                "oa_exiting_angle": {
                    "path": "ga_ls8c_oa_3-2-0_091086_2014-11-06_final_exiting-angle.tif",
                },
                "oa_fmask": {
                    "path": "ga_ls8c_oa_3-2-0_091086_2014-11-06_final_fmask.tif",
                },
                "oa_incident_angle": {
                    "path": "ga_ls8c_oa_3-2-0_091086_2014-11-06_final_incident-angle.tif",
                },
                "oa_lambertian_contiguity": {
                    "path": "ga_ls8c_oa_3-2-0_091086_2014-11-06_final_lambertian-contiguity.tif",
                },
                "oa_relative_azimuth": {
                    "path": "ga_ls8c_oa_3-2-0_091086_2014-11-06_final_relative-azimuth.tif",
                },
                "oa_relative_slope": {
                    "path": "ga_ls8c_oa_3-2-0_091086_2014-11-06_final_relative-slope.tif",
                },
                "oa_satellite_azimuth": {
                    "path": "ga_ls8c_oa_3-2-0_091086_2014-11-06_final_satellite-azimuth.tif",
                },
                "oa_satellite_view": {
                    "path": "ga_ls8c_oa_3-2-0_091086_2014-11-06_final_satellite-view.tif",
                },
                "oa_solar_azimuth": {
                    "path": "ga_ls8c_oa_3-2-0_091086_2014-11-06_final_solar-azimuth.tif",
                },
                "oa_solar_zenith": {
                    "path": "ga_ls8c_oa_3-2-0_091086_2014-11-06_final_solar-zenith.tif",
                },
                "oa_time_delta": {
                    "path": "ga_ls8c_oa_3-2-0_091086_2014-11-06_final_time-delta.tif",
                },
            },
            "accessories": {
                "checksum:sha1": {
                    "path": "ga_ls8c_aard_3-2-0_091086_2014-11-06_final.sha1"
                },
                "metadata:processor": {
                    "path": "ga_ls8c_aard_3-2-0_091086_2014-11-06_final.proc-info.yaml"
                },
            },
            "lineage": {"level1": ["d0a5afde-4d8d-5938-aa42-55b6c84a14fe"]},
        },
        output_metadata,
    )

    [proc_info] = expected_folder.rglob("*.proc-info.yaml")
    assert_same_as_file(
        {
            "fmask": {
                "parameters": {
                    "cloud_buffer_distance_metres": 0.0,
                    "cloud_shadow_buffer_distance_metres": 0.0,
                    "frantz_parallax_sentinel_2": False,
                },
                "percent_class_distribution": {
                    "clear": 79.06_773_496_852_651,
                    "cloud": 5.880_893_222_119_54,
                    "cloud_shadow": 0.684_885_048_789_727_6,
                    "snow": 0.0,
                    "water": 14.366_486_760_564_232,
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
                    "url": "https://github.com/GeoscienceAustralia/wagl",
                    "version": "5.4.2.dev252+g54d5ef2",
                },
                {
                    "name": "eugl",
                    "url": "https://github.com/OpenDataCubePipelines/eugl",
                    "version": "0.2.2.dev24+g6c2a9e5",
                },
                {"name": "gverify", "url": None, "version": "v0.25c"},
                {
                    "name": "fmask",
                    "url": "https://bitbucket.org/chchrsc/python-fmask",
                    "version": "0.5.4",
                },
                {
                    "name": "tesp",
                    "url": "https://github.com/OpenDataCubePipelines/tesp",
                    "version": "0.6.6.dev18+g9d22761.d20201007",
                },
                {
                    "name": "eodatasets3",
                    "url": "https://github.com/GeoscienceAustralia/eo-datasets",
                    "version": eodatasets3.__version__,
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
    [image] = expected_folder.rglob("*_lambertian_*_band08.tif")
    with rasterio.open(image) as d:
        d: DatasetReader
        assert d.count == 1, "Expected one band"
        assert d.nodata == -999.0

        # Verify the pixel values haven't changed.
        assert crc32(d.read(1).tobytes()) == 3_236_561_502  # for lambertian
        # (Rasterio's checksum is zero on some datasets for some reason? So we use crc above...)
        assert d.checksum(1) == 18_286  # for lambertian

        # The last overview is an odd size because of the tiny test data image size.
        assert d.overviews(1) == [8, 16, 31]
        assert d.driver == "GTiff"
        assert d.dtypes == ("int16",)
        assert d.compression == Compression.deflate

        assert d.height == 158
        assert d.width == 157

        # Verify the number of nodata pixels. This will change
        # with the mndwi masking threshold.
        assert len(numpy.where(d.read(1) == d.nodata)[0]) == 22_150

        # The reduced resolution makes it hard to test the chosen block size...
        assert d.block_shapes == [(26, 157)]

    # OA data should have no overviews.
    [*oa_images] = expected_folder.rglob("*_oa_*.tif")
    assert oa_images
    for image in oa_images:
        # fmask is the only OA that should have overviews according to spec (and Josh).
        if "fmask" in image.name:
            assert_image(image, overviews=[8, 16, 26])
        else:
            assert_image(image, overviews=[])

    # Check we didn't get height/width mixed up again :)
    # (The small size of our test data makes this slightly silly, though...)
    # At this current stage, the thumbnail hasn't been created.
    # [thumb_path] = expected_folder.rglob("*_nbar_*.jpg")
    # assert_image(thumb_path, bands=3, shape=(7, 8))


def test_maturity_calculation():
    from eodatasets3 import wagl

    # Simplified. Only a few ancillary parts that matter to us.
    wagl_doc = {
        "ancillary": {
            "aerosol": {
                "id": ["99d73c48-9985-51d2-9639-d37bcdfe119e"],
                "tier": "AATSR_CMP_MONTH",
                "value": 0.047_813_605_517_148_97,
            },
            "brdf": {
                "alpha_1": {
                    "band_1": 0.407_471_513_826_581_4,
                    "band_2": 0.407_472_440_438_251_7,
                    "band_3": 0.564_374_828_124_185,
                    "band_4": 0.452_550_357_394_962_35,
                    "band_5": 0.720_394_875_348_492_4,
                    "band_6": 0.475_077_458_430_413_66,
                    "band_7": 0.549_934_518_094_732,
                },
                "alpha_2": {
                    "band_1": 0.177_715_841_252_848_28,
                    "band_2": 0.177_716_091_422_247_15,
                    "band_3": 0.136_703_039_045_401_32,
                    "band_4": 0.167_629_648_004_969_63,
                    "band_5": 0.090_148_975_875_461_32,
                    "band_6": 0.121_059_126_731_143_88,
                    "band_7": 0.181_073_714_539_622_23,
                },
                "id": [
                    "2e95bdec-42e4-50a2-9a4c-1ea970e2696d",
                    "d02e1c58-7379-5c2d-a080-995838550d0d",
                ],
                "tier": "DEFINITIVE",
            },
            "elevation": {
                "id": [
                    "8ad73086-72cf-561a-aa0f-1e3c64d53384",
                    "e75ac77d-1ed0-55a5-888b-9ae48080eae9",
                ]
            },
            "ozone": {
                "id": ["83914de1-c12e-5035-af8d-e2dc1baa54d4"],
                "tier": "DEFINITIVE",
                "value": 0.295,
            },
            "water_vapour": {
                "id": ["e68035cd-1cd3-57fc-9b0e-2bf710a3df87"],
                "tier": "DEFINITIVE",
                "value": 0.490_000_009_536_743_16,
            },
        }
    }

    # July 2002 is when we consider our BRDF to be good enough: both Aqua
    # and Terra satellites were now operational.
    acq_before_brdf = datetime(2002, 6, 29, tzinfo=timezone.utc)

    acq_after_brdf = datetime(2002, 7, 1, tzinfo=timezone.utc)
    proc_after_brdf = acq_after_brdf + timedelta(days=7)

    # Normal, final dataset. Processed just outside of NRT window.
    assert (
        wagl._determine_maturity(
            acq_after_brdf, acq_after_brdf + timedelta(hours=49), wagl_doc
        )
        == "final"
    )

    # NRT when processed < 48 hours
    assert (
        wagl._determine_maturity(
            acq_after_brdf, acq_after_brdf + timedelta(hours=1), wagl_doc
        )
        == "nrt"
    )
    assert (
        wagl._determine_maturity(
            acq_before_brdf, acq_before_brdf + timedelta(hours=47), wagl_doc
        )
        == "nrt"
    )

    # Before 2001: final if water vapour is definitive.
    assert (
        wagl._determine_maturity(
            acq_before_brdf, acq_before_brdf + timedelta(days=3), wagl_doc
        )
        == "final"
    )

    # Interim whenever water vapour is fallback.
    wagl_doc["ancillary"]["water_vapour"]["tier"] = "FALLBACK_DATASET"
    assert (
        wagl._determine_maturity(acq_after_brdf, proc_after_brdf, wagl_doc) == "interim"
    )
    assert (
        wagl._determine_maturity(
            acq_before_brdf, acq_before_brdf + timedelta(days=3), wagl_doc
        )
        == "interim"
    )
    wagl_doc["ancillary"]["water_vapour"]["tier"] = "DEFINITIVE"

    # Fallback BRDF (when at least one is fallback)
    wagl_doc["ancillary"]["brdf"]["tier"] = "FALLBACK_DEFAULT"
    assert (
        wagl._determine_maturity(acq_after_brdf, proc_after_brdf, wagl_doc) == "interim"
    )
