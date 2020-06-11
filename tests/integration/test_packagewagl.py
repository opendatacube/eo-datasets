from binascii import crc32
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Tuple

import numpy as np
import pytest
import rasterio
from click.testing import CliRunner
from rasterio import DatasetReader
from rasterio.enums import Compression
from rio_cogeo import cogeo
from tests.integration.common import assert_same_as_file

import eodatasets3
from eodatasets3.model import DatasetDoc
from tests import assert_file_structure

h5py = pytest.importorskip(
    "h5py",
    reason="Extra dependencies needed to run wagl package test. "
    "Try pip install eodatasets3[wagl]",
)

# This test dataset comes from running `tests/integration/h5downsample.py` on a real
# wagl output.
WAGL_INPUT_PATH: Path = Path(
    __file__
).parent / "data/wagl-input/LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
# The matching Level1 metadata (produced by landsat_l1_prepare.py)
L1_METADATA_PATH: Path = Path(
    __file__
).parent / "data/wagl-input/LC08_L1TP_092084_20160628_20170323_01_T1.yaml"


def test_whole_wagl_package(
    l1_ls8_dataset: DatasetDoc, l1_ls8_folder: Path, tmp_path: Path
):
    out = tmp_path

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
    expected_folder = out / "ga_ls8c_ard_3/092/084/2016/06/28"
    assert_file_structure(
        expected_folder,
        {
            "ga_ls8c_ard_3-1-0_092084_2016-06-28_final.odc-metadata.yaml": "",
            "ga_ls8c_ard_3-1-0_092084_2016-06-28_final.proc-info.yaml": "",
            "ga_ls8c_ard_3-1-0_092084_2016-06-28_final.sha1": "",
            "ga_ls8c_nbar_3-1-0_092084_2016-06-28_final_band01.tif": "",
            "ga_ls8c_nbar_3-1-0_092084_2016-06-28_final_band02.tif": "",
            "ga_ls8c_nbar_3-1-0_092084_2016-06-28_final_band03.tif": "",
            "ga_ls8c_nbar_3-1-0_092084_2016-06-28_final_band04.tif": "",
            "ga_ls8c_nbar_3-1-0_092084_2016-06-28_final_band05.tif": "",
            "ga_ls8c_nbar_3-1-0_092084_2016-06-28_final_band06.tif": "",
            "ga_ls8c_nbar_3-1-0_092084_2016-06-28_final_band07.tif": "",
            "ga_ls8c_nbar_3-1-0_092084_2016-06-28_final_band08.tif": "",
            "ga_ls8c_nbar_3-1-0_092084_2016-06-28_final_thumbnail.jpg": "",
            "ga_ls8c_nbart_3-1-0_092084_2016-06-28_final_band01.tif": "",
            "ga_ls8c_nbart_3-1-0_092084_2016-06-28_final_band02.tif": "",
            "ga_ls8c_nbart_3-1-0_092084_2016-06-28_final_band03.tif": "",
            "ga_ls8c_nbart_3-1-0_092084_2016-06-28_final_band04.tif": "",
            "ga_ls8c_nbart_3-1-0_092084_2016-06-28_final_band05.tif": "",
            "ga_ls8c_nbart_3-1-0_092084_2016-06-28_final_band06.tif": "",
            "ga_ls8c_nbart_3-1-0_092084_2016-06-28_final_band07.tif": "",
            "ga_ls8c_nbart_3-1-0_092084_2016-06-28_final_band08.tif": "",
            "ga_ls8c_nbart_3-1-0_092084_2016-06-28_final_thumbnail.jpg": "",
            "ga_ls8c_oa_3-1-0_092084_2016-06-28_final_azimuthal-exiting.tif": "",
            "ga_ls8c_oa_3-1-0_092084_2016-06-28_final_azimuthal-incident.tif": "",
            "ga_ls8c_oa_3-1-0_092084_2016-06-28_final_combined-terrain-shadow.tif": "",
            "ga_ls8c_oa_3-1-0_092084_2016-06-28_final_exiting-angle.tif": "",
            "ga_ls8c_oa_3-1-0_092084_2016-06-28_final_fmask.tif": "",
            "ga_ls8c_oa_3-1-0_092084_2016-06-28_final_incident-angle.tif": "",
            "ga_ls8c_oa_3-1-0_092084_2016-06-28_final_nbar-contiguity.tif": "",
            "ga_ls8c_oa_3-1-0_092084_2016-06-28_final_nbart-contiguity.tif": "",
            "ga_ls8c_oa_3-1-0_092084_2016-06-28_final_relative-azimuth.tif": "",
            "ga_ls8c_oa_3-1-0_092084_2016-06-28_final_relative-slope.tif": "",
            "ga_ls8c_oa_3-1-0_092084_2016-06-28_final_satellite-azimuth.tif": "",
            "ga_ls8c_oa_3-1-0_092084_2016-06-28_final_satellite-view.tif": "",
            "ga_ls8c_oa_3-1-0_092084_2016-06-28_final_solar-azimuth.tif": "",
            "ga_ls8c_oa_3-1-0_092084_2016-06-28_final_solar-zenith.tif": "",
            "ga_ls8c_oa_3-1-0_092084_2016-06-28_final_time-delta.tif": "",
        },
    )
    [output_metadata] = expected_folder.rglob("*.odc-metadata.yaml")

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
    [image] = expected_folder.rglob("*_oa_*nbar-contiguity.tif")
    _assert_image(image, nodata=255, unique_pixel_counts={0: 1978, 1: 4184})

    [image] = expected_folder.rglob("*_oa_*nbart-contiguity.tif")
    _assert_image(image, nodata=255, unique_pixel_counts={0: 1979, 1: 4183})

    assert_same_as_file(
        {
            "$schema": "https://schemas.opendatacube.org/dataset",
            # A stable ID is taken from the WAGL doc.
            "id": "787eb74c-e7df-43d6-b562-b796137330ae",
            "label": "ga_ls8c_ard_3-1-0_092084_2016-06-28_final",
            "product": {
                "href": "https://collections.dea.ga.gov.au/product/ga_ls8c_ard_3",
                "name": "ga_ls8c_ard_3",
            },
            "crs": "epsg:32655",
            "geometry": {
                "coordinates": [
                    [
                        [386_170.809_107_605_5, -3_787_581.737_315_514_6],
                        [393_422.698_122_467_44, -3_754_539.332_156_166_4],
                        [402_370.463_567_812_2, -3_717_207.883_853_628_3],
                        [405_296.703_429_750_9, -3_713_106.822_612_258_6],
                        [405_302.307_692_307_7, -3_713_085.0],
                        [560_999.714_134_832_8, -3_745_790.820_117_99],
                        [591_203.344_050_317_7, -3_755_934.776_849_929_2],
                        [593_107.5, -3_756_373.614_649_681_4],
                        [593_066.089_284_004_1, -3_756_560.384_007_281_6],
                        [593_115.0, -3_756_576.810_780_758],
                        [593_115.0, -3_769_934.639_090_926_4],
                        [555_895.771_981_598_6, -3_924_204.823_795_153],
                        [554_316.830_569_659_8, -3_931_326.117_549_759],
                        [553_913.572_308_820_1, -3_932_420.854_216_015],
                        [550_505.686_408_068, -3_946_546.219_392_854],
                        [548_673.645_879_151_9, -3_946_645.831_477_726_3],
                        [548_393.076_923_077, -3_947_407.5],
                        [543_888.417_289_877_3, -3_946_906.014_911_907],
                        [535_826.373_854_402_9, -3_947_344.365_997_631_6],
                        [362_232.941_315_876_84, -3_905_575.014_223_633],
                        [362_109.819_892_458_1, -3_904_490.351_889_350_5],
                        [360_592.5, -3_904_126.385_350_318_6],
                        [361_565.347_585_850_9, -3_899_693.716_286_561_5],
                        [360_585.0, -3_891_057.151_898_734_3],
                        [366_618.297_729_428_5, -3_863_717.869_440_751],
                        [386_170.809_107_605_5, -3_787_581.737_315_514_6],
                    ]
                ],
                "type": "Polygon",
            },
            "grids": {
                "default": {
                    "shape": [79, 78],
                    "transform": [
                        2981.153_846_153_846,
                        0.0,
                        360_585.0,
                        0.0,
                        -2966.202_531_645_569_7,
                        -3_713_085.0,
                        0.0,
                        0.0,
                        1.0,
                    ],
                },
                "panchromatic": {
                    "shape": [157, 156],
                    "transform": [
                        1490.480_769_230_769_3,
                        0.0,
                        360_592.5,
                        0.0,
                        -1492.452_229_299_363,
                        -3_713_092.5,
                        0.0,
                        0.0,
                        1.0,
                    ],
                },
            },
            "properties": {
                "datetime": datetime(2016, 6, 28, 0, 2, 28, 624_635),
                "dea:dataset_maturity": "final",
                "dtr:end_datetime": datetime(2016, 6, 28, 0, 2, 43, 114_771),
                "dtr:start_datetime": datetime(2016, 6, 28, 0, 2, 14, 25815),
                "eo:cloud_cover": 63.069_613_577_531_236,
                "eo:gsd": 1490.480_769_230_769_3,
                "eo:instrument": "OLI_TIRS",
                "eo:platform": "landsat-8",
                "eo:sun_azimuth": 33.655_125_34,
                "eo:sun_elevation": 23.988_361_72,
                "fmask:clear": 32.735_343_657_403_305,
                "fmask:cloud": 63.069_613_577_531_236,
                "fmask:cloud_shadow": 4.139_470_857_647_722,
                "fmask:snow": 0.005_053_323_801_138_007,
                "fmask:water": 0.050_518_583_616_596_675,
                "gqa:abs_iterative_mean_x": 0.21,
                "gqa:abs_iterative_mean_xy": 0.27,
                "gqa:abs_iterative_mean_y": 0.18,
                "gqa:abs_x": 0.3,
                "gqa:abs_xy": 0.39,
                "gqa:abs_y": 0.25,
                "gqa:cep90": 0.46,
                "gqa:iterative_mean_x": -0.17,
                "gqa:iterative_mean_xy": 0.21,
                "gqa:iterative_mean_y": 0.12,
                "gqa:iterative_stddev_x": 0.19,
                "gqa:iterative_stddev_xy": 0.25,
                "gqa:iterative_stddev_y": 0.17,
                "gqa:mean_x": -0.1,
                "gqa:mean_xy": 0.14,
                "gqa:mean_y": 0.1,
                "gqa:stddev_x": 0.35,
                "gqa:stddev_xy": 0.45,
                "gqa:stddev_y": 0.29,
                "landsat:collection_category": "T1",
                "landsat:collection_number": 1,
                "landsat:landsat_product_id": "LC08_L1TP_092084_20160628_20170323_01_T1",
                "landsat:landsat_scene_id": "LC80920842016180LGN01",
                "landsat:wrs_path": 92,
                "landsat:wrs_row": 84,
                "odc:dataset_version": "3.1.0",
                "odc:file_format": "GeoTIFF",
                "odc:processing_datetime": datetime(2019, 7, 11, 23, 29, 29, 21245),
                "odc:producer": "ga.gov.au",
                "odc:product_family": "ard",
                "odc:region_code": "092084",
            },
            "measurements": {
                "nbar_blue": {
                    "path": "ga_ls8c_nbar_3-1-0_092084_2016-06-28_final_band02.tif"
                },
                "nbar_coastal_aerosol": {
                    "path": "ga_ls8c_nbar_3-1-0_092084_2016-06-28_final_band01.tif"
                },
                "nbar_green": {
                    "path": "ga_ls8c_nbar_3-1-0_092084_2016-06-28_final_band03.tif"
                },
                "nbar_nir": {
                    "path": "ga_ls8c_nbar_3-1-0_092084_2016-06-28_final_band05.tif"
                },
                "nbar_panchromatic": {
                    "grid": "panchromatic",
                    "path": "ga_ls8c_nbar_3-1-0_092084_2016-06-28_final_band08.tif",
                },
                "nbar_red": {
                    "path": "ga_ls8c_nbar_3-1-0_092084_2016-06-28_final_band04.tif"
                },
                "nbar_swir_1": {
                    "path": "ga_ls8c_nbar_3-1-0_092084_2016-06-28_final_band06.tif"
                },
                "nbar_swir_2": {
                    "path": "ga_ls8c_nbar_3-1-0_092084_2016-06-28_final_band07.tif"
                },
                "nbart_blue": {
                    "path": "ga_ls8c_nbart_3-1-0_092084_2016-06-28_final_band02.tif"
                },
                "nbart_coastal_aerosol": {
                    "path": "ga_ls8c_nbart_3-1-0_092084_2016-06-28_final_band01.tif"
                },
                "nbart_green": {
                    "path": "ga_ls8c_nbart_3-1-0_092084_2016-06-28_final_band03.tif"
                },
                "nbart_nir": {
                    "path": "ga_ls8c_nbart_3-1-0_092084_2016-06-28_final_band05.tif"
                },
                "nbart_panchromatic": {
                    "grid": "panchromatic",
                    "path": "ga_ls8c_nbart_3-1-0_092084_2016-06-28_final_band08.tif",
                },
                "nbart_red": {
                    "path": "ga_ls8c_nbart_3-1-0_092084_2016-06-28_final_band04.tif"
                },
                "nbart_swir_1": {
                    "path": "ga_ls8c_nbart_3-1-0_092084_2016-06-28_final_band06.tif"
                },
                "nbart_swir_2": {
                    "path": "ga_ls8c_nbart_3-1-0_092084_2016-06-28_final_band07.tif"
                },
                "oa_azimuthal_exiting": {
                    "path": "ga_ls8c_oa_3-1-0_092084_2016-06-28_final_azimuthal-exiting.tif"
                },
                "oa_azimuthal_incident": {
                    "path": "ga_ls8c_oa_3-1-0_092084_2016-06-28_final_azimuthal-incident.tif"
                },
                "oa_combined_terrain_shadow": {
                    "path": "ga_ls8c_oa_3-1-0_092084_2016-06-28_final_combined-terrain-shadow.tif"
                },
                "oa_exiting_angle": {
                    "path": "ga_ls8c_oa_3-1-0_092084_2016-06-28_final_exiting-angle.tif"
                },
                "oa_fmask": {
                    "path": "ga_ls8c_oa_3-1-0_092084_2016-06-28_final_fmask.tif"
                },
                "oa_incident_angle": {
                    "path": "ga_ls8c_oa_3-1-0_092084_2016-06-28_final_incident-angle.tif"
                },
                "oa_nbar_contiguity": {
                    "path": "ga_ls8c_oa_3-1-0_092084_2016-06-28_final_nbar-contiguity.tif"
                },
                "oa_nbart_contiguity": {
                    "path": "ga_ls8c_oa_3-1-0_092084_2016-06-28_final_nbart-contiguity.tif"
                },
                "oa_relative_azimuth": {
                    "path": "ga_ls8c_oa_3-1-0_092084_2016-06-28_final_relative-azimuth.tif"
                },
                "oa_relative_slope": {
                    "path": "ga_ls8c_oa_3-1-0_092084_2016-06-28_final_relative-slope.tif"
                },
                "oa_satellite_azimuth": {
                    "path": "ga_ls8c_oa_3-1-0_092084_2016-06-28_final_satellite-azimuth.tif"
                },
                "oa_satellite_view": {
                    "path": "ga_ls8c_oa_3-1-0_092084_2016-06-28_final_satellite-view.tif"
                },
                "oa_solar_azimuth": {
                    "path": "ga_ls8c_oa_3-1-0_092084_2016-06-28_final_solar-azimuth.tif"
                },
                "oa_solar_zenith": {
                    "path": "ga_ls8c_oa_3-1-0_092084_2016-06-28_final_solar-zenith.tif"
                },
                "oa_time_delta": {
                    "path": "ga_ls8c_oa_3-1-0_092084_2016-06-28_final_time-delta.tif"
                },
            },
            "accessories": {
                "checksum:sha1": {
                    "path": "ga_ls8c_ard_3-1-0_092084_2016-06-28_final.sha1"
                },
                "metadata:processor": {
                    "path": "ga_ls8c_ard_3-1-0_092084_2016-06-28_final.proc-info.yaml"
                },
                "thumbnail:nbar": {
                    "path": "ga_ls8c_nbar_3-1-0_092084_2016-06-28_final_thumbnail.jpg"
                },
                "thumbnail:nbart": {
                    "path": "ga_ls8c_nbart_3-1-0_092084_2016-06-28_final_thumbnail.jpg"
                },
            },
            "lineage": {"level1": ["fb1c622e-90aa-50e8-9d5e-ad69db82d0f6"]},
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
                    "clear": 32.735_343_657_403_305,
                    "cloud": 63.069_613_577_531_236,
                    "cloud_shadow": 4.139_470_857_647_722,
                    "snow": 0.005_053_323_801_138_007,
                    "water": 0.050_518_583_616_596_675,
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
                    "version": "5.3.1+118.g9edd420",
                },
                {
                    "name": "eugl",
                    "url": "https://github.com/OpenDataCubePipelines/eugl.git",
                    "version": "0.0.2+69.gb1d1231",
                },
                {"name": "gverify", "url": None, "version": "v0.25c"},
                {
                    "name": "fmask",
                    "url": "https://bitbucket.org/chchrsc/python-fmask",
                    "version": "0.5.3",
                },
                {
                    "name": "tesp",
                    "url": "https://github.com/OpenDataCubePipelines/tesp.git",
                    "version": "0.6.1",
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
    [image] = expected_folder.rglob("*_nbar_*_band08.tif")
    with rasterio.open(image) as d:
        d: DatasetReader
        assert d.count == 1, "Expected one band"
        assert d.nodata == -999.0

        # Verify the pixel values haven't changed.
        assert crc32(d.read(1).tobytes()) == 3_381_159_350
        # (Rasterio's checksum is zero on some datasets for some reason? So we use crc above...)
        assert d.checksum(1) == 58403

        # The last overview is an odd size because of the tiny test data image size.
        assert d.overviews(1) == [8, 16, 31]
        assert d.driver == "GTiff"
        assert d.dtypes == ("int16",)
        assert d.compression == Compression.deflate

        assert d.height == 157
        assert d.width == 156

        # The reduced resolution makes it hard to test the chosen block size...
        assert d.block_shapes == [(26, 156)]

    # OA data should have no overviews.
    [*oa_images] = expected_folder.rglob("*_oa_*.tif")
    assert oa_images
    for image in oa_images:
        # fmask is the only OA that should have overviews according to spec (and Josh).
        if "fmask" in image.name:
            _assert_image(image, overviews=[8, 16, 26])
        else:
            _assert_image(image, overviews=[])

    # Check we didn't get height/width mixed up again :)
    # (The small size of our test data makes this slightly silly, though...)
    [thumb_path] = expected_folder.rglob("*_nbar_*.jpg")
    _assert_image(thumb_path, bands=3, shape=(7, 8))


allow_anything = object()


def _assert_image(
    image: Path,
    overviews=allow_anything,
    nodata=allow_anything,
    unique_pixel_counts: Dict = allow_anything,
    bands=1,
    shape: Tuple[int, int] = None,
):
    __tracebackhide__ = True
    with rasterio.open(image) as d:
        d: DatasetReader
        assert d.count == bands, f"Expected {bands} band{'s' if bands > 1 else ''}"

        if overviews is not allow_anything:
            assert d.overviews(1) == overviews
        if nodata is not allow_anything:
            assert d.nodata == nodata

        if unique_pixel_counts is not allow_anything:
            array = d.read(1)
            value_counts = dict(zip(*np.unique(array, return_counts=True)))
            assert value_counts == unique_pixel_counts

        if shape:
            assert shape == d.shape, f"Unexpected shape: {shape!r} != {d.shape!r}"


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
