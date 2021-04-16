import uuid
from datetime import datetime, timezone
from pathlib import Path
from textwrap import dedent
from uuid import UUID

import numpy
import pytest
from ruamel import yaml

from eodatasets3 import DatasetAssembler
from eodatasets3.images import GridSpec
from eodatasets3.model import DatasetDoc
from tests import assert_file_structure
from tests.common import assert_same_as_file


def test_dea_style_package(
    l1_ls8_dataset: DatasetDoc, l1_ls8_dataset_path: Path, tmp_path: Path
):
    out = tmp_path

    [blue_geotiff_path] = l1_ls8_dataset_path.rglob("L*_B2.TIF")

    with DatasetAssembler(out, naming_conventions="dea") as p:
        # We add a source dataset, asking to inherit the common properties (eg. platform, instrument, datetime)
        p.add_source_path(l1_ls8_dataset_path, auto_inherit_properties=True)

        # It's a GA product of "numerus-unus" ("the number one").
        p.producer = "ga.gov.au"
        p.product_family = "ones"
        p.dataset_version = "3.0.0"

        # Known properties are normalised (see tests at bottom of file)
        p.platform = "LANDSAT_8"  # to: 'landsat-8'
        p.processed = "2016-03-04 14:23:30Z"  # into a date.
        p.maturity = "FINAL"  # lowercased
        p.properties["eo:off_nadir"] = "34"  # into a number

        # Write a measurement from a numpy array, using the source dataset's grid spec.
        p.write_measurement_numpy(
            "ones",
            numpy.ones((60, 60), numpy.int16),
            GridSpec.from_dataset_doc(l1_ls8_dataset),
            nodata=-999,
        )

        # Copy a measurement from an input file (it will write a COG with DEA naming conventions)
        p.write_measurement("blue", blue_geotiff_path)

        # Alternatively, all measurements could be by reference rather that a copy:
        # p.note_measurement("external_blue", blue_geotiff_path)
        # (See an example of referencing in eodatasets3/prepare/landsat_l1_prepare.py )

        # Write a thumbnail using the given bands as r/g/b.
        p.write_thumbnail("ones", "ones", "blue")
        # Write a singleband thumbnail using a bit flag
        p.write_thumbnail_singleband("blue", bit=1, kind="singleband")
        # Write a singleband thumbnail using a lookuptable
        p.write_thumbnail_singleband(
            "blue", lookup_table={1: (0, 0, 255)}, kind="singleband_lut"
        )

        # Note any software versions important to this created data.
        p.note_software_version(
            "numerus-unus-processor",
            "https://github.com/GeoscienceAustralia/eo-datasets",
            "1.2.3",
        )

        # p.done() will validate the dataset and write it to the destination atomically.
        dataset_id, metadata_path = p.done()

    assert isinstance(dataset_id, UUID), "Expected a random UUID to be assigned"

    out = tmp_path / "ga_ls8c_ones_3/090/084/2016/01/21"
    assert out == metadata_path.parent
    assert_file_structure(
        out,
        {
            "ga_ls8c_ones_3-0-0_090084_2016-01-21_final.odc-metadata.yaml": "",
            "ga_ls8c_ones_3-0-0_090084_2016-01-21_final_blue.tif": "",
            "ga_ls8c_ones_3-0-0_090084_2016-01-21_final_ones.tif": "",
            "ga_ls8c_ones_3-0-0_090084_2016-01-21_final_thumbnail.jpg": "",
            "ga_ls8c_ones_3-0-0_090084_2016-01-21_final.proc-info.yaml": "",
            "ga_ls8c_ones_3-0-0_090084_2016-01-21_final.sha1": "",
            "ga_ls8c_singleband_3-0-0_090084_2016-01-21_final_thumbnail.jpg": "",
            "ga_ls8c_singleband_lut_3-0-0_090084_2016-01-21_final_thumbnail.jpg": "",
        },
    )

    # TODO: check sha1 checksum list.

    assert_same_as_file(
        {
            "$schema": "https://schemas.opendatacube.org/dataset",
            "id": dataset_id,
            "label": "ga_ls8c_ones_3-0-0_090084_2016-01-21_final",
            "product": {
                # This was added automatically because we chose 'dea' conventions.
                "href": "https://collections.dea.ga.gov.au/product/ga_ls8c_ones_3",
                "name": "ga_ls8c_ones_3",
            },
            "crs": "epsg:32655",
            "geometry": {
                "coordinates": [
                    [
                        [879_315.0, -3_714_585.0],
                        [641_985.0, -3_714_585.0],
                        [641_985.0, -3_953_115.0],
                        [879_315.0, -3_953_115.0],
                        [879_315.0, -3_714_585.0],
                    ]
                ],
                "type": "Polygon",
            },
            "grids": {
                # Note that the two bands had identical grid specs, so it combined them into one grid.
                "default": {
                    "shape": [60, 60],
                    "transform": [
                        3955.5,
                        0.0,
                        641_985.0,
                        0.0,
                        -3975.500_000_000_000_5,
                        -3_714_585.0,
                        0.0,
                        0.0,
                        1.0,
                    ],
                }
            },
            "measurements": {
                "blue": {"path": "ga_ls8c_ones_3-0-0_090084_2016-01-21_final_blue.tif"},
                "ones": {"path": "ga_ls8c_ones_3-0-0_090084_2016-01-21_final_ones.tif"},
            },
            "properties": {
                "datetime": datetime(2016, 1, 21, 23, 50, 23, 54435),
                "dea:dataset_maturity": "final",
                "odc:dataset_version": "3.0.0",
                "odc:file_format": "GeoTIFF",
                "odc:processing_datetime": "2016-03-04T14:23:30",
                "odc:producer": "ga.gov.au",
                "odc:product_family": "ones",
                # The remaining fields were inherited from the source dataset
                # (because we set auto_inherit_properties=True, and they're in the whitelist)
                "eo:platform": "landsat-8",  # matching Stac's examples for capitalisation.
                "eo:instrument": "OLI_TIRS",  # matching Stac's examples for capitalisation.
                "eo:cloud_cover": 93.22,
                "eo:off_nadir": 34.0,
                "eo:gsd": 15.0,
                "eo:sun_azimuth": 74.007_443_8,
                "eo:sun_elevation": 55.486_483,
                "landsat:collection_category": "T1",
                "landsat:collection_number": 1,
                "landsat:landsat_product_id": "LC08_L1TP_090084_20160121_20170405_01_T1",
                "landsat:landsat_scene_id": "LC80900842016021LGN02",
                "landsat:wrs_path": 90,
                "landsat:wrs_row": 84,
                "odc:region_code": "090084",
            },
            "accessories": {
                # It wrote a checksum file for all of our files.
                "checksum:sha1": {
                    "path": "ga_ls8c_ones_3-0-0_090084_2016-01-21_final.sha1"
                },
                # We didn't add any extra processor metadata, so this just contains
                # some software versions.
                "metadata:processor": {
                    "path": "ga_ls8c_ones_3-0-0_090084_2016-01-21_final.proc-info.yaml"
                },
                # The thumbnails we made.
                "thumbnail": {
                    "path": "ga_ls8c_ones_3-0-0_090084_2016-01-21_final_thumbnail.jpg"
                },
                "thumbnail:singleband": {
                    "path": "ga_ls8c_singleband_3-0-0_090084_2016-01-21_final_thumbnail.jpg"
                },
                "thumbnail:singleband_lut": {
                    "path": "ga_ls8c_singleband_lut_3-0-0_090084_2016-01-21_final_thumbnail.jpg"
                },
            },
            "lineage": {"level1": ["a780754e-a884-58a7-9ac0-df518a67f59d"]},
        },
        generated_file=metadata_path,
    )


def test_minimal_package(tmp_path: Path, l1_ls8_folder: Path):
    """
    What's the minimum number of fields we can set and still produce a package?
    """

    out = tmp_path / "out"
    out.mkdir()

    [blue_geotiff_path] = l1_ls8_folder.rglob("L*_B2.TIF")

    with DatasetAssembler(out) as p:
        p.datetime = datetime(2019, 7, 4, 13, 7, 5)
        p.product_family = "quaternarius"
        p.processed_now()

        p.write_measurement("blue", blue_geotiff_path)

        # A friendly __str__ for notebook/terminal users:
        assert str(p) == dedent(
            f"""
            Assembling quaternarius (unfinished)
            - 1 measurements: blue
            - 4 properties: datetime, odc:file_format, odc:processing_datetime, odc:prod...
            Writing to {out}
        """
        )

        # p.done() will validate the dataset and write it to the destination atomically.
        dataset_id, metadata_path = p.done()

    assert dataset_id is not None
    assert_file_structure(
        out,
        {
            "quaternarius": {
                "2019": {
                    "07": {
                        "04": {
                            # Set a dataset version to get rid of 'beta' label.
                            "quaternarius_2019-07-04.odc-metadata.yaml": "",
                            "quaternarius_2019-07-04.proc-info.yaml": "",
                            "quaternarius_2019-07-04_blue.tif": "",
                            "quaternarius_2019-07-04.sha1": "",
                        }
                    }
                }
            }
        },
    )


def test_dataset_no_measurements(tmp_path: Path):
    """Can we make a dataset with no measurements? (eg. telemetry data)"""
    with DatasetAssembler(tmp_path) as p:
        # A custom label too.
        p.label = "chipmonk_sightings_2019"
        p.datetime = datetime(2019, 1, 1)
        p.product_family = "chipmonk_sightings"
        p.processed_now()

        dataset_id, metadata_path = p.done()

    with metadata_path.open("r") as f:
        doc = yaml.safe_load(f)

    assert doc["label"] == "chipmonk_sightings_2019", "Couldn't override label field"


def test_minimal_s1_dataset(tmp_path: Path):
    """A minimal dataset with sentinel-1a/b platform/instrument"""
    with DatasetAssembler(tmp_path) as p:
        # A custom label too.
        p.platform = "sentinel-1a"
        p.instrument = "c-sar"
        p.datetime = datetime(2018, 11, 4)
        p.product_family = "bck"
        p.processed = "2018-11-05T12:23:23"

        dataset_id, metadata_path = p.done()

    with metadata_path.open("r") as f:
        doc = yaml.safe_load(f)

    assert doc["label"] == "s1ac_bck_2018-11-04", "Unexpected dataset label"


def test_minimal_s2_dataset_normal(tmp_path: Path):
    """A minimal dataset with sentinel platform/instrument"""
    with DatasetAssembler(tmp_path) as p:
        # A custom label too.
        p.platform = "sentinel-2a"
        p.instrument = "msi"
        p.datetime = datetime(2018, 11, 4)
        p.product_family = "blueberries"
        p.processed = "2018-11-05T12:23:23"
        p.properties[
            "sentinel:sentinel_tile_id"
        ] = "S2A_OPER_MSI_L1C_TL_SGS__20170822T015626_A011310_T54KYU_N02.05"

        dataset_id, metadata_path = p.done()

    with metadata_path.open("r") as f:
        doc = yaml.safe_load(f)

    metadata_path_offset = metadata_path.relative_to(tmp_path).as_posix()
    assert metadata_path_offset == (
        "s2am_blueberries/2018/11/04/s2am_blueberries_2018-11-04.odc-metadata.yaml"
    )

    assert doc["label"] == "s2am_blueberries_2018-11-04", "Unexpected dataset label"


def test_s2_naming_conventions(tmp_path: Path):
    """A minimal dataset with sentinel platform/instrument"""
    p = DatasetAssembler(tmp_path, naming_conventions="dea_s2")
    p.platform = "sentinel-2a"
    p.instrument = "msi"
    p.datetime = datetime(2018, 11, 4)
    p.product_family = "blueberries"
    p.processed = "2018-11-05T12:23:23"
    p.producer = "ga.gov.au"
    p.dataset_version = "1.0.0"
    p.region_code = "Oz"
    p.properties["odc:file_format"] = "GeoTIFF"
    p.properties[
        "sentinel:sentinel_tile_id"
    ] = "S2A_OPER_MSI_L1C_TL_SGS__20170822T015626_A011310_T54KYU_N02.05"

    p.note_source_datasets(
        "telemetry",
        # Accepts multiple, and they can be strings or UUIDs:
        "ca705033-0fc4-4f38-a47e-f425dfb4d0c7",
        uuid.UUID("3781e90f-b677-40af-9439-b40f6e4dfadd"),
    )

    # The property normaliser should have extracted inner fields
    assert p.properties["sentinel:datatake_start_datetime"] == datetime(
        2017, 8, 22, 1, 56, 26, tzinfo=timezone.utc
    )

    dataset_id, metadata_path = p.done()

    # The s2 naming conventions have an extra subfolder of the datatake start time.
    metadata_path_offset = metadata_path.relative_to(tmp_path).as_posix()

    assert metadata_path_offset == (
        "ga_s2am_blueberries_1/Oz/2018/11/04/20170822T015626/"
        "ga_s2am_blueberries_1-0-0_Oz_2018-11-04.odc-metadata.yaml"
    )

    assert_same_as_file(
        {
            "$schema": "https://schemas.opendatacube.org/dataset",
            "accessories": {
                "checksum:sha1": {
                    "path": "ga_s2am_blueberries_1-0-0_Oz_2018-11-04.sha1"
                },
                "metadata:processor": {
                    "path": "ga_s2am_blueberries_1-0-0_Oz_2018-11-04.proc-info.yaml"
                },
            },
            "id": dataset_id,
            "label": "ga_s2am_blueberries_1-0-0_Oz_2018-11-04",
            "product": {
                "href": "https://collections.dea.ga.gov.au/product/ga_s2am_blueberries_1",
                "name": "ga_s2am_blueberries_1",
            },
            "properties": {
                "datetime": datetime(2018, 11, 4, 0, 0),
                "eo:instrument": "msi",
                "eo:platform": "sentinel-2a",
                "odc:dataset_version": "1.0.0",
                "odc:file_format": "GeoTIFF",
                "odc:processing_datetime": datetime(2018, 11, 5, 12, 23, 23),
                "odc:producer": "ga.gov.au",
                "odc:product_family": "blueberries",
                "odc:region_code": "Oz",
                "sentinel:datatake_start_datetime": datetime(2017, 8, 22, 1, 56, 26),
                "sentinel:sentinel_tile_id": "S2A_OPER_MSI_L1C_TL_SGS__20170822T015626_A011310_T54KYU_N02.05",
            },
            "lineage": {
                "telemetry": [
                    "ca705033-0fc4-4f38-a47e-f425dfb4d0c7",
                    "3781e90f-b677-40af-9439-b40f6e4dfadd",
                ]
            },
        },
        generated_file=metadata_path,
    )


def test_complain_about_missing_fields(tmp_path: Path, l1_ls8_folder: Path):
    """
    It should complain immediately if I add a file without enough metadata to write the filename.

    (and with a friendly error message)
    """

    out = tmp_path / "out"
    out.mkdir()

    [blue_geotiff_path] = l1_ls8_folder.rglob("L*_B2.TIF")

    # Default simple naming conventions need at least a date and family...
    with pytest.raises(
        ValueError, match="Need more properties to fulfill naming conventions."
    ):
        with DatasetAssembler(out) as p:
            p.write_measurement("blue", blue_geotiff_path)

    # It should mention the field that's missing (we added a date, so product_family is needed)
    with DatasetAssembler(out) as p:
        with pytest.raises(ValueError, match="odc:product_family"):
            p.datetime = datetime(2019, 7, 4, 13, 7, 5)
            p.write_measurement("blue", blue_geotiff_path)

    # DEA naming conventions should have stricter standards, and will tell your which fields you need to add.
    with DatasetAssembler(out, naming_conventions="dea") as p:
        # We set all the fields that work in default naming conventions.
        p.datetime = datetime(2019, 7, 4, 13, 7, 5)
        p.product_family = "quaternarius"
        p.processed_now()

        # These fields are mandatory for DEA, and so should be complained about.
        expected_extra_fields_needed = (
            "eo:platform",
            "eo:instrument",
            "odc:dataset_version",
            "odc:producer",
            "odc:region_code",
        )
        with pytest.raises(ValueError) as got_error:
            p.write_measurement("blue", blue_geotiff_path)

        # All needed fields should have been in the error message.
        for needed_field_name in expected_extra_fields_needed:
            assert needed_field_name in got_error.value.args[0], (
                f"Expected field {needed_field_name} to "
                f"be listed as mandatory in the error message"
            )


def test_dea_interim_folder_calculation(tmp_path: Path):
    """
    DEA Naming conventions should include maturity in the folder name
    when it's not a 'final' dataset.
    """
    with DatasetAssembler(tmp_path, naming_conventions="dea") as p:
        p.platform = "landsat-7"
        p.instrument = "ETM+"
        p.datetime = datetime(1998, 7, 30)
        p.product_family = "frogs"
        p.processed = "1999-11-20 00:00:53.152462Z"
        p.maturity = "interim"
        p.producer = "ga.gov.au"
        p.properties["landsat:landsat_scene_id"] = "LE70930821999324EDC00"
        p.dataset_version = "1.2.3"
        p.region_code = "093082"

        p.done()

    [metadata_path] = tmp_path.rglob("*.odc-metadata.yaml")
    calculated_path: Path = metadata_path.relative_to(tmp_path)
    assert calculated_path == Path(
        #                                  ⇩⇩⇩⇩⇩⇩⇩⇩ Adds interim flag
        "ga_ls7e_frogs_1/093/082/1998/07/30_interim/ga_ls7e_frogs_1-2-3_093082_1998-07-30_interim.odc-metadata.yaml"
    )


def test_dea_c3_naming_conventions(tmp_path: Path):
    """
    A sample scene for Alchemist C3 processing that tests the naming conventions.
    """
    p = DatasetAssembler(tmp_path, naming_conventions="dea_c3")
    p.platform = "landsat-7"
    p.datetime = datetime(1998, 7, 30)
    p.product_family = "wo"
    p.processed = "1998-07-30T12:23:23"
    p.maturity = "interim"
    p.producer = "ga.gov.au"
    p.region_code = "090081"

    # Try missing few fields and expect ValueError
    with pytest.raises(
        ValueError, match="Need more properties to fulfill naming conventions."
    ):
        p.done()

    # Put back the missed ones
    p.dataset_version = "1.6.0"
    p.collection_number = "3"

    # Collection number returned as integer via the getter.
    assert p.collection_number == 3

    # Success case
    dataset_id, metadata_path = p.done()
    metadata_path_offset = metadata_path.relative_to(tmp_path).as_posix()
    assert (
        metadata_path_offset
        == "ga_ls_wo_3/1-6-0/090/081/1998/07/30/ga_ls_wo_3_090081_1998-07-30_interim.odc-metadata.yaml"
    )


@pytest.mark.parametrize(
    "inherit_geom",
    [True, False],
    ids=["inherit geom from dataset", "don't inherit geom"],
)
def test_add_source_dataset(tmp_path: Path, inherit_geom):
    from eodatasets3 import serialise

    p = DatasetAssembler(tmp_path, naming_conventions="dea_c3")
    source_dataset = serialise.from_path(
        Path(__file__).parent / "data/LC08_L1TP_089080_20160302_20170328_01_T1.yaml"
    )
    p.add_source_dataset(
        source_dataset, auto_inherit_properties=True, inherit_geometry=inherit_geom
    )

    p.maturity = "interim"
    p.collection_number = "3"
    p.dataset_version = "1.6.0"
    p.producer = "ga.gov.au"
    p.processed = "1998-07-30T12:23:23"
    p.product_family = "wofs"
    p.write_measurement(
        "water",
        Path(__file__).parent
        / "data/wofs/ga_ls_wofs_3_099081_2020-07-26_interim_water_clipped.tif",
    )

    id, path = p.done()

    output = serialise.from_path(path)
    if inherit_geom:
        # POLYGON((609615 - 3077085, 378285 - 3077085, 378285 - 3310515, 609615 - 3310515, 609615 - 3077085))
        assert output.geometry == source_dataset.geometry
    else:
        # POLYGON((684285 - 3439275, 684285 - 3444495, 689925 - 3444495, 689925 - 3439275, 684285 - 3439275))
        # Geometry is not set from the source dataset, but instead from the added wofs measurement
        assert output.geometry != source_dataset.geometry
