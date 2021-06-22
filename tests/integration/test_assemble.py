"""
Basic tests of DatasetAssembler.

Some features are testsed in other sibling test files, such as alternative
naming conventions.
"""
from datetime import datetime, timezone
from pathlib import Path
from textwrap import dedent
from uuid import UUID

import numpy
import pytest
from ruamel import yaml

from eodatasets3 import DatasetAssembler, namer
from eodatasets3.images import GridSpec
from eodatasets3.model import DatasetDoc
from tests import assert_file_structure
from tests.common import assert_expected_eo3_doc


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
            "ga_ls8c_ones_3-0-0_090084_2016-01-21_final_singleband-thumbnail.jpg": "",
            "ga_ls8c_ones_3-0-0_090084_2016-01-21_final_singleband-lut-thumbnail.jpg": "",
        },
    )

    # TODO: check sha1 checksum list.

    assert_expected_eo3_doc(
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
                    "path": "ga_ls8c_ones_3-0-0_090084_2016-01-21_final_singleband-thumbnail.jpg"
                },
                "thumbnail:singleband_lut": {
                    "path": "ga_ls8c_ones_3-0-0_090084_2016-01-21_final_singleband-lut-thumbnail.jpg"
                },
            },
            "lineage": {"level1": ["a780754e-a884-58a7-9ac0-df518a67f59d"]},
        },
        metadata_path,
    )


def test_minimal_package_with_product_name(tmp_path: Path, l1_ls8_folder: Path):
    """
    You can specify an ODC product name manually to avoid most of the name generation.
    """
    out = tmp_path / "out"
    out.mkdir()

    [blue_geotiff_path] = l1_ls8_folder.rglob("L*_B2.TIF")

    with DatasetAssembler(out) as p:
        p.datetime = datetime(2019, 7, 4, 13, 7, 5)
        p.product_name = "loch_ness_sightings"
        p.processed = datetime(2019, 7, 4, 13, 8, 7)

        p.write_measurement("blue", blue_geotiff_path)

        dataset_id, metadata_path = p.done()

    assert dataset_id is not None
    assert_file_structure(
        out,
        {
            "loch_ness_sightings": {
                "2019": {
                    "07": {
                        "04": {
                            # Set a dataset version to get rid of 'beta' label.
                            "loch_ness_sightings_2019-07-04.odc-metadata.yaml": "",
                            "loch_ness_sightings_2019-07-04.proc-info.yaml": "",
                            "loch_ness_sightings_2019-07-04_blue.tif": "",
                            "loch_ness_sightings_2019-07-04.sha1": "",
                        }
                    }
                }
            }
        },
    )


def test_minimal_generated_naming_package(tmp_path: Path, l1_ls8_folder: Path):
    """
    What's the minimum number of fields we can set and still generate file/product
    names to produce a package?
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
            Writing to location: {out}/quaternarius/2019/07/04/quaternarius_2019-07-04.odc-metadata.yaml
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
        doc = yaml.YAML(typ="safe").load(f)

    assert doc["label"] == "chipmonk_sightings_2019", "Couldn't override label field"


def test_dataset_given_properties(tmp_path: Path):
    """Can we give existing properties to the assembler?"""

    properties = {
        "datetime": datetime(2019, 1, 1),
        "odc:product_family": "chipmonk_sightings",
        "odc:processing_datetime": "2021-06-15T01:33:43.378850",
    }
    names = namer(properties=properties)
    with DatasetAssembler(tmp_path, names=names) as p:
        # It should have normalised properties!
        assert p.processed == datetime(2021, 6, 15, 1, 33, 43, 378850, timezone.utc)

        dataset_id, metadata_path = p.done()

    relative_path = metadata_path.relative_to(tmp_path)
    assert relative_path == Path(
        "chipmonk_sightings/2019/01/01/chipmonk_sightings_2019-01-01.odc-metadata.yaml"
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
