from datetime import datetime
from pathlib import Path
from uuid import UUID

import numpy

from eodatasets3.assemble import DatasetAssembler
from eodatasets3.images import GridSpec
from eodatasets3.model import DatasetDoc
from tests import assert_file_structure
from tests.integration.common import assert_same_as_file


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
        p.properties["dea:dataset_maturity"] = "FINAL"  # lowercased
        p.properties["dea:processing_level"] = "level-2"

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
        # (See an example of referencing in eodatasets3/prepare/ls_usgs_l1_prepare.py )

        # Write a thumbnail using the given bands as r/g/b.
        p.write_thumbnail("ones", "ones", "blue")

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
        },
    )

    # TODO: check sha1 checksum list.

    assert_same_as_file(
        {
            "$schema": "https://schemas.opendatacube.org/dataset",
            "id": dataset_id,
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
                "dea:processing_level": "level-2",
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
                # The thumbnail we made.
                "thumbnail": {
                    "path": "ga_ls8c_ones_3-0-0_090084_2016-01-21_final_thumbnail.jpg"
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

        # p.done() will validate the dataset and write it to the destination atomically.
        dataset_id, metadata_path = p.done()

    assert dataset_id is not None
    for f in out.rglob("*"):
        print(str(f.name))
    assert_file_structure(
        out,
        {
            "quaternarius": {
                "2019": {
                    "07": {
                        "04": {
                            # Set a dataset version to get rid of 'beta' label.
                            "quaternarius_beta_x_2019-07-04_user.odc-metadata.yaml": "",
                            "quaternarius_beta_x_2019-07-04_user.proc-info.yaml": "",
                            "quaternarius_beta_x_2019-07-04_user_blue.tif": "",
                            "quaternarius_beta_x_2019-07-04_user.sha1": "",
                        }
                    }
                }
            }
        },
    )
