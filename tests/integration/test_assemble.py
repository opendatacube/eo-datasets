from datetime import datetime
from pathlib import Path
from uuid import UUID

import numpy

from eodatasets3.assemble import DatasetAssembler
from eodatasets3.images import GridSpec
from eodatasets3.model import DatasetDoc
from tests import assert_file_structure
from tests.integration.common import assert_same_as_file


def test_minimal_dea_package(
    l1_ls8_dataset: DatasetDoc, l1_ls8_folder: Path, tmp_path: Path
):
    out = tmp_path

    [blue_geotiff_path] = l1_ls8_folder.rglob("L*_B2.TIF")

    with DatasetAssembler(out, naming_conventions="dea") as p:
        p.add_source_dataset(l1_ls8_dataset, auto_inherit_properties=True)

        # It's a GA product called "ones".
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

        # Write a thumbnail using the given bands as r/g/b.
        p.write_thumbnail("ones", "ones", "blue")

        # p.done() will write the whole dataset into place. (renaming its temp directory)
        dataset_id, dataset_path = p.done()

    assert isinstance(dataset_id, UUID), "Expected a random UUID to be assigned"

    metadata_name = "ga_ls8c_ones_3-0-0_090084_2016-01-21_final.odc-metadata.yaml"
    out = tmp_path / "ga_ls8c_ones_3/090/084/2016/01/21"
    assert out == dataset_path
    assert_file_structure(
        out,
        {
            metadata_name: "",
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
                "odc:reference_code": "090084",
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
        generated_file=out / metadata_name,
    )
