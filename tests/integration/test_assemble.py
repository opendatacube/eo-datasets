from datetime import datetime
from pathlib import Path

import numpy

from eodatasets2.assemble import DatasetAssembler
from eodatasets2.images import GridSpec
from eodatasets2.model import DatasetDoc
from tests import assert_file_structure
from tests.integration.common import assert_same_as_file


def test_minimal_dea_package(l1_ls8_dataset: DatasetDoc, tmp_path: Path):
    out = tmp_path / "test-dataset"
    with DatasetAssembler(out, naming_conventions="dea") as p:
        p.add_source_dataset(l1_ls8_dataset, auto_inherit_properties=True)

        # It's a GA product called "ones".
        p.properties.producer = "ga.gov.au"
        p.properties["odc:product_family"] = "ones"

        processing_time = datetime.utcnow()
        p.properties.processed = processing_time

        # GA's collection 3 processes USGS Collection 1
        p.properties["odc:dataset_version"] = f"3.0.0"

        # TODO: maturity, where to load from?
        p.properties["dea:dataset_maturity"] = "final"
        p.properties["dea:processing_level"] = "level-2"

        p.write_measurement_numpy(
            "blue",
            numpy.ones((60, 60), numpy.int16),
            # Same pixel coords as our input level 1
            GridSpec.from_dataset(l1_ls8_dataset),
            nodata=-999,
        )

        p.write_thumbnail("blue", "blue", "blue")

        dataset_id = p.done()

    metadata_name = "ga_ls8c_ones_3-0-0_090084_2016-01-21_final.odc-metadata.yaml"
    assert_file_structure(
        tmp_path,
        {
            "test-dataset": {
                metadata_name: "",
                "ga_ls8c_ones_3-0-0_090084_2016-01-21_final_blue.tif": "",
                "ga_ls8c_ones_3-0-0_090084_2016-01-21_final_thumbnail.jpg": "",
                "ga_ls8c_ones_3-0-0_090084_2016-01-21_final.proc-info.yaml": "",
                "ga_ls8c_ones_3-0-0_090084_2016-01-21_final.sha1": "",
            }
        },
    )

    assert_same_as_file(
        {
            "$schema": "https://schemas.opendatacube.org/dataset",
            "id": dataset_id,
            "product": {
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
                "blue": {"path": "ga_ls8c_ones_3-0-0_090084_2016-01-21_final_blue.tif"}
            },
            "properties": {
                "datetime": datetime(2016, 1, 21, 23, 50, 23, 54435),
                "dea:dataset_maturity": "final",
                "dea:processing_level": "level-2",
                "eo:cloud_cover": 93.22,
                "eo:gsd": 30.0,
                "eo:instrument": "OLI_TIRS",
                "eo:platform": "landsat-8",
                "eo:sun_azimuth": 74.007_443_8,
                "eo:sun_elevation": 55.486_483,
                "landsat:collection_category": "T1",
                "landsat:collection_number": 1,
                "landsat:landsat_product_id": "LC08_L1TP_090084_20160121_20170405_01_T1",
                "landsat:landsat_scene_id": "LC80900842016021LGN02",
                "landsat:wrs_path": 90,
                "landsat:wrs_row": 84,
                "odc:dataset_version": "3.0.0",
                "odc:processing_datetime": processing_time,
                "odc:producer": "ga.gov.au",
                "odc:product_family": "ones",
                "odc:reference_code": "090084",
            },
            "lineage": {"level1": ["a780754e-a884-58a7-9ac0-df518a67f59d"]},
        },
        generated_file=tmp_path / "test-dataset" / metadata_name,
    )
