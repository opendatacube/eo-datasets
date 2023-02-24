from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

from affine import Affine
from shapely.geometry import shape

from eodatasets3 import DatasetDoc, Eo3Dict
from eodatasets3.model import GridDoc, MeasurementDoc, ProductDoc
from eodatasets3.prepare.esri_land_cover_prepare import as_eo3

from tests.common import assert_expected_eo3

ESRI_ANTIMERIDIAN_OVERLAP_TIF: Path = (
    Path(__file__).parent.parent / "data/esri-land-cover/60K_20200101-20210101.tif"
)


def test_esri_prepare():
    tif_url = ESRI_ANTIMERIDIAN_OVERLAP_TIF.resolve().as_uri()
    dataset_doc = as_eo3(tif_url)

    # Varies
    dataset_doc.properties.pop("odc:processing_datetime")

    assert_expected_eo3(
        DatasetDoc(
            id=UUID("411f3641-bc4a-501d-99e5-6c5c47379715"),
            product=ProductDoc("esri_land_cover"),
            accessories={},
            lineage={},
            label="esri_land_cover_60K_2020-01-01",
            locations=[
                tif_url,
            ],
            properties=Eo3Dict(
                {
                    "datetime": datetime(2020, 1, 1, 0, 0, tzinfo=timezone.utc),
                    "dtr:end_datetime": datetime(2021, 1, 1, 0, 0, tzinfo=timezone.utc),
                    "dtr:start_datetime": datetime(
                        2020, 1, 1, 0, 0, tzinfo=timezone.utc
                    ),
                    "odc:product": "esri_land_cover",
                    "odc:region_code": "60K",
                }
            ),
            crs="epsg:32760",
            measurements=dict(
                classification=MeasurementDoc(
                    path="60K_20200101-20210101.tif",
                )
            ),
            geometry=shape(
                {
                    "coordinates": (
                        (
                            (841600.2521048393, 7864419.112077935),
                            (841600.2521048393, 8221119.112077935),
                            (476100.25210483925, 8221119.112077935),
                            (476100.25210483925, 7864419.112077935),
                            (841600.2521048393, 7864419.112077935),
                        ),
                    ),
                    "type": "Polygon",
                }
            ),
            grids={
                "default": GridDoc(
                    (128, 128),
                    Affine(
                        2855.46875,
                        0.0,
                        476100.25210483925,
                        0.0,
                        -2786.71875,
                        8221119.112077935,
                    ),
                )
            },
        ),
        dataset_doc,
    )
