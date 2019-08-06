# pylint: disable=E501
from functools import partial
from pathlib import Path
from pprint import pformat

from ruamel import yaml
from deepdiff import DeepDiff

from eodatasets3.prepare import noaa_c_c_prwtreatm_1_prepare
from tests.integration.common import run_prepare_cli

NCEP_PR_WTR_FILE: Path = Path(
    __file__
).parent.parent / "data" / "noaa_c_c_prwtreatm_1/pr_wtr.eatm.2018.test.nc"

_diff = partial(DeepDiff, significant_digits=6)


def test_prepare_ncep_reanalysis1_pr_wtr(tmpdir):
    output_path = Path(tmpdir)
    expected_metadata_path = output_path / "pr_wtr.eatm.2018.test.ga-md.yaml"

    expected_doc = [
        {
            "crs": "epsg:4236",
            "datetime": "2018-01-01T00:00:00+00:00",
            "geometry": {
                "coordinates": [
                    [
                        [-1.25, 91.25],
                        [-1.25, -91.25],
                        [358.75, -91.25],
                        [358.75, 91.25],
                        [-1.25, 91.25],
                    ]
                ],
                "type": "Polygon",
            },
            "grids": {
                "default": {
                    "shape": [73, 144],
                    "transform": [2.5, 0.0, -1.25, 0.0, -2.5, 91.25, 0.0, 0.0, 1.0],
                }
            },
            "id": "fb3afcb0-4301-57c5-8455-35a64e3b0c53",
            "lineage": {},
            "measurements": {
                "water_vapour": {
                    "band": 1,
                    "layer": "pr_wtr",
                    "path": "pr_wtr.eatm.2018.test.nc",
                }
            },
            "product": {
                "href": "https://collections.dea.ga.gov.au/noaa_c_c_prwtreatm_1"
            },
            "properties": {
                "item:providers": [
                    {
                        "name": "NOAA/OAR/ESRL PSD",
                        "roles": ["producer"],
                        "url": "https://www.esrl.noaa.gov/psd/data/gridded/data.ncep.reanalysis.derived.surface.html",
                    }
                ],
                "odc:creation_datetime": "2019-05-15T07:29:04.948999+00:00",
                "odc:file_format": "NetCDF",
            },
        },
        {
            "crs": "epsg:4236",
            "datetime": "2018-01-01T06:00:00+00:00",
            "geometry": {
                "coordinates": [
                    [
                        [-1.25, 91.25],
                        [-1.25, -91.25],
                        [358.75, -91.25],
                        [358.75, 91.25],
                        [-1.25, 91.25],
                    ]
                ],
                "type": "Polygon",
            },
            "grids": {
                "default": {
                    "shape": [73, 144],
                    "transform": [2.5, 0.0, -1.25, 0.0, -2.5, 91.25, 0.0, 0.0, 1.0],
                }
            },
            "id": "47d52e5b-b6aa-5cb6-888d-06c8e4bfa756",
            "lineage": {},
            "measurements": {
                "water_vapour": {
                    "band": 2,
                    "layer": "pr_wtr",
                    "path": "pr_wtr.eatm.2018.test.nc",
                }
            },
            "product": {
                "href": "https://collections.dea.ga.gov.au/noaa_c_c_prwtreatm_1"
            },
            "properties": {
                "item:providers": [
                    {
                        "name": "NOAA/OAR/ESRL PSD",
                        "roles": ["producer"],
                        "url": "https://www.esrl.noaa.gov/psd/data/gridded/data.ncep.reanalysis.derived.surface.html",
                    }
                ],
                "odc:creation_datetime": "2019-05-15T07:34:18.424782+00:00",
                "odc:file_format": "NetCDF",
            },
        },
    ]

    run_prepare_cli(
        noaa_c_c_prwtreatm_1_prepare.main,
        "--output",
        str(output_path),
        str(NCEP_PR_WTR_FILE),
    )

    assert expected_metadata_path.exists()
    docs = list(yaml.safe_load_all(expected_metadata_path.open()))

    for idx in range(len(expected_doc)):
        doc_diff = _diff(
            expected_doc[idx],
            docs[idx],
            exclude_paths="root['properties']['odc:creation_datetime']",
        )
        assert doc_diff == {}, pformat(doc_diff)
