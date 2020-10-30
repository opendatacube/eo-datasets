import json
import shutil
from pathlib import Path

import pytest
from eodatasets3 import serialise
from eodatasets3.scripts import tostac
from tests.common import run_prepare_cli, assert_same

TO_STAC_DATA: Path = Path(__file__).parent.joinpath("data/tostac")
ODC_METADATA_FILE: str = "ga_ls8c_ard_3-1-0_088080_2020-05-25_final.odc-metadata.yaml"
STAC_TEMPLATE_FILE: str = "ga_ls_ard_3_stac_item.json"
STAC_EXPECTED_FILE: str = (
    "ga_ls8c_ard_3-1-0_088080_2020-05-25_final.stac-item_expected.json"
)


def test_tostac(input_doc_folder: Path):
    input_metadata_path = input_doc_folder.joinpath(ODC_METADATA_FILE)
    assert input_metadata_path.exists()

    run_tostac(input_metadata_path)

    name = input_metadata_path.stem.replace(".odc-metadata", "")
    actual_stac_path = input_metadata_path.with_name(f"{name}.stac-item.json")
    assert actual_stac_path.exists()

    expected_stac_path = input_doc_folder.joinpath(STAC_EXPECTED_FILE)
    assert expected_stac_path.exists()

    actual_doc = json.load(actual_stac_path.open())
    expected_doc = json.load(expected_stac_path.open())
    assert_same(expected_doc, actual_doc)


def test_add_property(input_doc_folder: Path):
    input_metadata_path = input_doc_folder.joinpath(ODC_METADATA_FILE)
    assert input_metadata_path.exists()

    input_doc = serialise.load_yaml(input_metadata_path)
    input_doc["properties"]["test"] = "testvalue"

    serialise.dump_yaml(input_metadata_path, input_doc)
    assert input_metadata_path.exists()

    run_tostac(input_metadata_path)

    name = input_metadata_path.stem.replace(".odc-metadata", "")
    actual_stac_path = input_metadata_path.with_name(f"{name}.stac-item.json")
    assert actual_stac_path.exists()

    actual_doc = json.load(actual_stac_path.open())
    assert actual_doc["properties"]["test"] == input_doc["properties"]["test"]


def test_invalid_crs(input_doc_folder: Path):
    input_metadata_path = input_doc_folder.joinpath(ODC_METADATA_FILE)
    assert input_metadata_path.exists()

    input_doc = serialise.load_yaml(input_metadata_path)
    del input_doc["crs"]

    serialise.dump_yaml(input_metadata_path, input_doc)
    assert input_metadata_path.exists()

    with pytest.raises(RuntimeError) as exp:
        run_tostac(input_metadata_path)
    assert (
        str(exp.value) == "Expect string or any object with "
        "`.to_epsg()` or `.to_wkt()` method"
    )


def run_tostac(input_metadata_path: Path):
    run_prepare_cli(
        tostac.run,
        "-u",
        "http://dea-public-data-dev.s3-ap-southeast-2.amazonaws.com/"
        "analysis-ready-data/ga_ls8c_ard_3/088/080/2020/05/25/",
        "-e",
        "https://explorer.dev.dea.ga.gov.au/",
        "--validate",
        input_metadata_path,
    )


@pytest.fixture
def input_doc_folder(tmp_path: Path) -> Path:
    tmp_input_path = tmp_path / TO_STAC_DATA.name
    if TO_STAC_DATA.is_file():
        shutil.copy(TO_STAC_DATA, tmp_input_path)
    else:
        shutil.copytree(TO_STAC_DATA, tmp_input_path)
    return tmp_input_path
