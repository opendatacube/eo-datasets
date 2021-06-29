from pathlib import Path
from typing import Dict

from eodatasets3 import serialise
from tests.common import dump_roundtrip


def test_stac_to_eo3_serialise(sentinel1_eo3):
    dump_roundtrip(sentinel1_eo3)


def test_valid_document_works(tmp_path: Path, example_metadata: Dict):
    assert_unchanged_after_roundstrip(example_metadata)


def assert_unchanged_after_roundstrip(doc: Dict):
    generated_doc = dump_roundtrip(doc)
    # Do a serialisation roundtrip and check that it's still identical.
    reserialised_doc = dump_roundtrip(
        serialise.to_doc(serialise.from_doc(generated_doc))
    )
    assert generated_doc == reserialised_doc
    assert serialise.from_doc(generated_doc) == serialise.from_doc(reserialised_doc)


def test_location_serialisation(tmp_path: Path, l1_ls8_folder_md_expected: Dict):

    l1_ls8_folder_md_expected["location"] = "s3://test/url/metadata.txt"
    assert_unchanged_after_roundstrip(l1_ls8_folder_md_expected)


def test_location_single_serialisation(tmp_path: Path, l1_ls8_folder_md_expected: Dict):

    # Always serialises a single location as 'location'
    location = "https://some/test/path"

    # Given multiple
    l1_ls8_folder_md_expected["locations"] = [location]

    reserialised_doc = dump_roundtrip(
        serialise.to_doc(serialise.from_doc(l1_ls8_folder_md_expected))
    )

    # We get singular
    assert reserialised_doc["location"] == location
    assert "locations" not in reserialised_doc
