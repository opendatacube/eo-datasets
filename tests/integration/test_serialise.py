from pathlib import Path
from typing import Dict

from eodatasets3 import serialise
from .common import assert_same, dump_roundtrip


def test_valid_document_works(tmp_path: Path, example_metadata: Dict):
    generated_doc = dump_roundtrip(example_metadata)

    # Do a serialisation roundtrip and check that it's still identical.
    reserialised_doc = dump_roundtrip(
        serialise.to_doc(serialise.from_doc(generated_doc))
    )

    assert_same(generated_doc, reserialised_doc)

    assert serialise.from_doc(generated_doc) == serialise.from_doc(reserialised_doc)
