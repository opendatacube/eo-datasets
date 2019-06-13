from pathlib import Path
from typing import Dict

import rapidjson

from eodatasets.prepare import serialise
from .common import assert_same


def test_valid_document_works(tmp_path: Path, example_metadata: Dict):
    generated_doc = _dump_roundtrip(example_metadata)

    # Do a serialisation roundtrip and check that it's still identical.
    reserialised_doc = _dump_roundtrip(
        serialise.to_doc(serialise.from_doc(generated_doc))
    )

    assert_same(generated_doc, reserialised_doc)

    assert serialise.from_doc(generated_doc) == serialise.from_doc(reserialised_doc)


def _dump_roundtrip(generated_doc):
    """Do a dump/load to normalise all doc-neutral dict/date/tuple/list types.

    The in-memory choice of dict/etc subclasses shouldn't matter, as long as the doc
    is identical once produced.
    """
    return rapidjson.loads(
        rapidjson.dumps(generated_doc, datetime_mode=True, uuid_mode=True)
    )
