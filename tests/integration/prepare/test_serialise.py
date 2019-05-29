import operator
from pathlib import Path
from typing import Dict, Union, Sequence

import pytest
from click.testing import CliRunner, Result

from eodatasets.prepare import ls_usgs_l1_prepare, validate, serialise
from .common import check_prepare_outputs, lists_to_tuples, assert_same



def test_valid_document_works(tmp_path: Path, example_metadata: Dict):
    generated_doc = example_metadata

    # Do a serialisation roundtrip and check that it's still identical.
    reserialised_doc = lists_to_tuples(
        serialise.to_doc(serialise.from_doc(generated_doc))
    )
    assert_same(generated_doc, reserialised_doc)

    assert serialise.from_doc(example_metadata) == serialise.from_doc(generated_doc)

