import operator
from pathlib import Path
from typing import Dict, Iterable

import pytest
import rapidjson
from click.testing import CliRunner, Result
from deepdiff import DeepDiff
from deepdiff.model import DiffLevel
from ruamel import yaml
from shapely.geometry.base import BaseGeometry

from eodatasets3 import serialise


def check_prepare_outputs(
    invoke_script,
    run_args,
    expected_doc: Dict,
    expected_metadata_path: Path,
    ignore_fields=(),
):
    __tracebackhide__ = operator.methodcaller("errisinstance", AssertionError)
    run_prepare_cli(invoke_script, *run_args)

    assert expected_metadata_path.exists()
    assert_same_as_file(
        expected_doc,
        expected_metadata_path,
        # We check the geometry below
        ignore_fields=("geometry",) + tuple(ignore_fields),
    )

    # Compare geometry after parsing, rather than comparing the raw dict values.
    produced_dataset = serialise.from_path(expected_metadata_path)
    expected_dataset = serialise.from_doc(expected_doc, skip_validation=True)
    assert_shapes_mostly_equal(
        produced_dataset.geometry, expected_dataset.geometry, 0.00000001
    )


def assert_shapes_mostly_equal(
    shape1: BaseGeometry, shape2: BaseGeometry, threshold: float
):
    __tracebackhide__ = operator.methodcaller("errisinstance", AssertionError)

    # Check area first, as it's a nicer error message when they're wildly different.
    assert shape1.area == pytest.approx(
        shape2.area, abs=threshold
    ), "Shapes have different areas"

    s1 = shape1.simplify(tolerance=threshold)
    s2 = shape2.simplify(tolerance=threshold)
    assert (s1 - s2).area < threshold, f"{s1} is not mostly equal to {s2}"


def assert_same(expected_doc: Dict, generated_doc: Dict):
    """
    Assert two documents are the same, ignoring trivial float differences
    """
    __tracebackhide__ = operator.methodcaller("errisinstance", AssertionError)
    doc_diffs = DeepDiff(expected_doc, generated_doc, significant_digits=6)
    assert doc_diffs == {}, "\n".join(format_doc_diffs(expected_doc, generated_doc))


def assert_same_as_file(expected_doc: Dict, generated_file: Path, ignore_fields=()):
    """Assert a file contains the given document content (after normalisation etc)"""
    __tracebackhide__ = operator.methodcaller("errisinstance", AssertionError)

    assert generated_file.exists(), f"Expected file to exist {generated_file.name}"

    with generated_file.open("r") as f:
        generated_doc = yaml.safe_load(f)

    expected_doc = dict(expected_doc)
    for field in ignore_fields:
        del generated_doc[field]
        if field in expected_doc:
            del expected_doc[field]

    expected_doc = dump_roundtrip(expected_doc)
    generated_doc = dump_roundtrip(generated_doc)
    assert_same(generated_doc, expected_doc)


def run_prepare_cli(invoke_script, *args, expect_success=True) -> Result:
    """Run the prepare script as a command-line command"""
    __tracebackhide__ = True

    res: Result = CliRunner().invoke(
        invoke_script, [str(a) for a in args], catch_exceptions=False
    )
    if expect_success:
        assert res.exit_code == 0, res.output

    return res


def format_doc_diffs(left: Dict, right: Dict) -> Iterable[str]:
    """
    Get a human-readable list of differences in the given documents.

    Returns a list of lines to print.
    """
    doc_diffs = DeepDiff(left, right, significant_digits=6)
    out = []
    if doc_diffs:
        out.append("Documents differ:")
    else:
        out.append("Doc differs in minor float precision:")
        doc_diffs = DeepDiff(left, right)

    def clean_offset(offset: str):
        if offset.startswith("root"):
            return offset[len("root") :]
        return offset

    if "values_changed" in doc_diffs:
        for offset, change in doc_diffs["values_changed"].items():
            out.extend(
                (
                    f"   {clean_offset(offset)}: ",
                    f'          {change["old_value"]!r}',
                    f'       != {change["new_value"]!r}',
                )
            )
    if "dictionary_item_added" in doc_diffs:
        out.append("Added fields:")
        for offset in doc_diffs.tree["dictionary_item_added"].items:
            offset: DiffLevel
            out.append(f"    {clean_offset(offset.path())} = {repr(offset.t2)}")
    if "dictionary_item_removed" in doc_diffs:
        out.append("Removed fields:")
        for offset in doc_diffs.tree["dictionary_item_removed"].items:
            offset: DiffLevel
            out.append(f"    {clean_offset(offset.path())} = {repr(offset.t1)}")
    return out


def dump_roundtrip(generated_doc):
    """Do a dump/load to normalise all doc-neutral dict/date/tuple/list types.

    The in-memory choice of dict/etc subclasses shouldn't matter, as long as the doc
    is identical once produced.
    """
    return rapidjson.loads(
        rapidjson.dumps(generated_doc, datetime_mode=True, uuid_mode=True)
    )
