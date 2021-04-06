from pathlib import Path
from pprint import pformat
from typing import Dict, Iterable

import rapidjson
from click.testing import CliRunner, Result
from deepdiff import DeepDiff
from ruamel import yaml


def check_prepare_outputs(
    invoke_script, run_args, expected_doc, expected_metadata_path
):
    __tracebackhide__ = True
    run_prepare_cli(invoke_script, *run_args)

    assert expected_metadata_path.exists()
    assert_same_as_file(expected_doc, expected_metadata_path)


def assert_same(expected_doc: Dict, generated_doc: Dict):
    """
    Assert two documents are the same, ignoring trivial float differences
    """
    __tracebackhide__ = True
    doc_diffs = DeepDiff(expected_doc, generated_doc, significant_digits=6)
    assert doc_diffs == {}, "\n".join(format_doc_diffs(expected_doc, generated_doc))


def assert_same_as_file(expected_doc: Dict, generated_file: Path, ignore_fields=()):
    """Assert a file contains the given document content (after normalisation etc)"""
    __tracebackhide__ = True

    assert generated_file.exists(), f"Expected file to exist {generated_file.name}"

    with generated_file.open("r") as f:
        generated_doc = yaml.safe_load(f)
    for field in ignore_fields:
        del generated_doc[field]

    expected_doc = dump_roundtrip(expected_doc)
    generated_doc = dump_roundtrip(generated_doc)

    assert_same(expected_doc, generated_doc)


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
    if "values_changed" not in doc_diffs:
        # Shouldn't happen?
        return [pformat(doc_diffs)]

    for offset, change in doc_diffs["values_changed"].items():
        if offset.startswith("root"):
            offset: str = offset[len("root") :]
        out.extend(
            (
                f"   {offset}: ",
                f'          {change["old_value"]!r}',
                f'       != {change["new_value"]!r}',
            )
        )
    return out


def dump_roundtrip(generated_doc):
    """Do a dump/load to normalise all doc-neutral dict/date/tuple/list types.

    The in-memory choice of dict/etc subclasses shouldn't matter, as long as the doc
    is identical once produced.
    """
    return rapidjson.loads(
        rapidjson.dumps(generated_doc, datetime_mode=True, uuid_mode=True)
    )
