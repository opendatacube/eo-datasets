from functools import partial
from pathlib import Path
from pprint import pformat, pprint
from typing import Dict

import rapidjson
from click.testing import CliRunner, Result
from deepdiff import DeepDiff
from ruamel import yaml

diff = partial(DeepDiff, significant_digits=6)


def check_prepare_outputs(
    invoke_script, run_args, expected_doc, expected_metadata_path
):
    __tracebackhide__ = True
    run_prepare_cli(invoke_script, *run_args)

    assert expected_metadata_path.exists()
    assert_same_as_file(expected_doc, expected_metadata_path)


def assert_same(expected_doc: Dict, generated_doc: Dict):
    __tracebackhide__ = True
    doc_diffs = diff(expected_doc, generated_doc)
    assert doc_diffs == {}, pformat(doc_diffs)


def assert_same_as_file(expected_doc: Dict, generated_file: Path, ignore_fields=()):
    __tracebackhide__ = True

    assert generated_file.exists(), f"Expected file to exist {generated_file.name}"

    with generated_file.open("r") as f:
        generated_doc = yaml.safe_load(f)
    for field in ignore_fields:
        del generated_doc[field]

    doc_diffs = diff(dump_roundtrip(expected_doc), dump_roundtrip(generated_doc))
    try:
        assert doc_diffs == {}, pformat(doc_diffs)
    except AssertionError:
        pprint(generated_doc)
        raise


def run_prepare_cli(invoke_script, *args, expect_success=True) -> Result:
    """Run the prepare script as a command-line command"""
    __tracebackhide__ = True

    res: Result = CliRunner().invoke(
        invoke_script, [str(a) for a in args], catch_exceptions=False
    )
    if expect_success:
        assert res.exit_code == 0, res.output

    return res


def dump_roundtrip(generated_doc):
    """Do a dump/load to normalise all doc-neutral dict/date/tuple/list types.

    The in-memory choice of dict/etc subclasses shouldn't matter, as long as the doc
    is identical once produced.
    """
    return rapidjson.loads(
        rapidjson.dumps(generated_doc, datetime_mode=True, uuid_mode=True)
    )
