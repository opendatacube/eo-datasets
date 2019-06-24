from functools import partial
from pathlib import Path
from pprint import pformat, pprint
from typing import Dict
import rapidjson
import yaml
from boltons.iterutils import remap
from click.testing import CliRunner, Result
from deepdiff import DeepDiff

diff = partial(DeepDiff, significant_digits=6)


def check_prepare_outputs(
    invoke_script, run_args, expected_doc, expected_metadata_path, normalise_tuples=True
):
    __tracebackhide__ = True
    run_prepare_cli(invoke_script, *run_args)

    assert expected_metadata_path.exists()
    generated_doc = yaml.safe_load(expected_metadata_path.open())
    if normalise_tuples:
        generated_doc = lists_to_tuples(generated_doc)

    assert_same(expected_doc, generated_doc)


def assert_same(expected_doc: Dict, generated_doc: Dict):
    __tracebackhide__ = True
    doc_diffs = diff(expected_doc, generated_doc)
    assert doc_diffs == {}, pformat(doc_diffs)


def assert_same_as_file(expected_doc: Dict, generated_file: Path, ignore_fields=()):
    __tracebackhide__ = True

    assert generated_file.exists(), f"Expected file to exist {generated_file.name}"

    generated_doc = yaml.load(generated_file.open("r"))
    for field in ignore_fields:
        del generated_doc[field]

    pprint(generated_doc)
    doc_diffs = diff(dump_roundtrip(expected_doc), dump_roundtrip(generated_doc))
    assert doc_diffs == {}, pformat(doc_diffs)


def lists_to_tuples(doc):
    """Recursively change any embedded lists into tuples"""
    return remap(doc, visit=lambda p, k, v: (k, tuple(v) if type(v) == list else v))


def run_prepare_cli(invoke_script, *args, expect_success=True) -> Result:
    """Run the prepare script as a command-line command"""
    __tracebackhide__ = True

    res: Result = CliRunner().invoke(invoke_script, args, catch_exceptions=False)
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
