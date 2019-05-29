from functools import partial
from pprint import pformat, pprint
from uuid import UUID

import yaml
from boltons.iterutils import remap
from click.testing import CliRunner, Result
from deepdiff import DeepDiff

from eodatasets.prepare import serialise

diff = partial(DeepDiff, significant_digits=6)


def check_prepare_outputs(
    invoke_script, run_args, expected_doc, expected_metadata_path
):
    __tracebackhide__ = True
    run_prepare_cli(invoke_script, *run_args)

    assert expected_metadata_path.exists()
    generated_doc = lists_to_tuples(yaml.safe_load(expected_metadata_path.open()))

    assert_same(expected_doc, generated_doc)


def assert_same(expected_doc, generated_doc):
    __tracebackhide__ = True
    doc_diffs = diff(expected_doc, generated_doc)
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
