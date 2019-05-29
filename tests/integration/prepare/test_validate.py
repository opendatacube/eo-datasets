import operator
from pathlib import Path
from typing import Dict, Union, Sequence

import pytest
from click.testing import CliRunner, Result

from eodatasets.prepare import ls_usgs_l1_prepare, validate, serialise
from .common import check_prepare_outputs


@pytest.fixture(params=("ls5", "ls7", "ls8"))
def example_metadata(
    request,
    l1_ls5_tarball_md_expected: Dict,
    l1_ls7_tarball_md_expected: Dict,
    l1_ls8_folder_md_expected: Dict,
):
    which = request.param
    if which == "ls5":
        return l1_ls5_tarball_md_expected
    elif which == "ls7":
        return l1_ls7_tarball_md_expected
    elif which == "ls8":
        return l1_ls8_folder_md_expected
    assert False


def test_valid_document_works(tmp_path: Path, example_metadata: Dict):
    _assert_valid(example_metadata, tmp_path)


def test_missing_field(tmp_path: Path, example_metadata: Dict):
    del example_metadata["id"]
    messages = _assert_invalid(example_metadata, tmp_path)
    assert list(messages.keys()) == ["structure"]
    assert "'id' is a required property" in messages["structure"]


def test_invalid_ls8_schema(tmp_path: Path, example_metadata: Dict):
    del example_metadata["$schema"]
    _assert_invalid_codes(example_metadata, tmp_path, "no_schema")


def test_allow_optional_geo(tmp_path: Path, example_metadata: Dict):
    # A doc can omit all geo fields and be valid.
    del example_metadata["crs"]
    del example_metadata["geometry"]

    for m in example_metadata["measurements"].values():
        if "grid" in m:
            del m["grid"]

    example_metadata["grids"] = {}
    _assert_valid(example_metadata, tmp_path)

    del example_metadata["grids"]
    _assert_valid(example_metadata, tmp_path)


def test_missing_geo_fields(tmp_path: Path, example_metadata: Dict):
    del example_metadata["crs"]
    _assert_invalid_codes(example_metadata, tmp_path, "incomplete_crs")


def _assert_valid(example_metadata, tmp_path, expect_no_warnings=True):
    __tracebackhide__ = operator.methodcaller("errisinstance", AssertionError)
    md_path = tmp_path / "test_dataset.yaml"
    serialise.dump_yaml(md_path, example_metadata)
    res = run_validate("-q", md_path, expect_success=True)
    messages = _read_messages(res)
    if expect_no_warnings:
        assert messages == {}
    return messages


def _assert_invalid_codes(doc: Dict, tmp_path: Path, *expected_error_codes):
    messages = _assert_invalid(doc, tmp_path)
    assert sorted(expected_error_codes) == sorted(messages.keys())


def _assert_invalid(doc: Dict, tmp_path: Path):
    __tracebackhide__ = operator.methodcaller("errisinstance", AssertionError)
    md_path = tmp_path / "test_dataset.yaml"
    serialise.dump_yaml(md_path, doc)
    res = run_validate("-q", md_path, expect_success=False)
    # One failed document
    assert res.exit_code == 1

    return _read_messages(res)


def _read_messages(res) -> Dict[str, str]:
    """Read the messages/warnings for validation tool stdout.

    Returned as a dict of error_code -> human_message
    """

    def _read_message(line: str):
        severity, code, *message = line.split("\t")
        return code, "\t".join(message)

    return dict(_read_message(line) for line in res.stdout.split("\n") if line)


def run_validate(*args: Union[str, Path], expect_success=True) -> Result:
    """eod-validate as a command-line command"""
    __tracebackhide__ = operator.methodcaller("errisinstance", AssertionError)

    res: Result = CliRunner(mix_stderr=False).invoke(
        validate.run, [str(a) for a in args], catch_exceptions=False
    )
    if expect_success:
        assert res.exit_code == 0, res.output

    return res
