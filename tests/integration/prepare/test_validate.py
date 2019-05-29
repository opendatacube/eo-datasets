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


def test_valid_ls8(tmp_path: Path, example_metadata: Dict):
    _assert_valid(example_metadata, tmp_path)


def test_missing_field(tmp_path: Path, example_metadata: Dict):
    del example_metadata["id"]
    _assert_invalid(
        dict(structure="'id' is a required property"), example_metadata, tmp_path
    )


def test_invalid_ls8_schema(tmp_path: Path, example_metadata: Dict):
    del example_metadata["$schema"]
    _assert_invalid(["no_schema"], example_metadata, tmp_path)


def test_allow_optional_geo(tmp_path: Path, example_metadata: Dict):
    # A doc can omit all geo fields and be valid.
    del example_metadata["crs"]
    del example_metadata["geometry"]

    example_metadata["grids"] = {}
    _assert_valid(example_metadata, tmp_path)

    del example_metadata["grids"]
    _assert_valid(example_metadata, tmp_path)


def test_missing_geo_fields(tmp_path: Path, example_metadata: Dict):
    del example_metadata["crs"]
    _assert_invalid(["incomplete_crs"], example_metadata, tmp_path)


def _assert_valid(example_metadata, tmp_path):
    md_path = tmp_path / "test_dataset.yaml"
    serialise.dump_yaml(md_path, example_metadata)
    run_validate(md_path, expect_success=True)


def _assert_invalid(
    expect_messages: Union[Sequence[str], Dict[str, str]], doc: Dict, tmp_path: Path
):
    md_path = tmp_path / "test_dataset.yaml"
    serialise.dump_yaml(md_path, doc)
    res = run_validate("-q", md_path, expect_success=False)
    assert res.exit_code == 1

    def _read_message(line: str):
        severity, code, *message = line.split("\t")
        return code, "\t".join(message)

    got_messages = dict(_read_message(line) for line in res.stdout.split("\n") if line)

    if isinstance(expect_messages, dict):
        # Did we output the expected set of error codes?
        assert sorted(got_messages.keys()) == sorted(expect_messages.keys())

        for code, expected_message in expect_messages.items():
            assert expected_message in got_messages[code], (
                f"Expected {code!r} to say {expected_message!r}\n"
                f"\t got {got_messages[code]}"
            )
    else:
        assert sorted(got_messages.keys()) == sorted(expect_messages)


def run_validate(*args: Union[str, Path], expect_success=True) -> Result:
    """eod-validate as a command-line command"""
    __tracebackhide__ = True

    res: Result = CliRunner(mix_stderr=False).invoke(
        validate.run, [str(a) for a in args], catch_exceptions=False
    )
    if expect_success:
        assert res.exit_code == 0, res.output

    return res
