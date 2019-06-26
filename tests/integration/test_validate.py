import operator
from pathlib import Path
from textwrap import dedent
from typing import Dict, Union

from click.testing import CliRunner, Result

from eodatasets2 import serialise, validate


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
    """ If you have one gis field, you should have all of them"""
    del example_metadata["crs"]
    _assert_invalid_codes(example_metadata, tmp_path, "incomplete_crs")


def test_warn_bad_formatting(tmp_path: Path, example_metadata: Dict):
    """ A warning if fields aren't formatted in standard manner."""
    example_metadata["properties"]["eo:platform"] = example_metadata["properties"][
        "eo:platform"
    ].upper()
    _assert_invalid_codes(
        example_metadata, tmp_path, "property_formatting", warnings_are_errors=True
    )


def test_missing_grid_def(tmp_path: Path, example_metadata: Dict):
    """A Measurement refers to a grid that doesn't exist"""
    a_measurement, *_ = list(example_metadata["measurements"])
    example_metadata["measurements"][a_measurement]["grid"] = "unknown_grid"
    _assert_invalid_codes(example_metadata, tmp_path, "invalid_grid_ref")


def test_invalid_shape(tmp_path: Path, example_metadata: Dict):
    """the geometry must be a valid shape"""

    # Points are in an invalid order.
    example_metadata["geometry"] = {
        "coordinates": (
            (
                (770_115.0, -2_768_985.0),
                (525_285.0, -2_981_715.0),
                (770_115.0, -2_981_715.0),
                (525_285.0, -2_768_985.0),
                (770_115.0, -2_768_985.0),
            ),
        ),
        "type": "Polygon",
    }

    messages = _assert_invalid_codes(example_metadata, tmp_path, "invalid_geometry")
    assert "not a valid shape" in messages["invalid_geometry"]


def test_crs_as_wkt(tmp_path: Path, example_metadata: Dict):
    """A CRS should be in epsg form if an EPSG exists, not WKT"""
    example_metadata["crs"] = dedent(
        """PROJCS["WGS 84 / UTM zone 55N",
    GEOGCS["WGS 84",
        DATUM["WGS_1984",
            SPHEROID["WGS 84",6378137,298.257223563,
                AUTHORITY["EPSG","7030"]],
            AUTHORITY["EPSG","6326"]],
        PRIMEM["Greenwich",0,
            AUTHORITY["EPSG","8901"]],
        UNIT["degree",0.01745329251994328,
            AUTHORITY["EPSG","9122"]],
        AUTHORITY["EPSG","4326"]],
    UNIT["metre",1,
        AUTHORITY["EPSG","9001"]],
    PROJECTION["Transverse_Mercator"],
    PARAMETER["latitude_of_origin",0],
    PARAMETER["central_meridian",147],
    PARAMETER["scale_factor",0.9996],
    PARAMETER["false_easting",500000],
    PARAMETER["false_northing",0],
    AUTHORITY["EPSG","32655"],
    AXIS["Easting",EAST],
    AXIS["Northing",NORTH]]
    """
    )

    # It's valid, but a warning is produced.
    warnings = _assert_valid(example_metadata, tmp_path, expect_no_warnings=False)
    assert "non_epsg" in warnings
    # Suggests an alternative
    assert "change CRS to 'epsg:32655'" in warnings["non_epsg"]

    # .. and it should fail when warnings are treated as errors.
    _assert_invalid(example_metadata, tmp_path, warnings_are_errors=True)


def _assert_valid(example_metadata, tmp_path, expect_no_warnings=True):
    __tracebackhide__ = operator.methodcaller("errisinstance", AssertionError)
    md_path = tmp_path / "test_dataset.yaml"
    serialise.dump_yaml(md_path, example_metadata)
    res = run_validate("-q", md_path, expect_success=True)
    messages = _read_messages(res)
    if expect_no_warnings:
        assert messages == {}
    return messages


def _assert_invalid_codes(
    doc: Dict, tmp_path: Path, *expected_error_codes, warnings_are_errors=False
):
    messages = _assert_invalid(doc, tmp_path, warnings_are_errors=warnings_are_errors)
    assert sorted(expected_error_codes) == sorted(messages.keys())
    return messages


def _assert_invalid(doc: Dict, tmp_path: Path, warnings_are_errors=False):
    __tracebackhide__ = operator.methodcaller("errisinstance", AssertionError)
    md_path = tmp_path / "test_dataset.yaml"
    serialise.dump_yaml(md_path, doc)

    args = ("-q",)
    if warnings_are_errors:
        args += ("-W",)

    res = run_validate(*args, md_path, expect_success=False)
    assert res.exit_code != 0, "Expected validation to fail"
    assert (
        res.exit_code == 1
    ), f"Expected error code to be 1 for 1 document failure.\n{res.output}"

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
