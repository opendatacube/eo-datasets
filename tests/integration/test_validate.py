import operator
from pathlib import Path
from textwrap import dedent
from typing import Dict, Union, Mapping, Sequence, Optional

import pytest
from click.testing import CliRunner, Result
from eodatasets3 import serialise, validate

# Either a dict or a path to a document
Doc = Union[Dict, Path]


class ValidateRunner:
    """
    Run the eo3 validator command-line interface and assert the results.
    """

    def __init__(self, tmp_path: Path) -> None:
        self.tmp_path = tmp_path
        self.quiet = False
        self.warnings_are_errors = False
        self.record_informational_messages = False
        self.thorough: bool = False

        self.result: Optional[Result] = None

    def assert_valid(self, *docs: Doc, expect_no_messages=True):
        __tracebackhide__ = operator.methodcaller("errisinstance", AssertionError)
        self.run_validate(docs)
        if expect_no_messages and self.messages:
            raise AssertionError(
                "\n".join(f"{k}: {v}" for k, v in self.messages.items())
            )

        assert (
            self.result.exit_code == 0
        ), f"Expected validation to succeed. Output:\n{self.result.output}"

    def assert_invalid(self, *docs: Doc, codes: Sequence[str] = None):
        __tracebackhide__ = operator.methodcaller("errisinstance", AssertionError)
        self.run_validate(docs)
        assert (
            self.result.exit_code != 0
        ), f"Expected validation to fail.\n{self.result.output}"

        if codes is not None:
            assert sorted(codes) == sorted(self.messages.keys())

    def run_validate(self, docs: Sequence[Doc]):
        __tracebackhide__ = operator.methodcaller("errisinstance", AssertionError)

        args = ()

        if self.quiet:
            args += ("-q",)
        if self.warnings_are_errors:
            args += ("-W",)
        if self.thorough:
            args += ("--thorough",)

        for i, doc in enumerate(docs):
            if isinstance(doc, Mapping):
                md_path = self.tmp_path / f"doc-{i}.yaml"
                serialise.dump_yaml(md_path, doc)
                doc = md_path
            args += (doc,)

        self.result = CliRunner(mix_stderr=False).invoke(
            validate.run, [str(a) for a in args], catch_exceptions=False
        )

    @property
    def messages(self) -> Dict[str, str]:
        """Read the messages/warnings for validation tool stdout.

        Returned as a dict of error_code -> human_message
        """

        def _read_message(line: str):
            severity, code, *message = line.split("\t")
            return code, "\t".join(message)

        return dict(
            _read_message(line)
            for line in self.result.stdout.split("\n")
            if line
            and line.startswith("-")
            and (self.record_informational_messages or not line.startswith("- I\t"))
        )


def test_valid_document_works(eo_validator: ValidateRunner, example_metadata: Dict):
    eo_validator.assert_valid(example_metadata)


def test_missing_field(eo_validator: ValidateRunner, example_metadata: Dict):
    """when a required field (id) is missing, validation should fail"""
    del example_metadata["id"]
    eo_validator.assert_invalid(example_metadata, codes=["structure"])
    assert "'id' is a required property" in eo_validator.messages["structure"]


def test_invalid_ls8_schema(eo_validator: ValidateRunner, example_metadata: Dict):
    """When there's no eo3 $schema defined"""
    del example_metadata["$schema"]
    eo_validator.assert_invalid(example_metadata, codes=("no_schema",))


def test_allow_optional_geo(eo_validator: ValidateRunner, example_metadata: Dict):
    """A doc can omit all geo fields and be valid."""
    del example_metadata["crs"]
    del example_metadata["geometry"]

    for m in example_metadata["measurements"].values():
        if "grid" in m:
            del m["grid"]

    example_metadata["grids"] = {}
    eo_validator.assert_valid(example_metadata)

    del example_metadata["grids"]
    eo_validator.assert_valid(example_metadata)


def test_missing_geo_fields(eo_validator: ValidateRunner, example_metadata: Dict):
    """ If you have one gis field, you should have all of them"""
    del example_metadata["crs"]
    eo_validator.assert_invalid(example_metadata, codes=["incomplete_crs"])


def test_warn_bad_formatting(eo_validator: ValidateRunner, example_metadata: Dict):
    """ A warning if fields aren't formatted in standard manner."""
    example_metadata["properties"]["eo:platform"] = example_metadata["properties"][
        "eo:platform"
    ].upper()
    eo_validator.warnings_are_errors = True
    eo_validator.assert_invalid(example_metadata, codes=["property_formatting"])


def test_missing_grid_def(eo_validator: ValidateRunner, example_metadata: Dict):
    """A Measurement refers to a grid that doesn't exist"""
    a_measurement, *_ = list(example_metadata["measurements"])
    example_metadata["measurements"][a_measurement]["grid"] = "unknown_grid"
    eo_validator.assert_invalid(example_metadata, codes=["invalid_grid_ref"])


def test_invalid_shape(eo_validator: ValidateRunner, example_metadata: Dict):
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

    eo_validator.assert_invalid(example_metadata)
    assert "not a valid shape" in eo_validator.messages["invalid_geometry"]


def test_crs_as_wkt(eo_validator: ValidateRunner, example_metadata: Dict):
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
    eo_validator.assert_valid(example_metadata, expect_no_messages=False)

    assert "non_epsg" in eo_validator.messages
    # Suggests an alternative
    assert "change CRS to 'epsg:32655'" in eo_validator.messages["non_epsg"]

    # .. and it should fail when warnings are treated as errors.
    eo_validator.warnings_are_errors = True
    eo_validator.assert_invalid(example_metadata)


def test_valid_with_product_doc(
    eo_validator: ValidateRunner, l1_ls8_metadata_path: Path
):
    """When a product is specified, it will validate that the measurements match the product"""

    # Document is valid on its own.
    eo_validator.assert_valid(l1_ls8_metadata_path)

    # It contains all measurements in the product, so will be valid when not thorough.
    product = dict(
        name="wrong_product",
        metadata_type="eo3",
        measurements=[dict(name="blue", dtype="uint8", nodata=-999)],
    )
    eo_validator.assert_valid(product, l1_ls8_metadata_path)


def test_measurements_compare_with_product_doc(
    eo_validator: ValidateRunner, l1_ls8_metadata_path: Path
):
    """A thorough validation should check nodata and dtype of measurements"""

    product = dict(
        name="wrong_product",
        metadata_type="eo3",
        measurements=[dict(name="blue", dtype="uint8", nodata=-999)],
    )

    # When thorough, the dtype and nodata are wrong
    eo_validator.thorough = True
    eo_validator.assert_invalid(product, l1_ls8_metadata_path)
    assert eo_validator.messages == {
        "different_dtype": "dtype mismatch for blue: product has dtype 'uint8', dataset has 'uint16'",
        "different_nodata": "nodata mismatch for blue: product -999 != dataset None",
    }


def test_missing_measurement_from_product(
    eo_validator: ValidateRunner, l1_ls8_metadata_path: Path
):
    """Validator should notice a missing measurement from the product def"""
    product = dict(
        name="wrong_product",
        metadata_type="eo3",
        measurements=[dict(name="razzmatazz", dtype="uint8", nodata=-999)],
    )
    eo_validator.assert_invalid(product, l1_ls8_metadata_path)
    assert eo_validator.messages == {
        "missing_measurement": "Product wrong_product expects a measurement 'razzmatazz')"
    }


def test_complains_when_no_product(
    eo_validator: ValidateRunner, l1_ls8_metadata_path: Path
):
    """When a product is specified, it will validate that the measurements match the product"""
    # Thorough checking should fail when there's no product provided
    eo_validator.thorough = True
    eo_validator.assert_invalid(l1_ls8_metadata_path, codes=["no_product"])


def test_is_product():
    """Product documents should be correctly identified as products"""
    product = dict(
        name="minimal_product", metadata_type="eo3", measurements=[dict(name="blue")]
    )
    assert validate.is_product(product)


def test_dataset_is_not_a_product(example_metadata: Dict):
    """
    Datasets should not be identified as products

    (checks all example metadata files)
    """
    assert not validate.is_product(example_metadata)


@pytest.fixture
def eo_validator(tmp_path) -> ValidateRunner:
    return ValidateRunner(tmp_path)
