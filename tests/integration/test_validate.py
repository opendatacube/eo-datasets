import operator
from pathlib import Path
from textwrap import dedent
from typing import Dict, Union, Mapping, Sequence, Optional, List

import numpy as np
import pytest
import rasterio
from click.testing import CliRunner, Result
from eodatasets3 import serialise, validate
from eodatasets3.model import DatasetDoc
from rasterio.io import DatasetWriter

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
        was_successful = self.result.exit_code == 0
        assert (
            was_successful
        ), f"Expected validation to succeed. Output:\n{self.result.output}"

        if expect_no_messages and self.messages:
            raise AssertionError(
                "Expected no messages. Got: "
                + "\n".join(f"{k}: {v}" for k, v in self.messages.items())
            )

    def assert_invalid(self, *docs: Doc, codes: Sequence[str] = None):
        __tracebackhide__ = operator.methodcaller("errisinstance", AssertionError)
        self.run_validate(docs)
        assert (
            self.result.exit_code != 0
        ), f"Expected validation to fail.\n{self.result.output}"
        assert self.result.exit_code == 1, "Expected error code 1 for 1 invalid path"

        if codes is not None:
            assert sorted(codes) == sorted(self.messages.keys())

    def run_validate(self, docs: Sequence[Doc], allow_extra_measurements=True):
        __tracebackhide__ = operator.methodcaller("errisinstance", AssertionError)

        args = ()

        if self.quiet:
            args += ("-q",)
        if self.warnings_are_errors:
            args += ("-W",)
        if self.thorough:
            args += ("--thorough",)
        if allow_extra_measurements:
            args += ("--expect-extra-measurements",)

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

        Returned as a dict of error_code -> human_message.

        (Note: this will swallow duplicates when the same error code is output multiple times.)
        """

        def _read_message(line: str):
            severity, code, *message = line.split()
            return code, " ".join(message)

        return dict(
            _read_message(line)
            for line in self.result.stdout.split("\n")
            if line
            and line.startswith("\t")
            and (self.record_informational_messages or not line.startswith("\tI "))
        )


def test_valid_document_works(eo_validator: ValidateRunner, example_metadata: Dict):
    """All of our example metadata files should validate"""
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
        name="our_product",
        metadata_type="eo3",
        measurements=[dict(name="blue", dtype="uint8", nodata=255)],
    )
    eo_validator.assert_valid(product, l1_ls8_metadata_path)


def test_warn_duplicate_measurement_name(
    eo_validator: ValidateRunner,
    l1_ls8_dataset: DatasetDoc,
):
    """When a product is specified, it will validate that names are not repeated between measurements and aliases."""

    # We have the "blue" measurement twice.
    product = dict(
        name="our_product",
        metadata_type="eo3",
        measurements=[
            *_copy_measurements(l1_ls8_dataset),
            dict(name="blue", dtype="uint8", nodata=255),
        ],
    )
    eo_validator.assert_invalid(product)
    assert eo_validator.messages == {
        "duplicate_measurement_name": "Name 'blue' is used more than once in a measurement name or alias."
    }

    # An *alias* clashes with the *name* of a measurement.
    product = dict(
        name="our_product",
        metadata_type="eo3",
        measurements=[
            *_copy_measurements(l1_ls8_dataset),
            dict(
                name="azul",
                aliases=[
                    "icecream",
                    # Clashes with the *name* of a measurement.
                    "blue",
                ],
                dtype="uint8",
                nodata=255,
            ),
        ],
    )
    eo_validator.assert_invalid(product)
    assert "duplicate_measurement_name" in eo_validator.messages


def test_dtype_compare_with_product_doc(
    eo_validator: ValidateRunner, l1_ls8_metadata_path: Path
):
    """'thorough' validation should check the dtype of measurements against the product"""

    product = dict(
        name="wrong_product",
        metadata_type="eo3",
        measurements=[dict(name="blue", dtype="uint8", nodata=None)],
    )

    # When thorough, the dtype and nodata are wrong
    eo_validator.thorough = True
    eo_validator.assert_invalid(product, l1_ls8_metadata_path)
    assert eo_validator.messages == {
        "different_dtype": "blue dtype: product 'uint8' != dataset 'uint16'"
    }


def test_nodata_compare_with_product_doc(
    eo_validator: ValidateRunner, l1_ls8_dataset: DatasetDoc, l1_ls8_metadata_path: Path
):
    """'thorough' validation should check the nodata of measurements against the product"""
    eo_validator.thorough = True
    eo_validator.record_informational_messages = True

    product = dict(
        name="usgs_ls8c_level1_1",
        metadata_type="eo3",
        measurements=[
            *_copy_measurements(l1_ls8_dataset, without=["blue"]),
            # Override blue with our invalid one.
            dict(name="blue", dtype="uint16", nodata=255),
        ],
    )

    # It is only an informational message, not an error, as the product can validly
    #    have a different nodata:
    # 1. When the product nodata is only a fill value for dc.load()
    # 2. When the original images don't specify their no nodata ... Like USGS Level 1 tifs.
    eo_validator.assert_valid(product, l1_ls8_metadata_path, expect_no_messages=False)
    assert eo_validator.messages == {
        "different_nodata": "blue nodata: product 255 != dataset None"
    }


def _copy_measurements(dataset: DatasetDoc, dtype="uint16", without=()) -> List:
    return [
        dict(name=name, dtype=dtype)
        for name, m in dataset.measurements.items()
        if name not in without
    ]


def test_measurements_compare_with_nans(
    eo_validator: ValidateRunner, l1_ls8_dataset: DatasetDoc, l1_ls8_metadata_path: Path
):
    """When dataset and product have NaN nodata values, it should handle them correctly"""
    eo_validator.thorough = True
    eo_validator.record_informational_messages = True
    blue_tif = l1_ls8_metadata_path.parent / l1_ls8_dataset.measurements["blue"].path

    _create_dummy_tif(blue_tif, dtype="float32")
    product = dict(
        name="usgs_ls8c_level1_1",
        metadata_type="eo3",
        measurements=[
            *_copy_measurements(l1_ls8_dataset, without=["blue"]),
            dict(name="blue", dtype="float32", nodata=float("NaN")),
        ],
    )

    # When product is NaN, dataset is None, they don't match
    eo_validator.assert_valid(product, l1_ls8_metadata_path, expect_no_messages=False)
    assert eo_validator.messages == {
        "different_nodata": "blue nodata: product nan != dataset None"
    }

    # When both are NaN, it should be valid
    _create_dummy_tif(blue_tif, nodata=float("NaN"))
    eo_validator.assert_valid(product, l1_ls8_metadata_path, expect_no_messages=True)
    # ODC can also represent NaNs as strings due to json's lack of NaN
    product["measurements"][-1]["nodata"] = "NaN"
    eo_validator.assert_valid(product, l1_ls8_metadata_path, expect_no_messages=True)

    # When product is None, dataset is NaN, they no longer match.
    product["measurements"][-1]["nodata"] = None
    eo_validator.assert_valid(product, l1_ls8_metadata_path, expect_no_messages=False)
    assert eo_validator.messages == {
        "different_nodata": "blue nodata: product None != dataset nan"
    }


def _create_dummy_tif(blue_tif, nodata=None, dtype="float32", **opts):
    with rasterio.open(
        blue_tif,
        "w",
        width=10,
        height=10,
        count=1,
        dtype=dtype,
        driver="GTiff",
        nodata=nodata,
        **opts,
    ) as ds:
        ds: DatasetWriter
        ds.write(np.ones((10, 10), dtype=dtype), 1)


def test_missing_measurement_from_product(
    eo_validator: ValidateRunner, l1_ls8_metadata_path: Path
):
    """Validator should notice a missing measurement from the product def"""
    product = dict(
        name="no_measurement",
        metadata_type="eo3",
        measurements=[dict(name="razzmatazz", dtype="int32", nodata=-999)],
    )
    eo_validator.assert_invalid(product, l1_ls8_metadata_path)
    assert eo_validator.messages == {
        "missing_measurement": "Product no_measurement expects a measurement 'razzmatazz')"
    }


def test_supports_measurementless_products(
    eo_validator: ValidateRunner, l1_ls8_metadata_path: Path
):
    """
    Validator should suport products without any measurements in the document.

    These are valid for prodcuts which can't be dc.load()'ed but are
    referred to for provenance, such as DEA's telemetry data or DEA's collection-2
    Level 1 products.
    """
    product = dict(name="no_measurement", metadata_type="eo3", measurements=None)
    eo_validator.assert_valid(product, l1_ls8_metadata_path)


def test_complains_about_measurement_lists(
    eo_validator: ValidateRunner, l1_ls8_metadata_path: Path
):
    """Complain when product measurements are a dict.

    datasets have measurements as a dict, products have them as a List, so this is a common error.
    """

    product = dict(name="bad_nodata", metadata_type="eo3", measurements={"a": {}})
    eo_validator.assert_invalid(product)
    assert eo_validator.messages == {
        "measurements_list": "Product measurements should be a list/sequence (Found a 'dict')."
    }


def test_complains_about_impossible_nodata_vals(
    eo_validator: ValidateRunner, l1_ls8_metadata_path: Path
):
    """Complain if a product nodata val cannot be represented in the dtype"""
    product = dict(
        name="bad_nodata",
        metadata_type="eo3",
        measurements=[
            dict(
                name="paradox",
                dtype="uint8",
                # Impossible for a uint6
                nodata=-999,
            )
        ],
    )
    eo_validator.assert_invalid(product)
    assert eo_validator.messages == {
        "unsuitable_nodata": "Measurement 'paradox' nodata -999 does not fit a 'uint8'"
    }


def test_complains_when_no_product(
    eo_validator: ValidateRunner, l1_ls8_metadata_path: Path
):
    """When a product is specified, it will validate that the measurements match the product"""
    # Thorough checking should fail when there's no product provided
    eo_validator.thorough = True
    eo_validator.record_informational_messages = True
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
