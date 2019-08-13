"""
Validate ODC dataset documents
"""
import collections
import enum
import math
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Counter, Dict, Generator, Optional, Union, Tuple

import attr
import click
import rasterio
from boltons.iterutils import get_path
from click import style, echo, secho
from eodatasets3 import serialise, model
from eodatasets3.model import DatasetDoc
from eodatasets3.ui import PathPath, is_absolute, uri_resolve
from eodatasets3.utils import default_utc
from rasterio import DatasetReader
from rasterio.crs import CRS
from rasterio.errors import CRSError
from shapely.validation import explain_validity


class Level(enum.Enum):
    info = 1
    warning = 2
    error = 3


@attr.s(auto_attribs=True, frozen=True)
class ValidationMessage:
    level: Level
    code: str
    reason: str
    hint: str = None

    def __str__(self) -> str:
        hint = ""
        if self.hint:
            hint = f" (Hint: {self.hint})"
        return f"{self.code}: {self.reason}{hint}"


def _info(code: str, reason: str, hint: str = None):
    return ValidationMessage(Level.info, code, reason, hint=hint)


def _warning(code: str, reason: str, hint: str = None):
    return ValidationMessage(Level.warning, code, reason, hint=hint)


def _error(code: str, reason: str, hint: str = None):
    return ValidationMessage(Level.error, code, reason, hint=hint)


def validate(
    doc: Dict,
    # Optionally check that the dataset mathces this product definition.
    product_definition: Optional[Dict] = None,
    # A thorough validation will try to open the data itself.
    thorough: bool = False,
    # Dataset location to use, if not the metadata path.
    readable_location: Union[str, Path] = None,
) -> Generator[ValidationMessage, None, None]:
    schema = doc.get("$schema")
    if schema is None:
        yield _error(
            "no_schema",
            f"No $schema field. "
            f"You probably want an ODC dataset schema {model.ODC_DATASET_SCHEMA_URL!r}",
        )
        return
    if schema != model.ODC_DATASET_SCHEMA_URL:
        yield _error(
            "unknown_doc_type",
            f"Unknown doc schema {schema!r}. Only ODC datasets are supported ({model.ODC_DATASET_SCHEMA_URL!r})",
        )
        return

    has_doc_errors = False
    for error in serialise.DATASET_SCHEMA.iter_errors(doc):
        has_doc_errors = True
        displayable_path = ".".join(error.absolute_path)

        hint = None
        if displayable_path == "crs" and "not of type" in error.message:
            hint = "epsg codes should be prefixed with 'epsg:1234'"

        context = f"({displayable_path}) " if displayable_path else ""
        yield _error("structure", f"{context}{error.message} ", hint=hint)

    if has_doc_errors:
        return

    dataset = serialise.from_doc(doc, skip_validation=True)

    if not dataset.product.href:
        _info("product_href", "A url (href) is recommended for products")

    yield from _validate_geo(dataset)

    # Note that a dataset may have no measurements (eg. telemetry data).
    # (TODO: a stricter mode for when we know we should have geo and measurement info)
    if dataset.measurements:
        for name, measurement in dataset.measurements.items():
            grid_name = measurement.grid
            if grid_name != "default" or dataset.grids:
                if grid_name not in dataset.grids:
                    yield _error(
                        "invalid_grid_ref",
                        f"Measurement {name!r} refers to unknown grid {grid_name!r}",
                    )

            if is_absolute(measurement.path):
                yield _warning(
                    "absolute_path",
                    f"measurement {name!r} has an absolute path: {measurement.path!r}",
                )

    yield from _validate_stac_properties(dataset)

    required_measurements: Dict[str, ExpectedMeasurement] = {}
    if product_definition is not None:
        required_measurements.update(
            {
                m.name: m
                for m in map(
                    ExpectedMeasurement.from_definition,
                    product_definition["measurements"],
                )
            }
        )

        product_name = product_definition.get("name")
        if product_name != dataset.product.name:
            # This is only informational as it's possible products may be indexed with finer-grained
            # categories than the original datasets: eg. a separate "nrt" product, or test product.
            yield _info(
                "product_mismatch",
                f"Dataset product name {dataset.product.name!r} "
                f"does not match the given product ({product_name!r}",
            )

        for name in required_measurements:
            if name not in dataset.measurements.keys():
                yield _error(
                    "missing_measurement",
                    f"Product {product_name} expects a measurement {name!r})",
                )

    # If we have a location:
    # For each measurement, try to load it.
    # If loadable:
    if thorough:
        for name, measurement in dataset.measurements.items():
            full_path = uri_resolve(readable_location, measurement.path)
            expected_measurement = required_measurements.get(name)

            band = measurement.band or 1
            with rasterio.open(full_path) as ds:
                ds: DatasetReader

                if band not in ds.indexes:
                    yield _error(
                        "incorrect_band",
                        f"Measurement {name!r} file contains no rio index {band!r}.",
                        hint=f"contains indexes {ds.indexes!r}",
                    )
                    continue

                if not expected_measurement:
                    # The measurement is not in the product definition
                    #
                    # This is only informational because a product doesn't have to define all
                    # measurements that the datasets contain.
                    #
                    # This is historically because dataset documents reflect the measurements that
                    # are stored on disk, which can differ. But products define the set of measurments
                    # that are mandatory in every dataset.
                    #
                    # (datasets differ when, for example, sensors go offline, or when there's on-disk
                    #  measurements like panchromatic that GA doesn't want in their product definitions)
                    if required_measurements:
                        yield _info(
                            "unspecified_measurement",
                            f"Measurement {name} is not in the product",
                        )
                else:
                    expected_dtype = expected_measurement.dtype
                    band_dtype = ds.dtypes[band - 1]
                    # TODO: NaN handling
                    if expected_dtype != band_dtype:
                        yield _error(
                            "different_dtype",
                            f"{name} dtype: "
                            f"product {expected_dtype!r} != dataset {band_dtype!r}",
                        )

                    # TODO: the nodata can also be a fill value, as mentioned by Kirill.
                    expected_nodata = expected_measurement.nodata
                    ds_nodata = ds.nodatavals[band - 1]
                    if expected_nodata != ds_nodata and not (
                        _is_nan(expected_nodata) and _is_nan(ds_nodata)
                    ):
                        yield _error(
                            "different_nodata",
                            f"{name} nodata: "
                            f"product {expected_nodata !r} != dataset {ds_nodata !r}",
                        )


# - product definition checks: nodata, dtype.
# - grid matches?


@attr.s(auto_attribs=True)
class ExpectedMeasurement:
    name: str
    dtype: str
    nodata: int

    @classmethod
    def from_definition(cls, doc: Dict):
        return ExpectedMeasurement(doc["name"], doc.get("dtype"), doc.get("nodata"))


def validate_paths(
    paths: List[Path], thorough: bool = False, strict=False
) -> Generator[Tuple[Path, List[ValidationMessage]], None, None]:
    """Validate the list of paths. Product documents can be specified before their datasets."""
    products: Dict[str, Dict] = {}

    for path in paths:
        # Load yaml. If product, add to products.
        # Otherwise validate.
        doc = serialise.load_yaml(path)

        if is_product(doc):
            products[doc["name"]] = doc
            continue
        messages = []

        # TODO: follow ODC's match rules?
        product = None
        product_name = get_path(doc, ("product", "name"), default=None)

        if products:
            if len(products) == 1:
                [product] = products.values()
            elif product_name is not None:
                product = products.get(product_name)

            if product is None:
                messages.append(
                    _warning(
                        "unknown_product",
                        f"Cannot match dataset to product",
                        hint=f"Nothing matches {product_name!r}"
                        if product_name
                        else "No product name in dataset (TODO: field matching)",
                    )
                )
        else:
            messages.append(
                ValidationMessage(
                    Level.error if thorough else Level.info,
                    "no_product",
                    f"No product provided: validating dataset information alone",
                )
            )

        messages.extend(
            validate(
                doc,
                product_definition=product,
                readable_location=path,
                thorough=thorough,
            )
        )
        yield path, messages


def is_product(doc: Dict) -> bool:
    """Is this a product document?"""
    return "metadata_type" in doc


def _validate_stac_properties(dataset: DatasetDoc):
    for name, value in dataset.properties.items():
        if name not in dataset.properties.KNOWN_STAC_PROPERTIES:
            yield _warning("unknown_property", f"Unknown stac property {name!r}")

        else:
            normaliser = dataset.properties.KNOWN_STAC_PROPERTIES.get(name)
            if normaliser and value is not None:
                try:
                    normalised_value = normaliser(value)

                    # Special case for dates, as "no timezone" and "utc timezone" are treated identical.
                    if isinstance(value, datetime):
                        value = default_utc(value)

                    if not isinstance(value, type(normalised_value)):
                        yield _warning(
                            "property_type",
                            f"Value {value} expected to be "
                            f"{type(normalised_value).__name__!r} (got {type(value).__name__!r})",
                        )
                    elif normalised_value != value:
                        if _is_nan(normalised_value) and _is_nan(value):
                            # Both are NaNs, ignore.
                            pass
                        else:
                            yield _warning(
                                "property_formatting",
                                f"Property {value!r} expected to be {normalised_value!r}",
                            )
                except ValueError as e:
                    yield _error("invalid_property", e.args[0])

    if "odc:producer" in dataset.properties:
        producer = dataset.properties["odc:producer"]
        # We use domain name to avoid arguing about naming conventions ('ga' vs 'geoscience-australia' vs ...)
        if "." not in producer:
            yield _warning(
                "producer_domain",
                "Property 'odc:producer' should be the organisation's domain name. Eg. 'ga.gov.au'",
            )

    # This field is a little odd, but is expected by the current version of ODC.
    # (from discussion with Kirill)
    if not dataset.properties.get("odc:file_format"):
        yield _warning(
            "global_file_format",
            "Property 'odc:file_format' is empty",
            hint="Usually 'GeoTIFF'",
        )


def _is_nan(v):
    return isinstance(v, float) and math.isnan(v)


def _validate_geo(dataset: DatasetDoc):
    has_some_geo = _has_some_geo(dataset)
    if not has_some_geo:
        yield _info("non_geo", "No geo information in dataset")
        return

    if dataset.geometry is None:
        yield _error("incomplete_geo", "Dataset has some geo fields but no geometry")
    elif not dataset.geometry.is_valid:
        yield _error(
            "invalid_geometry",
            f"Geometry is not a valid shape: {explain_validity(dataset.geometry)!r}",
        )

    # TODO: maybe we'll allow no grids: backwards compat with old metadata.
    if not dataset.grids:
        yield _error("incomplete_grids", "Dataset has some geo fields but no grids")

    if not dataset.crs:
        yield _error("incomplete_crs", "Dataset has some geo fields but no crs")
    else:
        # We only officially support epsg code (recommended) or wkt.
        if dataset.crs.lower().startswith("epsg:"):
            try:
                CRS.from_string(dataset.crs)
            except CRSError as e:
                yield _error("invalid_crs_epsg", e.args[0])

            if dataset.crs.lower() != dataset.crs:
                yield _warning("mixed_crs_case", "Recommend lowercase 'epsg:' prefix")
        else:
            wkt_crs = None
            try:
                wkt_crs = CRS.from_wkt(dataset.crs)
            except CRSError as e:
                yield _error(
                    "invalid_crs",
                    f"Expect either an epsg code or a WKT string: {e.args[0]}",
                )

            if wkt_crs and wkt_crs.is_epsg_code:
                yield _warning(
                    "non_epsg",
                    f"Prefer an EPSG code to a WKT when possible. (Can change CRS to 'epsg:{wkt_crs.to_epsg()}')",
                )


def _has_some_geo(dataset):
    return dataset.geometry is not None or dataset.grids or dataset.crs


@click.command(
    help=__doc__
    + """
Paths can be both product and dataset
documents, but each product must come before
its datasets to be matched against it.
"""
)
@click.argument("paths", nargs=-1, type=PathPath(exists=True, readable=True))
@click.option(
    "--warnings-as-errors",
    "-W",
    "strict_warnings",
    is_flag=True,
    help="Fail if any warnings are produced",
)
@click.option(
    "--thorough",
    is_flag=True,
    help="Attempt to read the data/measurements, and check their properties match",
)
@click.option("-q", "--quiet", is_flag=True, help="Only print problems, one per line")
def run(paths: List[Path], strict_warnings, quiet, thorough: bool):
    validation_counts: Counter[Level] = collections.Counter()

    s = {Level.info: {}, Level.warning: dict(bold=True), Level.error: dict(fg="red")}
    for path, messages in validate_paths(paths, thorough=thorough):
        if quiet:
            messages = [m for m in messages if m.level != Level.info]

        if not messages:
            if not quiet:
                secho(f"{path.stem}: {style('✓', fg='green')}")
            continue

        secho(path.stem)
        for message in messages:
            validation_counts[message.level] += 1

            displayable_code = style(f"{message.code}", **s[message.level])
            echo(
                f"- {message.level.name[0].upper()}\t{displayable_code}\t{message.reason}"
            )
            if message.hint:
                echo(f'\t({style("Hint", fg="green")}: {message.hint})')

    error_count = validation_counts.get(Level.error) or 0
    if strict_warnings:
        error_count += validation_counts.get(Level.warning) or 0

    if not quiet:
        result = (
            style("failure", fg="red") if error_count > 0 else style("ok", fg="green")
        )
        secho(f"\n{result}: ", nl=False, err=True)
        if validation_counts:
            echo(
                ", ".join(
                    f"{v} {k.name}{'s' if v > 1 else ''}"
                    for k, v in validation_counts.items()
                ),
                err=True,
            )
        else:
            secho("All good", fg="green", err=True)

    sys.exit(error_count)
