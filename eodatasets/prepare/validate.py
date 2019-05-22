import collections
import enum
import sys
from pathlib import Path
from typing import List, Counter

import attr
import click
from click import style, echo, secho
from rasterio.crs import CRS
from rasterio.errors import CRSError
from shapely.validation import explain_validity

from eodatasets.prepare import serialise
from eodatasets.prepare.model import Dataset
from eodatasets.ui import PathPath, uri_resolve


class Level(enum.Enum):
    info = 1
    warning = 2
    error = 3


@attr.s(auto_attribs=True, frozen=True)
class ValidationMessage:
    level: Level
    code: str
    reason: str
    path: Path = None


def _info(path, code: str, reason: str):
    return ValidationMessage(Level.info, code, reason, path)


def _warning(path, code: str, reason: str):
    return ValidationMessage(Level.warning, code, reason, path)


def _error(path, code: str, reason: str):
    return ValidationMessage(Level.error, code, reason, path)


def validate(path: Path, thorough: bool = False):
    doc = serialise.load_yaml(path)
    has_doc_errors = False
    for error in serialise.DATASET_SCHEMA.iter_errors(doc):
        has_doc_errors = True
        displayable_path = ".".join(error.absolute_path)
        context = f"({displayable_path}) " if displayable_path else ""
        yield _error(path, "doc_error", f"{context}{error.message} ")

    if has_doc_errors:
        return

    dataset = serialise.from_doc(doc, skip_validation=True)

    yield from _validate_geo(dataset, path)

    for name, measurement in dataset.measurements.items():
        grid_name = measurement.grid
        if grid_name not in dataset.grids:
            yield _error(
                path,
                "invalid_grid_ref",
                f"Measurement {name!r} refers to unknown grid {grid_name!r}",
            )

        full_measurement_path = uri_resolve(str(path.absolute()), measurement.path)
        if not full_measurement_path.startswith(str(path.absolute().parent)):
            yield _warning(
                path,
                "absolute_path",
                f"measurement {name!r} has an absolute path: {measurement.path!r}",
            )

        if thorough:
            # Load file, check dimensions etc are correct.
            pass


def _validate_geo(dataset: Dataset, path: Path):
    has_some_geo = dataset.geometry is not None or dataset.grids or dataset.crs
    if not has_some_geo:
        yield _info(path, "non_geo", "No geo information in dataset")
        return

    if dataset.geometry is None:
        yield _error(
            path, "incomplete_geo", "Dataset has some geo fields but no geometry"
        )
    elif not dataset.geometry.is_valid:
        yield _error(
            path,
            "invalid_geometry",
            f"Geometry is not a valid shape: {explain_validity(dataset.geometry)!r}",
        )

    # TODO: maybe we'll allow no grids: backwards compat with old metadata.
    if not dataset.grids:
        yield _error(
            path, "incomplete_grids", "Dataset has some geo fields but no grids"
        )

    if not dataset.crs:
        yield _error(path, "incomplete_crs", "Dataset has some geo fields but no crs")
    else:
        # We only officially support epsg code (recommended) or wkt.
        if dataset.crs.lower().startswith("epsg:"):
            try:
                CRS.from_string(dataset.crs)
            except CRSError as e:
                yield _error(path, "invalid_crs_epsg", e.args[0])

            if dataset.crs.lower() != dataset.crs:
                yield _warning(
                    path, "mixed_crs_case", "Recommend lowercase 'epsg:' prefix"
                )
        else:
            wkt_crs = None
            try:
                wkt_crs = CRS.from_wkt(dataset.crs)
            except CRSError as e:
                yield _error(
                    path,
                    "invalid_crs",
                    f"Expect either an epsg code or a WKT string: {e.args[0]}",
                )

            if wkt_crs and wkt_crs.is_epsg_code:
                yield _warning(
                    path,
                    "non_epsg",
                    f"Prefer an EPSG code to a WKT when possible. (Can change CRS to 'epsg:{wkt_crs.to_epsg()}')",
                )


@click.command()
@click.argument("paths", nargs=-1, type=PathPath(exists=True, readable=True))
@click.option("--warnings-as-errors", "-W", "strict_warnings", is_flag=True)
@click.option("-q", "--quiet", is_flag=True)
def run(paths: List[Path], strict_warnings, quiet):
    validation_counts: Counter[Level] = collections.Counter()

    s = {Level.info: {}, Level.warning: dict(bold=True), Level.error: dict(fg="red")}
    for path in paths:
        if not quiet:
            echo(f"\n{path.stem}")
        for message in validate(path):
            validation_counts[message.level] += 1
            if message.level == Level.info and quiet:
                continue

            displayable_code = style(
                f"{message.level.name[0].upper()}\t{message.code}", **s[message.level]
            )
            echo(f"{displayable_code}: {message.reason}")

    error_count = validation_counts.get(Level.error) or 0
    if strict_warnings:
        error_count += validation_counts.get(Level.warning)

    if not quiet:
        result = (
            style("failure", fg="red") if error_count > 0 else style("ok", fg="green")
        )
        secho(f"\n{result}: ", nl=False)
        if validation_counts:
            echo(", ".join(f"{v} {k.name}" for k, v in validation_counts.items()))
        else:
            secho("All good", fg="green")

    sys.exit(error_count)
