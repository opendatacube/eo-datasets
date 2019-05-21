import collections
import enum
import sys
from pathlib import Path
from typing import List, Counter

import attr
import click
from click import style, echo
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
    return ValidationMessage(Level.warning, code, reason, path)


def validate(path: Path, thorough: bool = False):
    dataset = serialise.from_path(path)

    yield from _validate_geo(dataset, path)

    for name, measurement in dataset.measurements.items():
        grid_name = measurement.grid
        if grid_name not in dataset.grids:
            yield _error(path, "invalid_grid_ref", f"Measurement {name!r} refers to unknown grid {grid_name!r}")

        full_measurement_path = uri_resolve(str(path.absolute()), measurement.path)
        if not full_measurement_path.startswith(str(path.absolute())):
            yield _warning(path, "absolute_path", f"measurement {name} has an absolute path: {path!r}")

        if thorough:
            # Load file, check dimensions etc are correct.
            pass


def _validate_geo(dataset: Dataset, path: Path):
    has_some_geo = dataset.geometry is not None or dataset.grids or dataset.crs
    if not has_some_geo:
        yield _info(path, 'non_geo', "No geo information in dataset")
        return

    if dataset.geometry is None:
        yield _error(path, "incomplete_geo", "Dataset has crs/grids but no geometry")
    elif not dataset.geometry.is_valid:
        yield _error(path, 'invalid_geometry',
                     f'Geometry is not a valid shape: {explain_validity(dataset.geometry)!r}')

    if not dataset.crs:
        yield _error(path, "incomplete_crs", "Dataset has geometry/grids but no crs")

    if not dataset.grids:
        yield _error(path, "incomplete_grids", "Dataset has geometry/crs but no grids")

    for name, grid in dataset.grids.items():
        # jsonschema already enforces shape/transform exists.
        pass


@click.command()
@click.argument("paths", nargs=-1, type=PathPath(exists=True, readable=True))
@click.option("--warnings-as-errors", '-W', 'strict_warnings', is_flag=True)
@click.option('-v', "--verbose", is_flag=True)
def run(paths: List[Path], strict_warnings, verbose):
    validation_counts: Counter[Level] = collections.Counter()

    s = {
        Level.info: {},
        Level.warning: dict(bold=True),
        Level.error: dict(fg='red'),
    }
    for path in paths:
        for message in validate(path):
            if message.level == Level.info and not verbose:
                continue

            displayable_code = style(message.code, **s[message.level])
            echo(f"{displayable_code}: {message.reason}")

    error_count = validation_counts.get(Level.error)
    if strict_warnings:
        error_count += validation_counts.get(Level.warning)

    if verbose:
        echo(f"\nDONE {len(paths)}: ", nl=False)
        echo(", ".join(f"{v} {k}" for k, v in validation_counts.items()))

    sys.exit(error_count)
