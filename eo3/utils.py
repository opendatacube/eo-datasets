import enum
import functools
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Tuple

import ciso8601
import click
from datacube.config import LocalConfig

EO3_SCHEMA = "https://schemas.opendatacube.org/dataset"


class ItemProvider(enum.Enum):
    PRODUCER = "producer"
    PROCESSOR = "processor"
    HOST = "host"
    LICENSOR = "licensor"


class ClickDatetime(click.ParamType):
    """
    Take a datetime parameter, supporting any ISO8601 date/time/timezone combination.
    """

    name = "date"

    def convert(self, value, param, ctx):
        if value is None:
            return value

        if isinstance(value, datetime):
            return value

        try:
            return ciso8601.parse_datetime(value)
        except ValueError:
            self.fail(
                (
                    "Invalid date string {!r}. Expected any ISO date/time format "
                    '(eg. "2017-04-03" or "2014-05-14 12:34")'.format(value)
                ),
                param,
                ctx,
            )


def read_paths_from_file(listing: Path) -> Iterable[Path]:
    """
    A generator that yields path from a file; paths encoded one per line
    """
    with listing.open("r") as f:
        for loc in f.readlines():
            path = Path(loc.strip())
            if not path.exists():
                raise FileNotFoundError(
                    f"No such file or directory: {os.path.abspath(loc)}"
                )

            yield path.absolute()


def default_utc(d: datetime) -> datetime:
    if d.tzinfo is None:
        return d.replace(tzinfo=timezone.utc)
    return d


def subfolderise(code: str) -> Tuple[str, ...]:
    """
    Cut a string folder name into subfolders if long.

    (Forward slashes only, as it assumes you're using Pathlib's normalisation)

    >>> subfolderise('089090')
    ('089', '090')
    >>> # Prefer fewer folders in first level.
    >>> subfolderise('12345')
    ('12', '345')
    >>> subfolderise('123456')
    ('123', '456')
    >>> subfolderise('1234567')
    ('123', '4567')
    >>> subfolderise('12')
    ('12',)
    """
    if len(code) > 2:
        return (code[: len(code) // 2], code[len(code) // 2 :])
    return (code,)


_NUMERIC_BAND_NAME = re.compile(r"(?P<number>\d+)(?P<suffix>[a-zA-Z]?)", re.IGNORECASE)


def normalise_band_name(band_name: str) -> str:
    """
    Normalise band names by our norms.

    Numeric bands have a 'band' prefix, others are lowercase with

    >>> normalise_band_name('4')
    'band04'
    >>> normalise_band_name('8a')
    'band08a'
    >>> normalise_band_name('8A')
    'band08a'
    >>> normalise_band_name('QUALITY')
    'quality'
    >>> normalise_band_name('Azimuthal-Angles')
    'azimuthal_angles'
    """

    match = _NUMERIC_BAND_NAME.match(band_name)
    if match:
        number = int(match.group("number"))
        suffix = match.group("suffix")
        band_name = f"band{number:02}{suffix}"

    return band_name.lower().replace("-", "_")


def get_collection_number(
    platform: str, producer: str, usgs_collection_number: int
) -> int:
    # This logic is in one place as it's not very future-proof...

    # We didn't do sentinel before now...
    if platform.startswith("sentinel"):
        return 3

    if producer == "usgs.gov":
        return usgs_collection_number
    elif producer == "ga.gov.au":
        # GA's collection 3 processes USGS Collection 1 and 2
        if usgs_collection_number == 1 or usgs_collection_number == 2:
            return 3
        else:
            raise NotImplementedError("Unsupported GA collection number.")
    raise NotImplementedError(
        f"Unsupported collection number mapping for org: {producer!r}"
    )


def is_doc_eo3(doc: Dict[str, Any]) -> bool:
    """Is this document eo3?

    :param doc: Parsed ODC Dataset metadata document

    :returns:
        False if this document is a legacy dataset
        True if this document is eo3

    :raises ValueError: For an unsupported document
    """
    schema = doc.get("$schema")
    # All legacy documents had no schema at all.
    if schema is None:
        return False

    if schema == EO3_SCHEMA:
        return True

    # Otherwise it has an unknown schema.
    #
    # Reject it for now.
    # We don't want future documents (like Stac items, or "eo4") to be quietly
    # accepted as legacy eo.
    raise ValueError(f"Unsupported dataset schema: {schema!r}")


def flatten_dict(
    d: Mapping, prefix: str = None, separator: str = "."
) -> Iterable[Tuple[str, Any]]:
    """
    Flatten a nested dicts into one level, with keys that show their original nested path ("a.b.c")

    Returns them as a generator of (key, value) pairs.

    (Doesn't currently venture into other collection types, like lists)

    >>> dict(flatten_dict({'a' : 1, 'b' : {'inner' : 2},'c' : 3}))
    {'a': 1, 'b.inner': 2, 'c': 3}
    >>> dict(flatten_dict({'a' : 1, 'b' : {'inner' : {'core' : 2}}}, prefix='outside', separator=':'))
    {'outside:a': 1, 'outside:b:inner:core': 2}
    """
    for k, v in d.items():
        name = f"{prefix}{separator}{k}" if prefix else k
        if isinstance(v, Mapping):
            yield from flatten_dict(v, prefix=name, separator=separator)
        else:
            yield name, v


def pass_config(*, required=True):
    """
    Get a datacube config as the first argument.

    Based on ODC's pass_config(), but allows the config to not exist.

    If required=False, allow the config to be None.
    """

    def pass_config_outer(fn):
        @functools.wraps(fn)
        def inner(*args, **kwargs):
            obj = click.get_current_context().obj

            paths = obj.get("config_files", None)
            # If the user is overriding the defaults
            specific_environment = obj.get("config_environment")
            parsed_config = None

            try:
                parsed_config = LocalConfig.find(paths=paths, env=specific_environment)
            except ValueError as e:
                if specific_environment:
                    raise click.ClickException(
                        f"No datacube config found for '{specific_environment}'"
                    ) from e
                elif required:
                    raise click.ClickException("No datacube config found") from e

            return fn(parsed_config, *args, **kwargs)

        return inner

    return pass_config_outer
