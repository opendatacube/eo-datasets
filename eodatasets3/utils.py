import enum
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Tuple, Dict, Any

import ciso8601
import click

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


def normalise_band_name(band_name: str) -> str:
    """
    Normalise band names by our norms.

    Numeric bands have a 'band' prefix, others are lowercase with

    >>> normalise_band_name('4')
    'band04'
    >>> normalise_band_name('QUALITY')
    'quality'
    >>> normalise_band_name('Azimuthal-Angles')
    'azimuthal_angles'
    """
    try:
        number = int(band_name)
        band_name = f"band{number:02}"
    except ValueError:
        pass
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
