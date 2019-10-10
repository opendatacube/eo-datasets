import enum
import os
import shutil
from urllib.parse import urlparse

import ciso8601
import click
import fsspec
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Tuple, Union


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
    with open_url_or_path(listing, "r") as f:
        for loc in f.readlines():
            path = Path(loc.strip())
            if not path.exists():
                raise FileNotFoundError(
                    "No such file or directory: %s" % (os.path.abspath(loc),)
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


def get_collection_number(producer: str, usgs_collection_number: int) -> int:
    # This logic is in one place as it's not very future-proof...

    if producer == "usgs.gov":
        return usgs_collection_number
    elif producer == "ga.gov.au":
        # GA's collection 3 processes USGS Collection 1
        if usgs_collection_number == 1:
            return 3
        else:
            raise NotImplementedError(f"Unsupported GA collection number.")
    raise NotImplementedError(
        f"Unsupported collection number mapping for org: {producer!r}"
    )


def open_url_or_path(url_or_path: Union[Path, str], mode: str = "rb"):
    return fsspec.open(str(url_or_path), mode)


class SimpleUrl(str):
    """Obscenely simple wrapper to try and support joining URL strings the same way as Pathlib paths"""

    def __truediv__(self, other):
        base = self
        # I don't want to write this, but urllib.parse.urljoin doesn't support s3://
        if not base.endswith("/"):
            base += "/"
        if other.startswith("/"):
            other = other[1:]
        return SimpleUrl(base + other)

    @property
    def parent(self):
        return SimpleUrl(self[: self.rindex("/")])

    @property
    def name(self) -> str:
        return self.rsplit("/")[-1]

    def absolute(self):
        return self

    def mkdir(self):
        pass

    def exists(self):
        return True

    def joinpath(self, *parts):
        return self / "/".join(parts)


def is_url(maybe_url):
    return "://" in str(maybe_url)


def _files_to_copy(src_base: Path, dst_base: SimpleUrl) -> Iterable[Tuple[Path, SimpleUrl]]:
    for base, _, files in os.walk(src_base):
        b = Path(base)
        for f in files:
            yield (b/f, dst_base/str(b/f))


def upload_directory(src: Path, dest: SimpleUrl):
    """
    Upload a local directory or file to a remote URL
    """
    url = urlparse(dest)
    fs = fsspec.filesystem(url.scheme)
    with fs.transaction:
        for f_src, f_dst in _files_to_copy(src, dest):
            fs.put(str(f_src), str(f_dst))


def copy_file(src: SimpleUrl, dest: Path):
    """
    Download a remote file or directory to a local path
    """
    if is_url(src):
        url = urlparse(src)
        fs = fsspec.filesystem(url.scheme)
        fs.get(src, str(dest / src.name))
    else:
        shutil.copy(str(src), dest)
