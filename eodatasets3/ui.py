import os
import urllib.parse
from pathlib import Path
from typing import Optional, Union
from urllib.parse import urljoin
from urllib.parse import urlparse

import click


class PathPath(click.Path):
    """A Click path argument that returns a pathlib Path, not a string"""

    def convert(self, value, param, ctx):
        return Path(super().convert(value, param, ctx))


def uri_resolve(base: Union[str, Path], path: Optional[str]) -> str:
    """
    Backport of datacube.utils.uris.uri_resolve()
    """
    if path:
        p = Path(path)
        if p.is_absolute():
            return p.as_uri()

    if isinstance(base, Path):
        base = base.absolute().as_uri()
    return urljoin(base, path)


def bool_style(b, color=True) -> str:
    if b:
        return click.style("✓", fg=color and "green")
    else:
        return click.style("✗", fg=color and "yellow")


def is_absolute(url):
    """
    >>> is_absolute('LC08_L1TP_108078_20151203_20170401_01_T1.TIF')
    False
    >>> is_absolute('data/LC08_L1TP_108078_20151203_20170401_01_T1.TIF')
    False
    >>> is_absolute('/g/data/somewhere/LC08_L1TP_108078_20151203_20170401_01_T1.TIF')
    True
    >>> is_absolute('file:///g/data/v10/somewhere/LC08_L1TP_108078_20151203_20170401_01_T1.TIF')
    True
    >>> is_absolute('http://example.com/LC08_L1TP_108078_20151203_20170401_01_T1.TIF')
    True
    >>> is_absolute('tar:///g/data/v10/somewhere/dataset.tar#LC08_L1TP_108078_20151203_20170401_01_T1.TIF')
    True
    """
    location = urlparse(url)
    return bool(location.scheme or location.netloc) or os.path.isabs(location.path)


def register_scheme(*schemes):
    """
    Register additional uri schemes as supporting relative offsets (etc), so that band/measurement paths can be
    calculated relative to the base uri.
    """
    urllib.parse.uses_netloc.extend(schemes)
    urllib.parse.uses_relative.extend(schemes)
    urllib.parse.uses_params.extend(schemes)


register_scheme("tar")
register_scheme("s3")
