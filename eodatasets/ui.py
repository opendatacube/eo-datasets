from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

import click


class PathPath(click.Path):
    """A Click path argument that returns a pathlib Path, not a string"""
    def convert(self, value, param, ctx):
        return Path(super().convert(value, param, ctx))


def uri_resolve(base: str, path: Optional[str]) -> str:
    """
    Backport of datacube.utils.uris.uri_resolve()
    """
    if path:
        p = Path(path)
        if p.is_absolute():
            return p.as_uri()

    return urljoin(base, path)

