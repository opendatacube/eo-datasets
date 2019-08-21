# coding=utf-8

from __future__ import absolute_import

from ._version import get_versions
from .assemble import DatasetAssembler, IfExists

REPO_URL = "https://github.com/GeoscienceAustralia/eo-datasets.git"

__version__ = get_versions()["version"]
del get_versions

__all__ = (DatasetAssembler, IfExists, REPO_URL, __version__)
