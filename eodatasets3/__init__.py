# coding=utf-8

from __future__ import absolute_import

from ._version import get_versions
from .assemble import DatasetAssembler, IfExists
from .properties import Eo3Properties, Eo3Dict

from .names import namer

REPO_URL = "https://github.com/GeoscienceAustralia/eo-datasets.git"

__version__ = get_versions()["version"]
del get_versions

__all__ = (
    "DatasetAssembler",
    "Eo3Dict",
    "Eo3Properties",
    "IfExists",
    "namer",
    "REPO_URL",
    "__version__",
)
