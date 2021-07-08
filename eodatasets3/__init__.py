# coding=utf-8

from __future__ import absolute_import

from ._version import get_versions
from .assemble import DatasetAssembler, DatasetPrepare, IfExists, IncompleteDatasetError
from .images import GridSpec
from .model import DatasetDoc
from .properties import Eo3Dict

from .names import namer, NamingConventions

REPO_URL = "https://github.com/GeoscienceAustralia/eo-datasets.git"

__version__ = get_versions()["version"]
del get_versions

__all__ = (
    "DatasetAssembler",
    "DatasetDoc",
    "DatasetPrepare",
    "Eo3Dict",
    "GridSpec",
    "IfExists",
    "IncompleteDatasetError",
    "NamingConventions",
    "namer",
    "REPO_URL",
    "__version__",
)
