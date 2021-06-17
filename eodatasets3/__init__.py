# coding=utf-8

from __future__ import absolute_import

from ._version import get_versions
from .assemble import DatasetAssembler, IfExists, IncompleteDatasetError
from .images import GridSpec
from .model import DatasetDoc
from .properties import Eo3Properties, Eo3Dict

from .names import namer, NameGenerator

REPO_URL = "https://github.com/GeoscienceAustralia/eo-datasets.git"

__version__ = get_versions()["version"]
del get_versions

__all__ = (
    "DatasetAssembler",
    "DatasetDoc",
    "Eo3Dict",
    "Eo3Properties",
    "GridSpec",
    "IfExists",
    "IncompleteDatasetError",
    "NameGenerator",
    "namer",
    "REPO_URL",
    "__version__",
)
