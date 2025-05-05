from ._version import __version__
from .assemble import DatasetAssembler, DatasetPrepare, IfExists, IncompleteDatasetError
from .images import GridSpec, ValidDataMethod
from .model import DatasetDoc
from .names import NamingConventions, namer
from .properties import Eo3Dict

REPO_URL = "https://github.com/opendatacube/eo-datasets.git"


__all__ = (
    "REPO_URL",
    "DatasetAssembler",
    "DatasetDoc",
    "DatasetPrepare",
    "Eo3Dict",
    "GridSpec",
    "IfExists",
    "IncompleteDatasetError",
    "NamingConventions",
    "ValidDataMethod",
    "__version__",
    "namer",
)
