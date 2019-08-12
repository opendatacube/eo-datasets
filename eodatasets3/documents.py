# coding=utf-8
"""
Common methods for UI code.
"""
from __future__ import absolute_import

import gzip
import json
from copy import deepcopy
from pathlib import Path, PurePath
from typing import Generator, Dict, Tuple, Optional

from boltons import iterutils
from eodatasets3 import serialise

_DOCUMENT_EXTENSIONS = (".yaml", ".yml", ".json")
_COMPRESSION_EXTENSIONS = ("", ".gz")

# Both compressed (*.gz) and uncompressed.
_ALL_SUPPORTED_EXTENSIONS = tuple(
    doc_type + compression_type
    for doc_type in _DOCUMENT_EXTENSIONS
    for compression_type in _COMPRESSION_EXTENSIONS
)


def is_supported_document_type(path):
    """
    Does a document path look like a supported type?
    :type path: pathlib.Path
    :rtype: bool
    >>> from pathlib import Path
    >>> is_supported_document_type(Path('/tmp/something.yaml'))
    True
    >>> is_supported_document_type(Path('/tmp/something.YML'))
    True
    >>> is_supported_document_type(Path('/tmp/something.yaml.gz'))
    True
    >>> is_supported_document_type(Path('/tmp/something.tif'))
    False
    >>> is_supported_document_type(Path('/tmp/something.tif.gz'))
    False
    """
    return any(
        [str(path).lower().endswith(suffix) for suffix in _ALL_SUPPORTED_EXTENSIONS]
    )


def find_metadata_path(dataset_path):
    """
    Find a metadata path for a given input/dataset path.

    :type dataset_path: pathlib.Path
    :rtype: Path
    """

    # They may have given us a metadata file directly.
    if dataset_path.is_file() and is_supported_document_type(dataset_path):
        return dataset_path

    for system_name in ("odc-metadata", "agdc-md", "ga-md"):
        # Otherwise there may be a sibling file with appended suffix '.ga-md.yaml'.
        expected_name = dataset_path.parent.joinpath(
            f"{dataset_path.stem}.{system_name}"
        )
        found = _find_any_metadata_suffix(expected_name)
        if found:
            return found

    if dataset_path.is_dir():
        # Eo3-style.
        for m in dataset_path.glob("*.odc-metadata.*"):
            return m

        for system_name in "agdc", "ga":
            # Otherwise if it's a directory, there may be an 'ga-metadata.yaml' file describing all contained datasets.
            expected_name = dataset_path.joinpath(system_name + "-metadata")
            found = _find_any_metadata_suffix(expected_name)
            if found:
                return found

    return None


def new_metadata_path(dataset_path):
    """
    Get the path where we should write a metadata file for this dataset.

    :type dataset_path: Path
    :rtype: Path
    """

    # - A dataset directory expects file 'ga-metadata.yaml'.
    # - A dataset file expects a sibling file with suffix '.ga-md.yaml'.

    if dataset_path.is_dir():
        return dataset_path.joinpath("ga-metadata.yaml")

    if dataset_path.is_file():
        return dataset_path.parent.joinpath("{}.ga-md.yaml".format(dataset_path.name))

    raise ValueError("Unhandled path type for %r" % dataset_path)


def _find_any_metadata_suffix(path):
    """
    Find any metadata files that exist with the given file name/path.
    (supported suffixes are tried on the name)
    :type path: pathlib.Path
    """
    existing_paths = list(
        filter(is_supported_document_type, path.parent.glob(path.name + "*"))
    )
    if not existing_paths:
        return None

    if len(existing_paths) > 1:
        raise ValueError("Multiple matched metadata files: {!r}".format(existing_paths))

    return existing_paths[0]


def find_and_read_documents(*paths: Path) -> Generator[Tuple[Path, Dict], None, None]:
    # Scan all paths immediately so we can fail fast if some are wrong.
    metadata_paths = [(path, find_metadata_path(path)) for path in paths]

    missing_paths = [path for (path, md) in metadata_paths if md is None]
    if missing_paths:
        raise ValueError(
            f"No metadata found for input path{'s' if len(missing_paths) > 1 else ''}: "
            f"{', '.join(map(str, missing_paths))}"
        )

    for input_path, metadata_path in metadata_paths:
        yield from read_documents(metadata_path)


def read_documents(*paths: Path) -> Generator[Tuple[Path, Dict], None, None]:
    """
    Read & parse documents from the filesystem (yaml or json).

    Note that a single yaml file can contain multiple documents.
    """
    for path in paths:
        suffix = path.suffix.lower()

        # If compressed, open as gzip stream.
        opener = open
        if suffix == ".gz":
            suffix = path.suffixes[-2].lower()
            opener = gzip.open

        with opener(str(path), "r") as f:
            if suffix in (".yaml", ".yml"):
                for parsed_doc in serialise.loads_yaml(f):
                    yield path, parsed_doc
            elif suffix == ".json":
                yield path, json.load(f)
            else:
                raise ValueError(
                    "Unknown document type for {}; expected one of {!r}.".format(
                        path.name, _ALL_SUPPORTED_EXTENSIONS
                    )
                )


def docpath_set(doc, path, value):
    """
    Set a value in a document using a path (sequence of keys).

    (It's designed to mirror `boltons.iterutils.get_path()` and related methods)

    >>> d = {'a': 1}
    >>> docpath_set(d, ['a'], 2)
    >>> d
    {'a': 2}
    >>> d = {'a':{'b':{'c': 1}}}
    >>> docpath_set(d, ['a', 'b', 'c'], 2)
    >>> d
    {'a': {'b': {'c': 2}}}
    >>> d = {}
    >>> docpath_set(d, ['a'], 2)
    >>> d
    {'a': 2}
    >>> d = {}
    >>> docpath_set(d, ['a', 'b'], 2)
    Traceback (most recent call last):
    ...
    KeyError: 'a'
    >>> d
    {}
    >>> docpath_set(d, [], 2)
    Traceback (most recent call last):
    ...
    ValueError: Cannot set a value to an empty path
    """
    if not path:
        raise ValueError("Cannot set a value to an empty path")

    d = doc
    for part in path[:-1]:
        d = d[part]

    d[path[-1]] = value


def make_paths_relative(
    doc: Dict, base_directory: PurePath, allow_paths_outside_base=False
):
    """
    Find all pathlib.Path values in a document structure and make them relative to the given path.

    >>> base = PurePath('/tmp/basket')
    >>> doc = {'id': 1, 'fruits': [{'apple': PurePath('/tmp/basket/fruits/apple.txt')}]}
    >>> make_paths_relative(doc, base)
    >>> doc
    {'id': 1, 'fruits': [{'apple': 'fruits/apple.txt'}]}
    >>> # No change if repeated. (relative paths still relative)
    >>> previous = deepcopy(doc)
    >>> make_paths_relative(doc, base)
    >>> doc == previous
    True
    >>> # Relative pathlibs also become relative strings for consistency.
    >>> doc = {'villains': PurePath('the-baron.txt')}
    >>> make_paths_relative(doc, base)
    >>> doc
    {'villains': 'the-baron.txt'}
    """
    for doc_path, value in iterutils.research(
        doc, lambda p, k, v: isinstance(v, PurePath)
    ):
        value: Path

        if value.is_absolute():
            if base_directory not in value.parents:
                if not allow_paths_outside_base:
                    raise ValueError(
                        f"Path {value.as_posix()!r} is outside path {base_directory.as_posix()!r} "
                        f"(allow_paths_outside_base={allow_paths_outside_base})"
                    )
                continue
            value = value.relative_to(base_directory)

        docpath_set(doc, doc_path, str(value))


def resolve_absolute_offset(
    dataset_path: Path, offset: str, target_path: Optional[Path] = None
) -> str:
    """
    Expand a filename (offset) relative to the dataset.

    >>> external_metadata_loc = Path('/tmp/target-metadata.yaml')
    >>> resolve_absolute_offset(
    ...     Path('/tmp/great_test_dataset'),
    ...     'band/my_great_band.jpg',
    ...     external_metadata_loc,
    ... )
    '/tmp/great_test_dataset/band/my_great_band.jpg'
    >>> resolve_absolute_offset(
    ...     Path('/tmp/great_test_dataset.tar.gz'),
    ...     'band/my_great_band.jpg',
    ...     external_metadata_loc,
    ... )
    'tar:/tmp/great_test_dataset.tar.gz!band/my_great_band.jpg'
    >>> resolve_absolute_offset(
    ...     Path('/tmp/great_test_dataset.tar'),
    ...     'band/my_great_band.jpg',
    ... )
    'tar:/tmp/great_test_dataset.tar!band/my_great_band.jpg'
    >>> resolve_absolute_offset(
    ...     Path('/tmp/MY_DATASET'),
    ...     'band/my_great_band.jpg',
    ...     Path('/tmp/MY_DATASET/ga-metadata.yaml'),
    ... )
    'band/my_great_band.jpg'
    """
    dataset_path = dataset_path.absolute()

    if target_path:
        # If metadata is stored inside the dataset, keep paths relative.
        if str(target_path.absolute()).startswith(str(dataset_path)):
            return offset
    # Bands are inside a tar file

    if ".tar" in dataset_path.suffixes:
        return "tar:{}!{}".format(dataset_path, offset)
    else:
        return str(dataset_path / offset)
