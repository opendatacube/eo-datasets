"""
Common methods for UI code.
"""

import gzip
import json
import os
import posixpath
from pathlib import Path, PurePath
from typing import Dict, Generator, Tuple
from urllib.parse import urlparse

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
        return dataset_path.parent.joinpath(f"{dataset_path.name}.ga-md.yaml")

    raise ValueError(f"Unhandled path type for {dataset_path!r}")


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
        raise ValueError(f"Multiple matched metadata files: {existing_paths!r}")

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

    >>> from copy import deepcopy
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
        value: PurePath
        value = relative_path(
            value, base_directory, allow_paths_outside_base=allow_paths_outside_base
        )
        docpath_set(doc, doc_path, value.as_posix())


def relative_url(value: str, base: str, allow_paths_outside_base=False) -> str:
    """
    Make a single url relative to the base url if it is inside it.

    By default, will throw a ValueError if not able to make it relative to the path.


    >>> relative_url('file:///g/data/v10/0/2015/blue.jpg', 'file:///g/data/v10/0/2015/odc-metadata.yaml')
    'blue.jpg'
    >>> relative_url('https://example.test/2015/images/blue.jpg', 'https://example.test/2015/odc-metadata.yaml')
    'images/blue.jpg'
    >>> relative_url('file:///g/data/v10/0/2018/blue.jpg', 'file:///g/data/v10/0/2015/odc-metadata.yaml')
    Traceback (most recent call last):
      ...
    ValueError: Path 'file:///g/data/v10/0/2018/blue.jpg' is outside path 'file:///g/data/v10/0/2015/odc-metadata.yaml'\
 (allow_paths_outside_base=False)
    """

    if not value:
        return value

    if not value.startswith(base) and not value.startswith(os.path.dirname(base)):
        if not allow_paths_outside_base:
            raise ValueError(
                f"Path {value!r} is outside path {base!r} "
                f"(allow_paths_outside_base={allow_paths_outside_base})"
            )
        return value

    return _make_relurl(value, base)


def _make_relurl(target: str, base: str) -> str:
    base = urlparse(base)
    target = urlparse(target)
    if base.netloc != target.netloc:
        raise ValueError("target and base netlocs do not match")
    base_dir = "." + posixpath.dirname(base.path)
    target = "." + target.path
    return posixpath.relpath(target, start=base_dir)


def relative_path(
    value: PurePath, base_directory: PurePath, allow_paths_outside_base=False
) -> PurePath:
    """
    Make a single path relative to the base directory if it is inside it.

    By default, will throw a ValueError if not able to make it relative to the path.

    >>> val =  PurePath('/tmp/minimal-pkg/loch_ness_sightings_2019-07-04_blue.tif')
    >>> base = PurePath('/tmp/minimal-pkg')
    >>> relative_path(val, base).as_posix()
    'loch_ness_sightings_2019-07-04_blue.tif'
    """
    if not value or not value.is_absolute():
        return value

    if base_directory not in value.parents:
        if not allow_paths_outside_base:
            raise ValueError(
                f"Path {value.as_posix()!r} is outside path {base_directory.as_posix()!r} "
                f"(allow_paths_outside_base={allow_paths_outside_base})"
            )
        return value
    return value.relative_to(base_directory)
