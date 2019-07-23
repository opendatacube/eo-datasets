# coding=utf-8
"""
Common methods for UI code.
"""
from __future__ import absolute_import

import gzip
import json
from pathlib import Path
from typing import Generator, Dict, Tuple

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
