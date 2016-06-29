# coding=utf-8
"""
Higher-level commands to package directories on the filesystem.
"""
from __future__ import absolute_import

import logging
import shutil
import tempfile
from contextlib import contextmanager

from pathlib import Path

from eodatasets import package, serialise

_LOG = logging.getLogger(__name__)


def package_newly_processed_data_folder(driver, input_data_paths, destination_path, parent_dataset_paths,
                                        metadata_expand_fn=None,
                                        hard_link=False,
                                        additional_files=None):
    """
    Package an input folder. This is assumed to have just been packaged on the current host.

    (we record host and date information)

    If this is an older dataset, use `package_existing_data_folder` instead.

    The package is created inside the given destination directory, named using
    the ga_label ("dataset id").

    Output is moved into place atomically once fully written.

    Set hard_link=True to hard link instead of copy unmodified files to the output directory (a large speedup)

    :type driver: eodatasets.drivers.DatasetDriver
    :type input_data_paths: list[pathlib.Path]
    :type destination_path: pathlib.Path
    :type metadata_expand_fn: (eodatasets.type.DatasetMetadata) -> None
    :type parent_dataset_paths: list[pathlib.Path]
    :type hard_link: bool

    :param additional_files: Additional files to record in the package.
    :type additional_files: list[Path]
    """
    return _package_folder(
        driver, input_data_paths, destination_path,
        _source_datasets_from_paths(driver, parent_dataset_paths),
        package.init_locally_processed_dataset,
        hard_link=hard_link,
        metadata_expand_fn=metadata_expand_fn,
        additional_files=additional_files
    )


def package_existing_data_folder(driver, input_data_paths, destination_path, parent_dataset_paths,
                                 metadata_expand_fn=None,
                                 additional_files=None,
                                 hard_link=False):
    """
    Package an input folder of possibly unknown origin.

    The package is created inside the given destination directory, named using
    the ga_label ("dataset id").

    Output is moved into place atomically once fully written.

    Set hard_link=True to hard link instead of copy unmodified files to the output directory (a large speedup)

    :type driver: eodatasets.drivers.DatasetDriver
    :type input_data_paths: list[pathlib.Path]
    :type destination_path: pathlib.Path
    :type metadata_expand_fn: (eodatasets.type.DatasetMetadata) -> None
    :type parent_dataset_paths: list[pathlib.Path]

    :param additional_files: Additional files to record in the package.
    :type additional_files: tuple[Path]

    :type hard_link: bool
    :return:
    """
    return _package_folder(
        driver, input_data_paths, destination_path,
        _source_datasets_from_paths(driver, parent_dataset_paths),
        package.init_existing_dataset,
        hard_link=hard_link,
        metadata_expand_fn=metadata_expand_fn,
        additional_files=additional_files
    )


def _source_datasets_from_paths(driver, parent_dataset_paths):
    parent_datasets = {}
    for parent in parent_dataset_paths:
        metadata = serialise.read_dataset_metadata(parent)
        source_id = metadata.product_type
        parent_datasets.update({source_id: metadata})
    return parent_datasets


def _package_folder(driver, input_data_paths, destination_path, source_datasets,
                    init_dataset,
                    metadata_expand_fn=None,
                    hard_link=True,
                    additional_files=None):
    """
    Package a folder into a destination directory as the dataset id. The output is written atomically.

    Output is moved into place atomically once fully written.

    :type driver: eodatasets.drivers.DatasetDriver
    :type input_data_paths: list[pathlib.Path]
    :type destination_path: pathlib.Path
    :type source_datasets: dict[str, eodatasets.type.DatasetMetadata]
    :type metadata_expand_fn: (eodatasets.type.DatasetMetadata) -> None
    :type init_dataset: callable
    :type hard_link: bool

    :param additional_files: Additional files to record in the package.
    :type additional_files: tuple[Path]

    :return: list of (created packages, already existing packages)
    """
    created_packages = []
    existing_packages = []

    for dataset_folder in input_data_paths:
        dataset_folder = Path(dataset_folder)

        with temp_dir(prefix='.packagetmp.', base_dir=destination_path) as temp_output_dir:
            dataset = init_dataset(dataset_folder, source_datasets)
            if metadata_expand_fn is not None:
                metadata_expand_fn(dataset)

            dataset_id = package.package_dataset(  # Also updates dataset
                dataset_driver=driver,
                dataset=dataset,
                image_path=dataset_folder,
                target_path=temp_output_dir,
                hard_link=hard_link,
                additional_files=additional_files
            )

            # Output package permissions should match the parent dir.
            shutil.copymode(str(destination_path), str(temp_output_dir))
            packaged_path = destination_path / dataset_id

            if packaged_path.exists():
                _LOG.warning('Package already exists: %r', packaged_path)
                existing_packages.append(packaged_path)
                shutil.rmtree(temp_output_dir, ignore_errors=True)
            else:
                # Move finished folder into place.
                temp_output_dir.rename(packaged_path)
                created_packages.append(packaged_path)
                _LOG.info('Completed package %r', packaged_path)

    return created_packages, existing_packages


@contextmanager
def temp_dir(prefix="", base_dir=None):
    temp_output_dir = Path(tempfile.mkdtemp(prefix=prefix, dir=str(base_dir)))
    try:
        yield Path(temp_output_dir)
    finally:
        # Clean up if still exists
        with ignored(OSError):
            shutil.rmtree(str(temp_output_dir), ignore_errors=True)


@contextmanager
def ignored(*exceptions):
    try:
        yield
    except exceptions:
        pass
