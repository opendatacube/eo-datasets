# coding=utf-8
"""
Higher-level commands to package directories on the filesystem.
"""
from __future__ import absolute_import

import os
import tempfile

from pathlib import Path
from eodatasets import package, serialise


def package_newly_processed_data_folder(driver, input_data_paths, destination_path, parent_dataset_paths,
                                        hard_link=False):
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
    :type parent_dataset_paths: list[pathlib.Path]
    :type hard_link: bool
    :return:
    """
    return _package_folder(
        driver, input_data_paths, destination_path, parent_dataset_paths,
        package.init_locally_processed_dataset,
        hard_link=hard_link
    )


def package_existing_data_folder(driver, input_data_paths, destination_path, parent_dataset_paths, hard_link=False):
    """
    Package an input folder of possibly unknown origin.

    The package is created inside the given destination directory, named using
    the ga_label ("dataset id").

    Output is moved into place atomically once fully written.

    Set hard_link=True to hard link instead of copy unmodified files to the output directory (a large speedup)

    :type driver: eodatasets.drivers.DatasetDriver
    :type input_data_paths: list[pathlib.Path]
    :type destination_path: pathlib.Path
    :type parent_dataset_paths: list[pathlib.Path]
    :type hard_link: bool
    :return:
    """
    return _package_folder(
        driver, input_data_paths, destination_path, parent_dataset_paths,
        package.init_existing_dataset,
        hard_link=hard_link
    )


def _package_folder(driver, input_data_paths, destination_path, parent_dataset_paths,
                    init_dataset,
                    hard_link=True):
    """
    Package a folder into a destination directory as the dataset id. The output is written atomically.

    Output is moved into place atomically once fully written.

    :type driver: eodatasets.drivers.DatasetDriver
    :type input_data_paths: list[pathlib.Path]
    :type destination_path: pathlib.Path
    :type parent_dataset_paths: list[pathlib.Path]
    :type init_dataset: callable
    :type hard_link: bool
    :return:
    """
    parent_datasets = {}

    # TODO: Multiple parents?
    if parent_dataset_paths:
        source_id = driver.expected_source().get_id()
        parent_datasets.update({source_id: serialise.read_dataset_metadata(parent_dataset_paths[0])})

    for dataset_folder in input_data_paths:
        temp_output_dir = Path(tempfile.mkdtemp(prefix='.packagetmp.', dir=str(destination_path)))

        dataset = init_dataset(dataset_folder, driver, parent_datasets)

        dataset_id = package.package_dataset(
            driver,
            dataset,
            dataset_folder,
            temp_output_dir,
            hard_link=hard_link
        )

        os.rename(str(temp_output_dir), str(destination_path / dataset_id))
