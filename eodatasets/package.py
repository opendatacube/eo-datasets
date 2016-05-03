# coding=utf-8
from __future__ import absolute_import

import datetime
import logging
import os
import shutil
import socket
import uuid
from functools import partial
from subprocess import check_call

from pathlib import Path

import eodatasets
import eodatasets.type as ptype
from eodatasets import serialise, verify, metadata, documents
from eodatasets.browseimage import create_dataset_browse_images

GA_CHECKSUMS_FILE_NAME = 'package.sha1'

_LOG = logging.getLogger(__name__)

_RUNTIME_ID = uuid.uuid1()


def init_locally_processed_dataset(directory, source_datasets, uuid_=None):
    """
    Create a blank dataset for a newly created dataset on this machine.

    :param uuid_: The existing dataset_id, if any.
    :rtype: ptype.DatasetMetadata
    """
    md = ptype.DatasetMetadata(
        id_=uuid_,
        # Default creation time is creation of an image.
        creation_dt=datetime.datetime.utcfromtimestamp(directory.stat().st_ctime),
        lineage=ptype.LineageMetadata(
            machine=ptype.MachineMetadata(
                hostname=socket.getfqdn(),
                runtime_id=_RUNTIME_ID,
                uname=' '.join(os.uname())
            ),
            source_datasets=source_datasets
        )
    )
    md.lineage.machine.note_software_version('eodatasets', eodatasets.__version__)
    return md


def init_existing_dataset(directory, source_datasets, uuid_=None, source_hostname=None):
    """
    Package an existing dataset folder (with mostly unknown provenance).

    This is intended for old datasets where little information was recorded.

    For brand new datasets, it's better to use init_locally_processed_dataset() to capture
    local machine information.

    :param uuid_: The existing dataset_id, if any.
    :param source_hostname: Hostname where processed, if known.
    :rtype: ptype.DatasetMetadata
    """
    md = ptype.DatasetMetadata(
        id_=uuid_,
        # Default creation time is creation of an image.
        creation_dt=datetime.datetime.utcfromtimestamp(directory.stat().st_ctime),
        lineage=ptype.LineageMetadata(
            machine=ptype.MachineMetadata(
                hostname=source_hostname
            ),
            source_datasets=source_datasets
        )
    )
    md.lineage.machine.note_software_version('eodatasets', eodatasets.__version__)
    return md


def package_dataset(dataset_driver,
                    dataset,
                    image_path,
                    target_path,
                    hard_link=False,
                    additional_files=None):
    """
    Package the given dataset folder.

    This includes copying the dataset into a folder, generating
    metadata and checksum files, as well as optionally generating
    a browse image.

    Validates, and *Modifies* the passed in dataset with extra metadata.

    :type hard_link: bool
    :type dataset_driver: eodatasets.drivers.DatasetDriver
    :type dataset: ptype.Dataset
    :type image_path: Path
    :type target_path: Path
    :param additional_files: Additional files to record in the package.
    :type additional_files: tuple[Path]

    :raises IncompletePackage: If not enough metadata can be extracted from the dataset.
    :return: The generated GA Dataset ID (ga_label)
    :rtype: str
    """
    if additional_files is None:
        additional_files = []
    _check_additional_files_exist(additional_files)

    dataset_driver.fill_metadata(dataset, image_path, additional_files=additional_files)

    checksums = verify.PackageChecksum()

    target_path = target_path.absolute()
    image_path = image_path.absolute()

    target_metadata_path = documents.find_metadata_path(target_path)
    if target_metadata_path is not None and target_metadata_path.exists():
        _LOG.info('Already packaged? Skipping %s', target_path)
        return

    _LOG.debug('Packaging %r -> %r', image_path, target_path)
    package_directory = target_path.joinpath('product')

    file_paths = []

    def save_target_checksums_and_paths(source_path, target_path):
        _LOG.debug('%r -> %r', source_path, target_path)
        checksums.add_file(target_path)
        file_paths.append(target_path)

    prepare_target_imagery(
        image_path,
        destination_directory=package_directory,
        include_path=dataset_driver.include_file,
        translate_path=partial(dataset_driver.translate_path, dataset),
        after_file_copy=save_target_checksums_and_paths,
        hard_link=hard_link
    )

    write_additional_files(additional_files, checksums, target_path)

    validate_metadata(dataset)
    dataset = expand_driver_metadata(dataset_driver, dataset, file_paths)

    #: :type: ptype.DatasetMetadata
    dataset = ptype.rebase_paths(image_path, package_directory, dataset)

    create_dataset_browse_images(
        dataset_driver,
        dataset,
        target_path,
        after_file_creation=checksums.add_file
    )

    target_checksums_path = target_path / GA_CHECKSUMS_FILE_NAME
    dataset.checksum_path = target_checksums_path

    target_metadata_path = serialise.write_dataset_metadata(target_path, dataset)

    checksums.add_file(target_metadata_path)
    checksums.write(target_checksums_path)

    return dataset.ga_label


def _check_additional_files_exist(additional_files):
    """
    :type additional_files: tuple[Path]
    """
    # Check that all given additional paths exist.
    for path in additional_files:
        path = path.absolute()
        if not path.is_file():
            raise ValueError('Given file does not exist: %s' % (path,))


def write_additional_files(additional_files, checksums, target_path):
    """
    :type additional_files: tuple[Path]
    :type checksums: eodatasets.verify.PackageChecksum
    :type target_path: pathlib.Path
    """
    additional_directory = target_path.joinpath('additional')
    for path in additional_files:
        target_path = additional_directory.joinpath(path.name)
        if not target_path.parent.exists():
            target_path.parent.mkdir(parents=True)
        shutil.copy(str(path.absolute()), str(target_path))
        checksums.add_file(target_path)


def prepare_target_imagery(
        source_directory,
        destination_directory,
        include_path=lambda path: True,
        translate_path=lambda p: p,
        after_file_copy=lambda source_path, final_path: None,
        compress_imagery=True,
        hard_link=False):
    """
    Copy a directory of files if not already there. Possibly compress images.

    :type translate_path: (Path) -> Path
    :type source_directory: Path
    :type destination_directory: Path
    :type after_file_copy: Path -> None
    :type hard_link: bool
    :type compress_imagery: bool
    """
    if not destination_directory.exists():
        destination_directory.mkdir()

    for source_file in source_directory.rglob('*'):
        # Skip hidden files and directories
        if source_file.name.startswith('.') or source_file.is_dir() or not include_path(source_file):
            continue

        rel_source_file = source_file.relative_to(source_directory)

        rel_target_path = translate_path(rel_source_file)

        absolute_target_path = destination_directory / rel_target_path

        _copy_file(source_file, absolute_target_path, compress_imagery, hard_link=hard_link)

        after_file_copy(source_file, absolute_target_path)


def _copy_file(source_path, destination_path, compress_imagery=True, hard_link=False):
    """
    Copy a file from source to destination if needed. Maybe apply compression.

    (it's generally faster to compress during a copy operation than as a separate step)

    :type source_path: Path
    :type destination_path: Path
    :type compress_imagery: bool
    :type hard_link: bool
    :return: Size in bytes of destination file.
    :rtype int
    """

    source_file = str(source_path)
    destination_file = str(destination_path)

    # Copy to destination path.
    original_suffix = source_path.suffix.lower()
    suffix = destination_path.suffix.lower()

    if destination_path.exists():
        _LOG.info('Destination exists: %r', destination_file)
    elif (original_suffix == suffix) and hard_link:
        _LOG.info('Hard linking %r -> %r', source_file, destination_file)
        os.link(source_file, destination_file)
    # If a tif image, compress it losslessly.
    elif suffix == '.tif' and compress_imagery:
        _LOG.info('Copying compressed %r -> %r', source_file, destination_file)
        check_call(
            [
                'gdal_translate',
                '--config', 'GDAL_CACHEMAX', '512',
                '--config', 'TILED', 'YES',
                '-co', 'COMPRESS=lzw',
                '-co', 'predictor=2',
                source_file, destination_file
            ]
        )
    else:
        _LOG.info('Copying %r -> %r', source_file, destination_file)
        shutil.copy(source_file, destination_file)

    return destination_path.stat().st_size


class IncompletePackage(Exception):
    """
    Package is incomplete: (eg. Not enough metadata could be found.)
    """
    pass


def _file_size_bytes(*file_paths):
    """
    Total file size for the given paths.
    :type file_paths: list[Path]
    :rtype: int
    """
    return sum([p.stat().st_size for p in file_paths])


def validate_metadata(dataset):
    """
    :rtype: ptype.DatasetMetadata
    """
    # TODO: Add proper validation
    if not dataset.platform or not dataset.platform.code:
        raise IncompletePackage('Incomplete dataset. Not enough metadata found: %r' % dataset)


def expand_driver_metadata(dataset_driver, dataset, image_paths):
    """
    :type dataset_driver: eodatasets.drivers.DatasetDriver
    :type dataset: ptype.DatasetMetadata
    :type image_paths: list[Path]
    :rtype: ptype.DatasetMetadata
    :raises IncompletePackage:
        Mot enough metadata can be extracted from the dataset.
    """

    dataset.product_type = dataset_driver.get_id()
    dataset.ga_label = dataset_driver.get_ga_label(dataset)

    dataset.size_bytes = _file_size_bytes(*image_paths)

    if image_paths:
        bands = [dataset_driver.to_band(dataset, path) for path in image_paths]

        if bands:
            if not dataset.image:
                dataset.image = ptype.ImageMetadata()

            dataset.image.bands = {band.number: band for band in bands if band}

    return metadata.expand_common_metadata(dataset)


def package_inplace_dataset(dataset_driver, dataset, image_path):
    """
    Create a metadata file for the given dataset without modifying it.

    :type dataset_driver: eodatasets.drivers.DatasetDriver
    :type dataset: ptype.Dataset
    :type image_path: Path
    :rtype: Path
    :return: Path to the created metadata file.
    """
    dataset_driver.fill_metadata(dataset, image_path)
    typical_checksum_file = image_path.joinpath(GA_CHECKSUMS_FILE_NAME)
    if typical_checksum_file.exists():
        dataset.checksum_path = typical_checksum_file

    image_paths = list(image_path.iterdir()) if image_path.is_dir() else [image_path]

    validate_metadata(dataset)
    dataset = expand_driver_metadata(dataset_driver, dataset, image_paths)
    return serialise.write_dataset_metadata(image_path, dataset)
