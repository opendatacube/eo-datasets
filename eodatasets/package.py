# coding=utf-8
from __future__ import absolute_import
import os
import shutil
import logging
import time
from subprocess import check_call
import datetime
import uuid
import socket
from functools import partial

from pathlib import Path

from eodatasets import serialise, verify, metadata
from eodatasets.browseimage import create_dataset_browse_images
import eodatasets.type as ptype


GA_CHECKSUMS_FILE_NAME = 'package.sha1'

_LOG = logging.getLogger(__name__)

_RUNTIME_ID = uuid.uuid1()


def init_locally_processed_dataset(directory, dataset_driver, source_datasets,
                                   software_provenance, uuid_=None):
    """
    Create a blank dataset for a newly created dataset on this machine.

    :type software_provenance: eodatasets.provenance.SoftwareProvenance
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
                software=software_provenance,
                uname=' '.join(os.uname())
            ),
            source_datasets=source_datasets
        )
    )

    return dataset_driver.fill_metadata(md, directory)


def init_existing_dataset(directory, dataset_driver, source_datasets,
                          software_provenance=None, uuid_=None, source_hostname=None):
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
                hostname=source_hostname,
                software=software_provenance
            ),
            source_datasets=source_datasets

        )
    )
    return dataset_driver.fill_metadata(md, directory)


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
                source_file, destination_file
            ]
        )
    else:
        _LOG.info('Copying %r -> %r', source_file, destination_file)
        shutil.copy(source_file, destination_file)

    return destination_path.stat().st_size


def prepare_target_imagery(
        image_directory,
        package_directory,
        translate_path=lambda path: path,
        after_file_copy=lambda source_path, final_path: None,
        compress_imagery=True,
        hard_link=False):
    """
    Copy a directory of files if not already there. Possibly compress images.

    :type translate_path: (Path) -> Path
    :type image_directory: Path
    :type package_directory: Path
    :type after_file_copy: Path -> None
    :type hard_link: bool
    :type compress_imagery: bool
    """
    if not package_directory.exists():
        package_directory.mkdir()

    for source_path in image_directory.iterdir():
        # Skip hidden files
        if source_path.name.startswith('.'):
            continue

        target_path = translate_path(source_path)
        if target_path is None:
            continue

        target_path = ptype.rebase_path(image_directory, package_directory, target_path)

        _copy_file(source_path, target_path, compress_imagery, hard_link=hard_link)

        after_file_copy(source_path, target_path)


class IncompletePackage(Exception):
    """
    Package is incomplete: (eg. Not enough metadata could be found.)
    """
    pass


def _folder_contents_bytes(image_path):
    return _file_size_bytes(image_path.iterdir())


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
    typical_checksum_file = image_path.joinpath(GA_CHECKSUMS_FILE_NAME)
    if typical_checksum_file.exists():
        dataset.checksum_path = typical_checksum_file

    image_paths = list(image_path.iterdir()) if image_path.is_dir() else [image_path]

    validate_metadata(dataset)
    dataset = expand_driver_metadata(dataset_driver, dataset, image_paths)
    return serialise.write_dataset_metadata(image_path, dataset)


def package_dataset(dataset_driver,
                    dataset,
                    image_path,
                    target_path,
                    hard_link=False):
    """
    Package the given dataset folder.

    This includes copying the dataset into a folder, generating
    metadata and checksum files, as well as optionally generating
    a browse image.

    :type hard_link: bool
    :type dataset_driver: eodatasets.drivers.DatasetDriver
    :type dataset: ptype.Dataset
    :type image_path: Path
    :type target_path: Path

    :raises IncompletePackage: If not enough metadata can be extracted from the dataset.
    :return: The generated GA Dataset ID (ga_label)
    :rtype: str
    """
    checksums = verify.PackageChecksum()

    target_path = target_path.absolute()
    image_path = image_path.absolute()

    target_metadata_path = serialise.expected_metadata_path(target_path)
    if target_metadata_path.exists():
        _LOG.info('Already packaged? Skipping %s', target_path)
        return

    _LOG.debug('Packaging %r -> %r', image_path, target_path)
    package_directory = target_path.joinpath('product')

    file_paths = []

    def after_file_copy(source_path, target_path):
        _LOG.debug('%r -> %r', source_path, target_path)
        checksums.add_file(target_path)
        file_paths.append(target_path)

    prepare_target_imagery(
        image_path,
        package_directory,
        translate_path=partial(dataset_driver.translate_path, dataset),
        after_file_copy=after_file_copy,
        hard_link=hard_link
    )

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
