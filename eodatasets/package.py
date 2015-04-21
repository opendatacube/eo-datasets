import json
import shutil
import logging
import time
from subprocess import check_call
import datetime

from pathlib import Path

from eodatasets import serialise, verify, drivers
from eodatasets.browseimage import create_dataset_browse_images

import eodatasets.type as ptype


GA_CHECKSUMS_FILE_NAME = 'package.sha1'

_LOG = logging.getLogger(__name__)

# From the gaip codebase. Lookup table for sensor information.
with Path(__file__).parent.joinpath('sensors.json').open() as fo:
    SENSORS = json.load(fo)


def expand_band_information(satellite, sensor, band_metadata):
    """
    Use the gaip reference table to add per-band metadata if available.
    :param satellite: satellite as reported by LPGS (eg. LANDSAT_8)
    :param sensor: sensor as reported by LPGS (eg. OLI_TIRS)
    :type band_metadata: ptype.BandMetadata
    :rtype: ptype.BandMetadata
    """

    bands = SENSORS[satellite]['sensors'][sensor]['bands']

    band = bands.get(band_metadata.number)
    if band:
        band_metadata.label = band['desc']
        band_metadata.cell_size = band['resolution']
        band_metadata.type_ = band['type_desc'].lower()

    return band_metadata


def init_local_dataset(uuid=None):
    """
    Create blank metadata for a newly created dataset on this machine.
    :param uuid: The existing dataset_id, if any.
    :rtype: ptype.DatasetMetadata
    """
    md = ptype.DatasetMetadata(
        id_=uuid,
        lineage=ptype.LineageMetadata(
            machine=ptype.MachineMetadata(),
        )
    )
    return md


def _copy_file(source_path, destination_path, compress_imagery=True):
    """
    Copy a file from source to destination if needed. Maybe apply compression.

    (it's generally faster to compress during a copy operation than as a separate step)

    :type source_path: Path
    :type destination_path: Path
    :type compress_imagery: bool
    :return: Size in bytes of destination file.
    :rtype int
    """
    source_size_bytes = source_path.stat().st_size

    if destination_path.exists():
        if source_path.resolve() == destination_path.resolve():
            return source_size_bytes

        if destination_path.stat().st_size == source_size_bytes:
            return source_size_bytes

    source_file = str(source_path)
    destination_file = str(destination_path)

    # Copy to destination path.

    suffix = destination_path.suffix.lower()

    # If a tif image, losslessly compress it.
    if suffix == '.tif' and compress_imagery:
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


def prepare_target_imagery(image_directory,
                           package_directory,
                           compress_imagery=True,
                           filename_match=None,
                           after_file_copy=lambda file_path: None):
    """
    Copy a directory of files if not already there. Possibly compress images.

    :type image_directory: Path
    :type package_directory: Path
    :return: Total size of imagery in bytes
    :rtype int
    """
    if not package_directory.exists():
        package_directory.mkdir()

    size_bytes = 0
    for source_path in image_directory.iterdir():
        # Skip hidden files and envi headers. (envi files are converted to tif during copy)
        if source_path.name.startswith('.') or source_path.suffix == '.hdr':
            continue

        destination_path = package_directory.joinpath(source_path.name)
        if destination_path.suffix == '.bin':
            destination_path = destination_path.with_suffix('.tif')

        if filename_match and not filename_match(destination_path):
            continue

        size_bytes += _copy_file(source_path, destination_path, compress_imagery)
        after_file_copy(destination_path)

    return size_bytes


def do_package(dataset_driver,
               image_directory,
               target_directory,
               source_datasets=None):
    """
    Package the given dataset folder.
    :type dataset_driver: drivers.DatasetDriver
    :type image_directory: Path or str
    :type target_directory: Path or str
    :type source_datasets: dict of (str, ptype.DatasetMetadata)
    """
    start = time.time()
    checksums = verify.PackageChecksum()

    target_path = Path(target_directory).absolute()
    image_path = Path(image_directory).absolute()

    target_metadata_path = serialise.expected_metadata_path(target_path)
    if target_metadata_path.exists():
        _LOG.info('Already packaged? Skipping %s', target_path)
        return
    target_checksums_path = target_path / GA_CHECKSUMS_FILE_NAME

    _LOG.debug('Packaging %r -> %r', image_path, target_path)
    if image_path.resolve() != target_path.resolve():
        package_directory = target_path.joinpath('package')
    else:
        package_directory = target_path

    size_bytes = prepare_target_imagery(
        image_path,
        package_directory,
        filename_match=dataset_driver.file_is_pertinent,
        after_file_copy=checksums.add_file
    )

    if not target_path.exists():
        target_path.mkdir()

    #: :type: ptype.DatasetMetadata
    d = dataset_driver.fill_metadata(init_local_dataset(), package_directory)
    d.product_type = dataset_driver.get_id()
    d.size_bytes = size_bytes
    d.checksum_path = target_checksums_path
    # Default creation time is creation of the source folder.
    d.creation_dt = datetime.datetime.utcfromtimestamp(image_path.stat().st_ctime)

    d.lineage.source_datasets = source_datasets

    if d.image and d.image.bands:
        for number, band_metadata in d.image.bands.items():
            expand_band_information(d.platform.code, d.instrument.name, band_metadata)

    create_dataset_browse_images(
        dataset_driver,
        d,
        target_path,
        after_file_creation=checksums.add_file
    )

    target_metadata_path = serialise.write_dataset_metadata(target_path, d)
    checksums.add_file(target_metadata_path)

    checksums.write(target_checksums_path)
    _LOG.info('Packaged in %.02f: %s', time.time() - start, target_metadata_path)

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s')
    import doctest

    doctest.testmod()
    logging.getLogger().setLevel(logging.DEBUG)


