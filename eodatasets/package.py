import os
import shutil
import logging
import time

from subprocess import check_call

from pathlib import Path

from gaip import acquisition
from eodatasets import image, serialise
from eodatasets.metadata import mdf, mtl, adsfolder
import eodatasets.type as ptype


GA_METADATA_FILE_NAME = 'ga-metadata.yaml'

_LOG = logging.getLogger(__name__)


def _to_old_platform_names(satellite, sensor, band_number):
    """
    Translate names to those used by the old LPGS.

    Older gaip code still uses the old names.

    :type band_number: str
    :type satellite: str
    :type sensor: str
    :return: satellite, sensor, band_name
    :rtype (str, str, str)
    """
    # The sensor map uses old lpgs satellite names. Translate them to new LPGS names for now.
    if satellite == 'LANDSAT_5':
        satellite = 'Landsat5'
    if satellite == 'LANDSAT_7':
        satellite = 'Landsat7'

        if sensor == 'ETM':
            sensor = 'ETM+'

        if str(band_number) == '6_vcid_1':
            band_number = '61'
        if str(band_number) == '6_vcid_2':
            band_number = '62'

    return satellite, sensor, band_number


def expand_band_information(satellite, sensor, band_metadata, checksum=True):
    """
    Use the gaip reference table to add per-band metadata if availabe.
    :param satellite: satellite as reported by LPGS (eg. LANDSAT_8)
    :param sensor: sensor as reported by LPGS (eg. OLI_TIRS)
    :type band_metadata: ptype.BandMetadata
    :rtype: ptype.BandMetadata
    """

    satellite, sensor, band_number = _to_old_platform_names(satellite, sensor, band_metadata.number)

    bands = acquisition.SENSORS[satellite]['sensors'][sensor]['bands']

    band = bands.get(band_number)
    if band:
        band_metadata.label = band['desc']
        band_metadata.cell_size = band['resolution']
        band_metadata.type_ = band['type_desc'].lower()

    if checksum:
        _LOG.info('Checksumming band %r', band_metadata.number)
        band_metadata.checksum_md5 = image.calculate_file_md5(band_metadata.path)

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


def create_browse_images(
        d,
        target_directory,
        red_band_id='7', green_band_id='5', blue_band_id='1',
        browse_filename='browse'):
    """

    :type d: ptype.DatasetMetadata
    :type target_directory: Path
    :type red_band_id: str
    :type green_band_id: str
    :type blue_band_id: str
    :type browse_filename: str
    :return:
    """
    if d.image and d.image.bands:
        d.browse = {
            'medium': image.create_browse(
                d.image.bands[red_band_id],
                d.image.bands[green_band_id],
                d.image.bands[blue_band_id],
                target_directory / (browse_filename + '.jpg'),
                constrain_horizontal_res=1024
            ),
            'full': image.create_browse(
                d.image.bands[red_band_id],
                d.image.bands[green_band_id],
                d.image.bands[blue_band_id],
                target_directory / (browse_filename + '.fr.jpg')
            )
        }
    return d


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


def prepare_target_imagery(image_directory, package_directory, compress_imagery=True):
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

        size_bytes += _copy_file(source_path, destination_path, compress_imagery)

    return size_bytes


def find_file(path, file_pattern):
    # Crude but effective. TODO: multiple/no result handling.
    return path.glob(file_pattern).next()


def do_package(metadata_extract_fn, image_directory, target_directory, source_datasets=None):

    start = time.time()

    target_path = Path(target_directory).absolute()
    image_path = Path(image_directory).absolute()

    target_metadata_path = target_path / GA_METADATA_FILE_NAME
    if target_metadata_path.exists():
        _LOG.info('Already packaged? Skipping %s', target_path)
        return

    _LOG.debug('Packaging %r -> %r', image_path, target_path)
    if image_path.resolve() != target_path.resolve():
        package_directory = target_path.joinpath('package')
    else:
        package_directory = target_path

    size_bytes = prepare_target_imagery(image_path, package_directory)

    if not target_path.exists():
        target_path.mkdir()

    d = metadata_extract_fn(init_local_dataset(), package_directory)
    d.size_bytes = size_bytes

    d.lineage.source_datasets = source_datasets

    if d.image and d.image.bands:
        for number, band_metadata in d.image.bands.items():
            expand_band_information(d.platform.code, d.instrument.name, band_metadata)

    create_browse_images(d, target_path)

    serialise.write_yaml_metadata(d, target_metadata_path, target_path)

    _LOG.info('Packaged in %.02f: %s', time.time()-start, target_metadata_path)


def generate_ortho_metadata(d, package_directory):
    """
    :type image_directory: str
    :type target_directory: str
    :type source_datasets: dict of (str, ptype.DatasetMetadata)
    :return:
    """

    mtl_path = find_file(package_directory, '*_MTL.txt')
    _LOG.info('Reading MTL %r', mtl_path)

    return mtl.populate_from_mtl(d, mtl_path)


def generate_raw_metadata(d, package_directory):
    d = mdf.extract_md(d, package_directory)
    d = adsfolder.extract_md(d, package_directory)

    # TODO: Antenna coords for groundstation? Heading?
    # TODO: Bands?
    return d


def get_dataset(directory):
    return serialise.read_yaml_metadata(find_file(directory, GA_METADATA_FILE_NAME))


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s')
    import doctest

    doctest.testmod()
    logging.getLogger().setLevel(logging.DEBUG)

    # Package RAW
    raw_ls8_dir = os.path.expanduser('~/ops/inputs/LANDSAT-8.11308/LC81160740742015089ASA00')
    do_package(generate_raw_metadata, raw_ls8_dir, raw_ls8_dir)

    # Package ORTHO, linking to previous RAW.
    ls8_packaged_out = 'out-ls8-test'
    do_package(
        generate_ortho_metadata,
        os.path.expanduser('~/ops/inputs/LS8_PINKMATTER_OUT'),
        ls8_packaged_out,
        source_datasets={'raw': get_dataset(Path(raw_ls8_dir))}
    )

    # Package NBAR, linking to previous ORTHO
    def do_no_metadata(d, package_directory):
        print 'No NBAR metadata yet'
        print d, package_directory
        return d

    do_package(
        do_no_metadata,
        os.path.expanduser('~/ops/inputs/nbar_out'),
        'out-nbar-test',
        source_datasets={
            'ortho': get_dataset(Path(ls8_packaged_out))
        }
    )

