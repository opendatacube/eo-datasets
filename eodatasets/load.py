import os
import shutil
import logging
from subprocess import check_call

import pathlib
import yaml
from pathlib import Path

from gaip import acquisition
from eodatasets import image
from eodatasets.metadata import mdf, mtl, adsfolder
import eodatasets.type as ptype


_LOG = logging.getLogger(__name__)


def expand_band_information(satellite, sensor, band_metadata, checksum=True):
    """
    Use the gaip reference table to add per-band metadata if availabe.
    :param satellite: satellite as reported by LPGS (eg. LANDSAT_8)
    :param sensor: sensor as reported by LPGS (eg. OLI_TIRS)
    :type band_metadata: ptype.BandMetadata
    :rtype: ptype.BandMetadata
    """
    bands = acquisition.SENSORS[satellite]['sensors'][sensor]['bands']

    band = bands.get(band_metadata.number)
    if band:
        band_metadata.label = band['desc']
        band_metadata.cell_size = band['resolution']
        band_metadata.type = band['type_desc'].lower()

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


def write_yaml_metadata(d, metadata_file, target_directory):
    """
    Write the given dataset to yaml.

    All 'Path' values are converted to relative paths: relative to the given
    target directroy.

    :type d: ptype.DatasetMetadata
    :type target_directory: str
    :type metadata_file: str
    """
    _LOG.info('Writing metadata file %r', metadata_file)
    with open(str(metadata_file), 'w') as f:
        ptype.yaml.dump(
            d,
            f,
            default_flow_style=False,
            indent=4,
            Dumper=create_relative_dumper(target_directory),
            allow_unicode=True
        )


def _copy_file(source_path, destination_path, compress_imagery=True):
    """
    Copy a file from source to destination if needed. Maybe apply compression.

    (it's generally faster to compress during a copy operation than as a separate step)

    :type source_path: Path
    :type destination_path: Path
    :type compress_imagery: bool
    :return: Size in bytes of destionation file.
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

    suffix = source_path.suffix.lower()

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
    # TODO: Handle partial packages existing.
    if not package_directory.exists():
        package_directory.mkdir()

    size_bytes = 0
    for source_path in image_directory.iterdir():
        if source_path.name.startswith('.'):
            continue

        destination_path = package_directory.joinpath(source_path.name)

        size_bytes += _copy_file(source_path, destination_path, compress_imagery)

    return size_bytes


def package(image_directory, target_directory, source_datasets=None):
    """

    :param image_directory:
    :param target_directory:
    :param source_datasets:
    :return:
    """
    target_path = Path(target_directory).absolute()
    image_path = Path(image_directory).absolute()

    package_directory = target_path.joinpath('package')

    size_bytes = prepare_target_imagery(image_path, package_directory)

    mtl_path = next(package_directory.glob('*_MTL.txt'))  # Crude but effective

    _LOG.info('Reading MTL %r', mtl_path)

    d = init_local_dataset()
    d = mtl.populate_from_mtl(d, mtl_path)
    d.size_bytes = size_bytes

    if not target_path.exists():
        target_path.mkdir()

    create_browse_images(d, target_path)

    for number, band_metadata in d.image.bands.items():
        expand_band_information(d.platform.code, d.instrument.name, band_metadata)

    d.lineage.source_datasets = source_datasets

    write_yaml_metadata(d, target_path, target_path / 'ga-metadata.yaml')


def package_nbar(image_directory, target_directory, source_datasets=None):
    # copy datasets.
    # Load bands

    # Generic package function: Pass a DatasetMetadata, image folder, browse bands.
    # Method hashes and generates browse.
    pass


def package_raw(image_directory, target_directory):
    image_path = Path(image_directory).absolute()
    target_path = Path(target_directory).absolute()

    # We don't need to modify/copy anything. Just generate metadata.
    d = init_local_dataset()
    d = mdf.extract_md(d, image_path)
    d = adsfolder.extract_md(d, image_path)
    d.size_bytes = prepare_target_imagery(image_path, target_path)

    # TODO: Bands?
    # TODO: Antenna coords for groundstation? Heading?

    write_yaml_metadata(d, target_path, target_path / 'ga-metadata.yaml')


def create_relative_dumper(folder):
    class RelativeDumper(yaml.Dumper):
        pass

    def path_representer(dumper, data):
        """
        :type dumper: BaseRepresenter
        :type data: pathlib.Path
        :rtype: yaml.nodes.Node
        """
        return dumper.represent_scalar(u'tag:yaml.org,2002:str', str(data.relative_to(folder)))

    RelativeDumper.add_multi_representer(pathlib.Path, path_representer)

    return RelativeDumper


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s')
    import doctest

    doctest.testmod()
    logging.getLogger().setLevel(logging.DEBUG)

    import time

    start = time.time()
    raw_ls8_dir = os.path.expanduser('~/ops/inputs/LANDSAT-8.11308/LC81160740742015089ASA00')
    package_raw(raw_ls8_dir, raw_ls8_dir)
    _LOG.info('Packaged RAW in %r', time.time() - start)

    # TODO: Link raw as source of  ortho.

    start = time.time()
    package(os.path.expanduser('~/ops/inputs/LS8_something'), 'out-ls8-test')
    _LOG.info('Packaged ORTHO in %r', time.time() - start)

    # package(os.path.expanduser('~/ops/inputs/lpgsOut/LE7_20150202_091_075'), 'out-ls7-test')
