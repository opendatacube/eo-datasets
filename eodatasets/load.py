
import os
import shutil
import logging
from subprocess import check_call

import pathlib
import yaml
from pathlib import Path

from gaip import acquisition

from eodatasets import image
from eodatasets.read import mdf, mtl
import eodatasets.type as ptype


_LOG = logging.getLogger(__name__)


def expand_band_information(satellite, sensor, band_metadata):
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


def create_browse_images(d, target_directory):
    # Create browse
    # TODO: Full resolution too?
    d.browse = {
        'medium': image.create_browse(
            d.image.bands['7'],
            d.image.bands['5'],
            d.image.bands['1'],
            target_directory / 'thumb.jpg'
        )
    }

    return d




def write_yaml_metadata(d, target_directory, metadata_file):
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


def transfer_target_imagery(image_directory, package_directory, compress_imagery=True):
    """

    :type image_directory: pathlib.Path
    :type package_directory: pathlib.Path
    :return:
    """
    # TODO: Handle partial packages existing.
    if not package_directory.exists():
        package_directory.mkdir()

    # Loop through files, copy them.
    # If file is tif, use translate instead.

    for source_path in image_directory.iterdir():
        if source_path.name.startswith('.'):
            continue

        destination_path = package_directory.joinpath(source_path.name)

        if destination_path.exists() and (destination_path.stat().st_size == source_path.stat().st_size):
            continue

        source_file = str(source_path)
        destination_file = str(destination_path)

        suffix = source_path.suffix.lower()

        # If a tif image, copy losslessly compressed.
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


def package(image_directory, target_directory, source_datasets=None):
    # TODO: If image directory is not inside target directory, copy images.

    # TODO: If copying the image, why not compress it?

    target_path = Path(target_directory).absolute()
    image_path = Path(image_directory)

    package_directory = target_path.joinpath('package')

    transfer_target_imagery(image_path, package_directory)

    mtl_file = next(Path(package_directory).glob('*_MTL.txt'))  # Crude but effective

    _LOG.info('Reading MTL %r', mtl_file)

    d = init_local_dataset()
    d = mtl.populate_from_mtl(d, mtl_file)

    if not target_path.exists():
        target_path.mkdir()

    create_browse_images(d, target_path)

    for number, band_metadata in d.image.bands.items():
        expand_band_information(d.platform.code, d.instrument.name, band_metadata)

    d.lineage.source_datasets = source_datasets

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
    package(os.path.expanduser('~/ops/inputs/LS8_something'), 'out-ls8-test')
    _LOG.info('Packaged in %r', time.time()-start)

    # package(os.path.expanduser('~/ops/inputs/lpgsOut/LE7_20150202_091_075'), 'out-ls7-test')
