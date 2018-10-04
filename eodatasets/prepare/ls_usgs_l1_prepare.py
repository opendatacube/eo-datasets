"""
Ingest data from the command-line.
"""
from __future__ import absolute_import

import hashlib
import logging
import os
import re
import tarfile
import uuid
from datetime import datetime
from pathlib import Path

import click
import yaml
from osgeo import osr

from . import serialise

try:
    # flake8 doesn't recognise type hints as usage
    from typing import List, Optional, Union, Iterable, Dict, Tuple  # noqa: F401
except ImportError:
    pass

LANDSAT_8_BANDS = [
    ('1', 'coastal_aerosol'),
    ('2', 'blue'),
    ('3', 'green'),
    ('4', 'red'),
    ('5', 'nir'),
    ('6', 'swir1'),
    ('7', 'swir2'),
    ('8', 'panchromatic'),
    ('9', 'cirrus'),
    ('10', 'lwir1'),
    ('11', 'lwir2'),
    ('QUALITY', 'quality'),
]

TIRS_ONLY = LANDSAT_8_BANDS[9:12]
OLI_ONLY = [*LANDSAT_8_BANDS[0:9], LANDSAT_8_BANDS[11]]

LANDSAT_BANDS = [
    ('1', 'blue'),
    ('2', 'green'),
    ('3', 'red'),
    ('4', 'nir'),
    ('5', 'swir1'),
    ('7', 'swir2'),
    ('QUALITY', 'quality'),
]

MTL_PAIRS_RE = re.compile(r'(\w+)\s=\s(.*)')


def _parse_value(s):
    # type: (str) -> Union[int, float, str]
    """
    >>> _parse_value("asdf")
    'asdf'
    >>> _parse_value("123")
    123
    >>> _parse_value("3.14")
    3.14
    """
    s = s.strip('"')
    for parser in [int, float]:
        try:
            return parser(s)
        except ValueError:
            pass
    return s


def find_in(path, s, suffix='txt'):
    # type: (Path, str, str) -> Optional[Path]
    """Recursively find any file with a certain string in its name

    Search through `path` and its children for the first occurance of a
    file with `s` in its name. Returns the path of the file or `None`.
    """

    def matches(p):
        # type: (Path) -> bool
        return s in p.name and p.name.endswith(suffix)

    if path.is_file():
        return path if matches(path) else None

    for root, _, files in os.walk(str(path)):
        for f in files:
            p = Path(root) / f
            if matches(p):
                return p
    return None


def _parse_group(lines, key_transform=lambda s: s.lower()):
    # type: (Iterable[Union[str, bytes]]) -> dict
    tree = {}

    for line in lines:
        # If line is bytes-like convert to str
        if isinstance(line, bytes):
            line = line.decode('utf-8')
        match = MTL_PAIRS_RE.findall(line)
        if match:
            key, value = match[0]
            if key == 'GROUP':
                tree[key_transform(value)] = _parse_group(lines)
            elif key == 'END_GROUP':
                break
            else:
                tree[key_transform(key)] = _parse_value(value)
    return tree


def get_geo_ref_points(info):
    # type: (Dict) -> Dict
    return {
        'ul': {'x': info['corner_ul_projection_x_product'], 'y': info['corner_ul_projection_y_product']},
        'ur': {'x': info['corner_ur_projection_x_product'], 'y': info['corner_ur_projection_y_product']},
        'll': {'x': info['corner_ll_projection_x_product'], 'y': info['corner_ll_projection_y_product']},
        'lr': {'x': info['corner_lr_projection_x_product'], 'y': info['corner_lr_projection_y_product']},
    }


def get_coords(geo_ref_points, epsg_code):
    # type: (Dict, int) -> Dict

    spatial_ref = osr.SpatialReference()
    spatial_ref.ImportFromEPSG(epsg_code)
    t = osr.CoordinateTransformation(spatial_ref, spatial_ref.CloneGeogCS())

    def transform(p):
        lon, lat, z = t.TransformPoint(p['x'], p['y'])
        return {'lon': lon, 'lat': lat}

    return {key: transform(p) for key, p in geo_ref_points.items()}


def get_satellite_band_names(sat, instrument, file_name):
    # type: (str, str, str) -> List[Tuple[str, str]]
    """
    To load the band_names for referencing either LANDSAT8 or LANDSAT7 or LANDSAT5 bands
    Landsat7 and Landsat5 have same band names
    """

    name_len = file_name.split('_')
    if sat == 'LANDSAT_8':
        if instrument == 'TIRS':
            sat_img = TIRS_ONLY
        elif instrument == 'OLI':
            sat_img = OLI_ONLY
        else:
            sat_img = LANDSAT_8_BANDS
    elif len(name_len) > 7:
        sat_img = LANDSAT_BANDS
    else:
        sat_img = LANDSAT_BANDS[:6]
    return sat_img


def get_mtl_content(acquisition_path):
    # type: (Path) -> Tuple[Dict, str]
    """
    Path is pointing to the folder , where the USGS Landsat scene list in MTL format is downloaded
    from Earth Explorer or GloVis
    """
    if acquisition_path.is_file() and tarfile.is_tarfile(str(acquisition_path)):
        with tarfile.open(str(acquisition_path), 'r') as tp:
            try:
                internal_file = next(filter(lambda memb: 'MTL' in memb.name, tp.getmembers()))
                filename = Path(internal_file.name).stem
                with tp.extractfile(internal_file) as fp:
                    mtl_tree = _parse_group(fp)['l1_metadata_file']
            except StopIteration:
                raise RuntimeError(
                    "MTL file not found in {}".format(str(acquisition_path))
                )
    else:
        path = find_in(acquisition_path, 'MTL')
        if not path:
            raise RuntimeError("No MTL file")

        filename = Path(path).stem
        with path.open('r') as fp:
            mtl_tree = _parse_group(fp)['l1_metadata_file']

    return mtl_tree, filename


def prepare_dataset(path):
    # type: (Path) -> Optional[Dict]
    mtl_doc, mtl_filename = get_mtl_content(path)

    if not mtl_doc:
        return None

    data_format = mtl_doc['product_metadata']['output_format']
    if data_format.upper() == 'GEOTIFF':
        data_format = 'GeoTIFF'

    epsg_code = 32600 + mtl_doc['projection_parameters']['utm_zone']

    geo_ref_points = get_geo_ref_points(mtl_doc['product_metadata'])

    band_mappings = get_satellite_band_names(
        mtl_doc['product_metadata']['spacecraft_id'],
        mtl_doc['product_metadata']['sensor_id'],
        mtl_filename
    )

    return {
        'id': str(uuid.uuid5(uuid.NAMESPACE_URL, path.as_posix())),
        'product_type': 'level1',
        'extent': {
            'center_dt': '{} {}'.format(
                mtl_doc['product_metadata']['date_acquired'],
                mtl_doc['product_metadata']['scene_center_time']
            ),
            'coord': get_coords(geo_ref_points, epsg_code),
        },
        'format': {'name': data_format},
        'grid_spatial': {
            'projection': {
                'geo_ref_points': geo_ref_points,
                'spatial_reference': 'EPSG:%s' % epsg_code,
            }
        },
        'image': {
            'bands': {
                band_name: {
                    'path': mtl_doc['product_metadata']['file_name_band_' + band_fname.lower()],
                    'layer': 1,
                } for band_fname, band_name in band_mappings
            }
        },
        'mtl': mtl_doc,
        'lineage': {
            'machine': {
                'software_versions': {

                },
            },
            'source_datasets': {},
        },
    }


def resolve_doc_offset(dataset_path, offset):
    # type: (Path, str) -> str
    """
    Expand a filename (offset) relative to the dataset.

    >>> resolve_doc_offset(Path('/tmp/great_test_dataset/LS7_TESTO_MTL.txt'), 'band/my_great_band.jpg')
    '/tmp/great_test_dataset/band/my_great_band.jpg'
    >>> resolve_doc_offset(Path('/tmp/great_test_dataset.tar.gz'), 'band/my_great_band.jpg')
    'tar:/tmp/great_test_dataset.tar.gz!band/my_great_band.jpg'
    >>> resolve_doc_offset(Path('/tmp/uh_oh'), 'band/my_great_band.jpg')
    Traceback (most recent call last):
    ...
    NotImplementedError: Unexpected dataset path structure /tmp/uh_oh
    """
    if '.tar' in dataset_path.suffixes:
        return 'tar:{}!{}'.format(dataset_path, offset)
    elif dataset_path.suffix == '.txt':
        return str(dataset_path.parent.absolute() / offset)
    else:
        raise NotImplementedError("Unexpected dataset path structure {}".format(dataset_path))


def find_gz_mtl(ds_path, output_folder):
    # type: (Path, Path) -> Optional[Path]
    """
    Find the MTL metadata file for the archived dataset and extract the xml
    file and store it temporally in output folder

    :param Path ds_path: the dataset path
    :param Path output_folder: the output folder

    :returns: xml with full path

    """
    mtl_pattern = re.compile(r"_MTL\.txt$")
    with tarfile.open(str(ds_path), 'r') as tar_gz:
        members = [m for m in tar_gz.getmembers() if mtl_pattern.search(m.name)]
        if not members:
            return None
        tar_gz.extractall(output_folder, members)

    return output_folder / members[0].name


def yaml_checkums_correctly(output_yaml, data_path):
    with output_yaml.open() as yaml_f:
        logging.info("Running checksum comparison")
        # It can match any dataset in the yaml.
        for doc in yaml.safe_load_all(yaml_f):
            yaml_sha1 = doc['checksum_sha1']
            checksum_sha1 = hashlib.sha1(data_path.open('rb').read()).hexdigest()
            if checksum_sha1 == yaml_sha1:
                return True

    return False


@click.command(help="""\b
                    Prepare USGS Landsat Collection 1 data for ingestion into the Data Cube.
                    This prepare script supports only for MTL.txt metadata file
                    To Set the Path for referring the datasets -
                    Download the  Landsat scene data from Earth Explorer or GloVis into
                    'some_space_available_folder' and unpack the file.
                    For example: yourscript.py --output [Yaml- which writes datasets into this file for indexing]
                    [Path for dataset as : /home/some_space_available_folder/]""")
@click.option('--output', help="Write output into this directory",
              required=True,
              type=click.Path(exists=True, writable=True, dir_okay=True, file_okay=False))
@click.argument('datasets',
                type=click.Path(exists=True, readable=True, writable=False),
                nargs=-1)
@click.option(
    '--newer-than',
    type=serialise.ClickDatetime(),
    default=None,
    help="Only prepare files newer than this date"
)
@click.option(
    '--checksum/--no-checksum', 'check_checksum',
    help="Checksum the input dataset to confirm match",
    default=False
)
@click.option(
    '--absolute-paths/--relative-paths', 'force_absolute_paths',
    help="Embed absolute paths in the metadata document (not recommended)",
    default=False
)
def main(output, datasets, check_checksum, force_absolute_paths, newer_than):
    # type: (str, List[str], bool, bool, datetime) -> None

    output = Path(output)

    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                        level=logging.INFO)

    for ds in datasets:
        ds_path = Path(ds)
        (mode, ino, dev, nlink, uid, gid, size, atime, mtime, ctime) = os.stat(ds)
        create_date = datetime.utcfromtimestamp(ctime)
        if newer_than and (create_date <= newer_than):
            logging.info(
                "Creation time {} older than start date {:%Y-%m-%d %H:%M} ...SKIPPING {}".format(
                    newer_than - create_date, newer_than, ds_path.name
                )
            )
            continue

        if ds_path.suffix == '.txt':
            mtl_path = ds_path
        elif '.tar' in ds_path.suffixes:
            mtl_path = find_gz_mtl(ds_path, output)
            if not mtl_path:
                raise RuntimeError('No MTL file within tarball %s' % ds_path)
        else:
            logging.warning("Unreadable dataset %s", ds_path)
            continue

        logging.info("Processing %s", ds_path)
        output_yaml = output / '{}.yaml'.format(_dataset_name(ds_path))

        logging.info("Output %s", output_yaml)
        if output_yaml.exists():
            logging.info("Output already exists %s", output_yaml)
            if check_checksum and yaml_checkums_correctly(output_yaml, ds_path):
                logging.info("Dataset preparation already done...SKIPPING %s", ds_path.name)
                continue

        prepare_and_write(ds_path, mtl_path, output_yaml, use_absolute_paths=force_absolute_paths)

    # delete intermediate MTL files for archive datasets in output folder
    output_mtls = list(output.rglob('*MTL.txt'))
    for mtl_path in output_mtls:
        try:
            mtl_path.unlink()
        except OSError:
            pass


def prepare_and_write(ds_path,
                      mtl_path,
                      output_yaml_path,
                      use_absolute_paths=False):
    # type: (Path, Path, Path, bool) -> None
    doc = prepare_dataset(mtl_path)

    if use_absolute_paths:
        for band in doc['image']['bands'].values():
            band['path'] = resolve_doc_offset(ds_path, band['path'])

    serialise.dump_yaml(output_yaml_path, doc)


def _dataset_name(ds_path):
    # type: (Path) -> str
    """
    >>> _dataset_name(Path("example/LE07_L1GT_104078_20131209_20161119_01_T1.tar.gz"))
    'LE07_L1GT_104078_20131209_20161119_01_T1'
    >>> _dataset_name(Path("example/LE07_L1GT_104078_20131209_20161119_01_T2/SOME_TEST_MTL.txt"))
    'LE07_L1GT_104078_20131209_20161119_01_T2'
    """
    if '.tar' in ds_path.suffixes:
        return ds_path.name.split('.')[0]
    elif ds_path.name.lower().endswith('_mtl.txt'):
        return ds_path.parent.name
    else:
        raise NotImplementedError("Unexpected path pattern {}".format(ds_path))


if __name__ == "__main__":
    main()
