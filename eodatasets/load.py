import datetime
import os

import shutil
import logging

import pathlib
import yaml
from pathlib import Path, PosixPath

from gaip import acquisition

from gaip.mtl import load_mtl
from eodatasets import image
import eodatasets.type as ptype


_LOG = logging.getLogger(__name__)


def _get(dictionary, *keys):
    """

    :type dictionary: dict
    :type keys: list of str

    >>> _get({'b': 4, 'a': 2}, 'a')
    2
    >>> _get({'a': {'b': 4}}, 'a', 'b')
    4
    >>> _get({'c': {'b': 4}}, 'a', 'b')
    """
    s = dictionary
    for k in keys:
        if k not in s:
            _LOG.debug('Not found %r', keys)
            return None

        s = s[k]
    return s


def _read_mtl_band_filenames(mtl_):
    """
    Read the list of bands from an mtl dictionary.
    :type mtl_: dict of (str, obj)
    :rtype: dict of (str, str)
    
    >>> _read_mtl_band_filenames({'PRODUCT_METADATA': {
    ...    'file_name_band_9': "LC81010782014285LGN00_B9.TIF",
    ...    'file_name_band_11': "LC81010782014285LGN00_B11.TIF",
    ...    'file_name_band_quality': "LC81010782014285LGN00_BQA.TIF"
    ...    }})
    {'9': 'LC81010782014285LGN00_B9.TIF', '11': 'LC81010782014285LGN00_B11.TIF', 'quality': 'LC81010782014285LGN00_BQA.TIF'}
    >>> _read_mtl_band_filenames({'PRODUCT_METADATA': {
    ...    'file_name_band_9': "LC81010782014285LGN00_B9.TIF",
    ...    'corner_ul_lat_product': -24.98805,
    ...    }})
    {'9': 'LC81010782014285LGN00_B9.TIF'}
    """
    product_md = mtl_['PRODUCT_METADATA']
    return dict([(k.split('_')[-1], v) for (k, v) in product_md.items() if k.startswith('file_name_band_')])


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

    return band_metadata


def _read_bands(mtl_, satellite, sensor, folder_path):
    """

    :param mtl_:
    :param relative_from_dir:
    >>> _read_bands({'PRODUCT_METADATA': {
    ...     'file_name_band_9': "LC81010782014285LGN00_B9.TIF"}
    ... }, 'LANDSAT_8', 'OLI_TIRS', folder_path=PosixPath('product/'))
    {'9': BandMetadata(path=PosixPath('product/LC81010782014285LGN00_B9.TIF'), type=u'atmosphere', label=u'Cirrus', number='9', cell_size=25.0)}
    """
    bs = _read_mtl_band_filenames(mtl_)

    # TODO: shape, size, md5
    return dict([
        (
            number,
            expand_band_information(
                satellite, sensor,
                ptype.BandMetadata(path=folder_path / filename, number=number)
            )
        )
        for (number, filename) in bs.items()])


def populate_from_mtl(md, mtl_path):
    """

    :type md: eodatasets.type.DatasetMetad
    :param mtl_path:
    :rtype: eodatasets.type.DatasetMetad
    """
    if not md:
        md = ptype.DatasetMetadata()

    mtl_path = Path(mtl_path).absolute()
    mtl_ = load_mtl(str(mtl_path))
    return populate_from_mtl_dict(md, mtl_, mtl_path.parent)


def populate_from_mtl_dict(md, mtl_, folder):
    """

    :param mtl_: Parsed mtl file
    :param folder: Folder containing imagery (and mtl)
    :type md: eodatasets.type.DatasetMetadata
    :type mtl_: dict of (str, obj)
    :rtype: eodatasets.type.DatasetMetad
    """
    md.usgs_dataset_id = _get(mtl_, 'METADATA_FILE_INFO', 'landsat_scene_id') or md.usgs_dataset_id
    md.creation_dt = _get(mtl_, 'METADATA_FILE_INFO', 'file_date')
    md.product_type=None,

    # md.size_bytes=None,
    satellite_id = _get(mtl_, 'PRODUCT_METADATA', 'spacecraft_id')
    md.platform.code = satellite_id

    sensor_id = _get(mtl_, 'PRODUCT_METADATA', 'sensor_id')
    md.instrument.name = sensor_id
    # type
    # operation mode

    # md.format_=None,

    md.acquisition.groundstation = ptype.GroundstationMetadata(code=_get(mtl_, "METADATA_FILE_INFO", "station_id"))
    # md.acquisition.groundstation.antenna_coord
    # aos, los, groundstation, heading, platform_orbit

    # Extent
    product_md = _get(mtl_, 'PRODUCT_METADATA')

    date = _get(product_md, 'date_acquired')
    center_time = _get(product_md, 'scene_center_time')
    md.extent.center_dt = datetime.datetime.combine(date, center_time)
    # md.extent.reference_system = ?

    md.extent.coord = ptype.Polygon(
        ul=ptype.Coord(lat=_get(product_md, 'corner_ul_lat_product'), lon=_get(product_md, 'corner_ul_lon_product')),
        ur=ptype.Coord(lat=_get(product_md, 'corner_ur_lat_product'), lon=_get(product_md, 'corner_ur_lon_product')),
        ll=ptype.Coord(lat=_get(product_md, 'corner_ll_lat_product'), lon=_get(product_md, 'corner_ll_lon_product')),
        lr=ptype.Coord(lat=_get(product_md, 'corner_lr_lat_product'), lon=_get(product_md, 'corner_lr_lon_product')),
    )
    # from_dt=None,
    # to_dt=None

    # We don't have a single set of dimensions. Depends on the band?
    # md.grid_spatial.dimensions = []   
    md.grid_spatial.projection.geo_ref_points = ptype.Polygon(
        ul=ptype.Point(x=_get(product_md, 'corner_ul_projection_x_product'),
                       y=_get(product_md, 'corner_ul_projection_y_product')),
        ur=ptype.Point(x=_get(product_md, 'corner_ur_projection_x_product'),
                       y=_get(product_md, 'corner_ur_projection_y_product')),
        ll=ptype.Point(x=_get(product_md, 'corner_ll_projection_x_product'),
                       y=_get(product_md, 'corner_ll_projection_y_product')),
        lr=ptype.Point(x=_get(product_md, 'corner_lr_projection_x_product'),
                       y=_get(product_md, 'corner_lr_projection_y_product'))
    )
    # centre_point=None,
    projection_md = _get(mtl_, 'PROJECTION_PARAMETERS')
    md.grid_spatial.projection.datum = _get(projection_md, 'datum')
    md.grid_spatial.projection.ellipsoid = _get(projection_md, 'ellipsoid')


    # Where does this come from? 'ul' etc.
    # point_in_pixel=None,
    md.grid_spatial.projection.map_projection = _get(projection_md, 'map_projection')
    md.grid_spatial.projection.resampling_option=_get(projection_md, 'resampling_option')
    md.grid_spatial.projection.datum = _get(projection_md, 'datum')
    md.grid_spatial.projection.ellipsoid = _get(projection_md, 'ellipsoid')
    md.grid_spatial.projection.zone = _get(projection_md, 'utm_zone')
    md.grid_spatial.projection.orientation = _get(projection_md, 'orientation')

    image_md = _get(mtl_, 'IMAGE_ATTRIBUTES')

    md.image.satellite_ref_point_start = ptype.Point(
        _get(product_md, 'wrs_path'),
        _get(product_md, 'wrs_row')
    )

    md.image.cloud_cover_percentage = _get(image_md, 'cloud_cover')
    md.image.sun_elevation = _get(image_md, 'sun_elevation')
    md.image.sun_azimuth = _get(image_md, 'sun_azimuth')

    md.image.ground_control_points_model = _get(image_md, 'ground_control_points_model')
    # md.image. = _get(image_md, 'earth_sun_distance')
    md.image.geometric_rmse_model = _get(image_md, 'geometric_rmse_model')
    md.image.geometric_rmse_model_y = _get(image_md, 'geometric_rmse_model_y')
    md.image.geometric_rmse_model_x = _get(image_md, 'geometric_rmse_model_x')

    md.image.bands.update(_read_bands(mtl_, satellite_id, sensor_id, folder))

    # Example "LPGS_2.3.0"
    soft_v = _get(mtl_, 'METADATA_FILE_INFO', 'processing_software_version')
    md.lineage.algorithm.name, md.lineage.algorithm.version = soft_v.split('_')

    md.lineage.algorithm.parameters = {}  # ? TODO

    md.lineage.ancillary.update({
        'cpf': ptype.AncillaryMetadata(name=_get(product_md, 'cpf_name')),
        'bpf_oli': ptype.AncillaryMetadata(name=_get(product_md, 'bpf_name_oli')),
        'bpf_tirs': ptype.AncillaryMetadata(name=_get(product_md, 'bpf_name_tirs')),
        'rlut': ptype.AncillaryMetadata(name=_get(product_md, 'rlut_file_name'))
    })

    return md


def init_local_dataset(uuid=None):
    """
    Create blank metadata for a newly created dataset on this machine.
    :param uuid: The existing dataset_id, if any.
    :rtype: ptype.DatasetMetadata
    """
    md = ptype.DatasetMetadata(
        id_=uuid,
        platform=ptype.PlatformMetadata(),
        instrument=ptype.InstrumentMetadata(),
        acquisition=ptype.AcquisitionMetadata(),
        extent=ptype.ExtentMetadata(),
        grid_spatial=ptype.GridSpatialMetadata(projection=ptype.ProjectionMetadata()),
        image=ptype.ImageMetadata(bands={}),
        lineage=ptype.LineageMetadata(
            algorithm=ptype.AlgorithmMetadata(),
            machine=ptype.MachineMetadata(),
            ancillary={},
            source_datasets={}
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


def checksum_bands(d):
    for number, band_metadata in d.image.bands.items():
        _LOG.info('Checksumming band %r', number)
        band_metadata.checksum_md5 = image.calculate_file_md5(band_metadata.path)

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


def package(image_directory, target_directory, source_datasets=None):
    # TODO: If image directory is not inside target directory, copy images.

    target_directory = Path(target_directory).absolute()

    package_directory = target_directory / 'package'

    _LOG.info('Copying %r -> %r', image_directory, package_directory)
    # TODO: Handle partial packages existing.
    if not package_directory.exists():
        shutil.copytree(str(image_directory), str(package_directory))

    mtl_file = next(Path(package_directory).glob('*_MTL.txt'))  # Crude but effective

    _LOG.info('Reading MTL %r', mtl_file)

    d = init_local_dataset()
    d = populate_from_mtl(d, mtl_file)

    if not target_directory.exists():
        target_directory.mkdir()

    create_browse_images(d, target_directory)
    checksum_bands(d)

    d.lineage.source_datasets = source_datasets

    write_yaml_metadata(d, target_directory, target_directory / 'ga-metadata.yaml')


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
    import doctest

    doctest.testmod()
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)

    package(os.path.expanduser('~/ops/inputs/LS8_something'), 'out-ls8-test')
    # package(os.path.expanduser('~/ops/inputs/lpgsOut/LE7_20150202_091_075'), 'out-ls7-test')





