from __future__ import absolute_import
import datetime
import logging

from pathlib import Path

import eodatasets.type as ptype

import re
_LOG = logging.getLogger(__name__)


def parse_type(s):
    """Parse the string `s` and return a native python object."""

    strptime = datetime.datetime.strptime

    def yesno(s):
        """Parse Y/N"""
        if len(s) == 1:
            if s == 'Y':
                return True
            if s == 'N':
                return False
        raise ValueError

    def none(s):
        """Parse a NONE"""
        if len(s) == 4 and s == 'NONE':
            return None
        raise ValueError

    parsers = [int,
               float,
               lambda x: strptime(x, '%Y-%m-%dT%H:%M:%SZ'),
               lambda x: strptime(x, '%Y-%m-%d').date(),
               lambda x: strptime(x[0:15], '%H:%M:%S.%f').time(),
               lambda x: yesno(x.strip('"')),
               lambda x: none(x.strip('"')),
               lambda x: str(x.strip('"'))]

    for parser in parsers:
        try:
            return parser(s)
        except ValueError:
            pass
    raise ValueError


def load_mtl(filename, root='L1_METADATA_FILE', pairs=r'(\w+)\s=\s(.*)'):
    """Parse an MTL file and return dict-of-dict's containing the metadata."""

    def parse(lines, tree, level=0):
        """Parse it"""
        while lines:
            line = lines.pop(0)
            match = re.findall(pairs, line)
            if match:
                key, value = match[0]
                if key == 'GROUP':
                    tree[value] = {}
                    parse(lines, tree[value], level + 1)
                elif key == 'END_GROUP':
                    break
                else:
                    tree[key.lower()] = parse_type(value)

    tree = {}
    with open(filename, 'r') as fo:
        parse(fo.readlines(), tree)

    return tree[root]


def _read_mtl_band_filenames(mtl_):
    """
    Read the list of bands from an mtl dictionary.
    :type mtl_: dict of (str, obj)
    :rtype: dict of (str, str)

    >>> _read_mtl_band_filenames({'PRODUCT_METADATA': {
    ...    'file_name_band_9': "LC81010782014285LGN00_B9.TIF",
    ...    'file_name_band_11': "LC81010782014285LGN00_B11.TIF",
    ...    'file_name_band_quality': "LC81010782014285LGN00_BQA.TIF"
    ... }})
    {'9': 'LC81010782014285LGN00_B9.TIF', '11': 'LC81010782014285LGN00_B11.TIF', 'quality': \
'LC81010782014285LGN00_BQA.TIF'}
    >>> _read_mtl_band_filenames({'PRODUCT_METADATA': {
    ...    'file_name_band_9': "LC81010782014285LGN00_B9.TIF",
    ...    'corner_ul_lat_product': -24.98805,
    ... }})
    {'9': 'LC81010782014285LGN00_B9.TIF'}
    >>> _read_mtl_band_filenames({'PRODUCT_METADATA': {
    ...    'file_name_band_6_vcid_1': "LE71140732005007ASA00_B6_VCID_1.TIF"
    ... }})
    {'6_vcid_1': 'LE71140732005007ASA00_B6_VCID_1.TIF'}
    """
    product_md = mtl_['PRODUCT_METADATA']
    return dict([(k.split('band_')[-1], v) for (k, v) in product_md.items() if k.startswith('file_name_band_')])


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


def _read_bands(mtl_, folder_path):
    """

    :type mtl_: dict of (str, obj)
    :type folder_path: pathlib.Path
    >>> _read_bands({'PRODUCT_METADATA': {
    ...     'file_name_band_9': "LC81010782014285LGN00_B9.TIF"}
    ... }, folder_path=Path('product/'))
    {'9': BandMetadata(path=PosixPath('product/LC81010782014285LGN00_B9.TIF'), number='9')}
    """
    bs = _read_mtl_band_filenames(mtl_)

    # TODO: shape, size, md5
    return dict([
        (
            number,
            ptype.BandMetadata(path=folder_path / filename, number=number)
        )
        for (number, filename) in bs.items()])


def populate_from_mtl(md, mtl_path, base_folder=None):
    """

    :type md: eodatasets.type.DatasetMetadata
    :type mtl_path: Path
    :rtype: eodatasets.type.DatasetMetadata
    """
    if not md:
        md = ptype.DatasetMetadata()

    if not base_folder:
        base_folder = mtl_path.parent
    mtl_ = load_mtl(str(mtl_path.absolute()))
    return populate_from_mtl_dict(md, mtl_, base_folder)


def populate_from_mtl_dict(md, mtl_, folder):
    """

    :param mtl_: Parsed mtl file
    :param folder: Folder containing imagery (and mtl)
    :type md: eodatasets.type.DatasetMetadata
    :type mtl_: dict of (str, obj)
    :rtype: eodatasets.type.DatasetMetadata
    """
    md.usgs_dataset_id = _get(mtl_, 'METADATA_FILE_INFO', 'landsat_scene_id') or md.usgs_dataset_id
    md.creation_dt = _get(mtl_, 'METADATA_FILE_INFO', 'file_date')

    # TODO: elsewhere we've used 'GAORTHO01' etc. Here it's 'L1T' etc.
    md.product_level = _get(mtl_, 'PRODUCT_METADATA', 'data_type')

    # md.size_bytes=None,
    satellite_id = _get(mtl_, 'PRODUCT_METADATA', 'spacecraft_id')
    if not md.platform:
        md.platform = ptype.PlatformMetadata()
    md.platform.code = satellite_id

    md.format_ = ptype.FormatMetadata(name=_get(mtl_, 'PRODUCT_METADATA', 'output_format'))

    product_md = _get(mtl_, 'PRODUCT_METADATA')
    sensor_id = _get(mtl_, 'PRODUCT_METADATA', 'sensor_id')
    if not md.instrument:
        md.instrument = ptype.InstrumentMetadata()
    md.instrument.name = sensor_id
    # md.instrument.type_
    md.instrument.operation_mode = _get(product_md, 'sensor_mode')

    if not md.acquisition:
        md.acquisition = ptype.AcquisitionMetadata()

    md.acquisition.groundstation = ptype.GroundstationMetadata(code=_get(mtl_, "METADATA_FILE_INFO", "station_id"))
    # md.acquisition.groundstation.antenna_coord
    # aos, los, groundstation, heading, platform_orbit

    # Extent
    if not md.extent:
        md.extent = ptype.ExtentMetadata()

    date = _get(product_md, 'date_acquired')
    center_time = _get(product_md, 'scene_center_time')
    md.extent.center_dt = datetime.datetime.combine(date, center_time)
    # md.extent.reference_system = ?

    md.extent.coord = ptype.CoordPolygon(
        ul=ptype.Coord(lat=_get(product_md, 'corner_ul_lat_product'), lon=_get(product_md, 'corner_ul_lon_product')),
        ur=ptype.Coord(lat=_get(product_md, 'corner_ur_lat_product'), lon=_get(product_md, 'corner_ur_lon_product')),
        ll=ptype.Coord(lat=_get(product_md, 'corner_ll_lat_product'), lon=_get(product_md, 'corner_ll_lon_product')),
        lr=ptype.Coord(lat=_get(product_md, 'corner_lr_lat_product'), lon=_get(product_md, 'corner_lr_lon_product')),
    )
    # from_dt=None,
    # to_dt=None

    # We don't have a single set of dimensions. Depends on the band?
    # md.grid_spatial.dimensions = []
    if not md.grid_spatial:
        md.grid_spatial = ptype.GridSpatialMetadata()
    if not md.grid_spatial.projection:
        md.grid_spatial.projection = ptype.ProjectionMetadata()

    md.grid_spatial.projection.geo_ref_points = ptype.PointPolygon(
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
    md.grid_spatial.projection.resampling_option = _get(projection_md, 'resampling_option')
    md.grid_spatial.projection.datum = _get(projection_md, 'datum')
    md.grid_spatial.projection.ellipsoid = _get(projection_md, 'ellipsoid')
    md.grid_spatial.projection.zone = _get(projection_md, 'utm_zone')
    md.grid_spatial.projection.orientation = _get(projection_md, 'orientation')

    image_md = _get(mtl_, 'IMAGE_ATTRIBUTES')

    if not md.image:
        md.image = ptype.ImageMetadata()

    md.image.satellite_ref_point_start = ptype.Point(
        _get(product_md, 'wrs_path'),
        _get(product_md, 'wrs_row')
    )

    md.image.cloud_cover_percentage = _get(image_md, 'cloud_cover')
    md.image.sun_elevation = _get(image_md, 'sun_elevation')
    md.image.sun_azimuth = _get(image_md, 'sun_azimuth')
    md.image.sun_earth_distance = _get(image_md, 'earth_sun_distance')

    md.image.ground_control_points_model = _get(image_md, 'ground_control_points_model')
    # md.image. = _get(image_md, 'earth_sun_distance')
    md.image.geometric_rmse_model = _get(image_md, 'geometric_rmse_model')
    md.image.geometric_rmse_model_y = _get(image_md, 'geometric_rmse_model_y')
    md.image.geometric_rmse_model_x = _get(image_md, 'geometric_rmse_model_x')

    if not md.image.bands:
        md.image.bands = {}

    md.image.bands.update(_read_bands(mtl_, folder))

    if not md.lineage:
        md.lineage = ptype.LineageMetadata()
    if not md.lineage.algorithm:
        md.lineage.algorithm = ptype.AlgorithmMetadata()
    # Example "LPGS_2.3.0"
    soft_v = _get(mtl_, 'METADATA_FILE_INFO', 'processing_software_version')
    md.lineage.algorithm.name, md.lineage.algorithm.version = soft_v.split('_')

    md.lineage.algorithm.parameters = {}  # ? TODO

    if not md.lineage.ancillary:
        md.lineage.ancillary = {}

    md.lineage.ancillary_quality = _get(product_md, 'ephemeris_type')
    md.lineage.ancillary.update(
        _wrap_ancillary({
            'cpf': _get(product_md, 'cpf_name'),
            'bpf_oli': _get(product_md, 'bpf_name_oli'),
            'bpf_tirs': _get(product_md, 'bpf_name_tirs'),
            'rlut': _get(product_md, 'rlut_file_name')
        })
    )

    return md


def _wrap_ancillary(dict_):
    """
    Remove fields from the dict whose values are None.

    Returns a new dict.
    :type dict_: dict
    :rtype dict

    >>> _wrap_ancillary({'cpf': 'L7CPF20050101_20050331.09', 'rlut': None})
    {'cpf': AncillaryMetadata(name='L7CPF20050101_20050331.09')}
    >>> _wrap_ancillary({'cpf': 'L7CPF20050101_20050331.09', 'bpf_oli': 'LO8BPF20140127130115_20140127144056.01'})
    {'bpf_oli': AncillaryMetadata(name='LO8BPF20140127130115_20140127144056.01'), \
'cpf': AncillaryMetadata(name='L7CPF20050101_20050331.09')}
    >>> _wrap_ancillary({})
    {}
    """
    return {k: ptype.AncillaryMetadata(name=v) for k, v in dict_.iteritems() if v is not None}
