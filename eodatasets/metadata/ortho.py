# coding=utf-8
from __future__ import absolute_import

import datetime
import logging
import re
import xml.etree.cElementTree as etree

from pathlib import Path

import eodatasets.type as ptype

_LOG = logging.getLogger(__name__)


def populate_ortho(md, base_folder):
    """
    Find any relevant Ortho metadata files for the given dataset and populate it.

    :type md: eodatasets.type.DatasetMetadata
    :type base_folder: pathlib.Path
    :rtype: eodatasets.type.DatasetMetadata
    """
    mtl_path = _get_file(base_folder, '*_MTL.txt')
    work_order = _find_parent_file(base_folder, 'work_order.xml')

    return _populate_ortho_from_files(base_folder, md, mtl_path, work_order)


def _parse_type(s):
    """Parse the string `s` and return a native python object.

    >>> _parse_type('01:40:54.7722350Z')
    datetime.time(1, 40, 54, 772235)
    >>> _parse_type('2015-03-29')
    datetime.date(2015, 3, 29)
    >>> # Some have added quotes
    >>> _parse_type('"01:40:54.7722350Z"')
    datetime.time(1, 40, 54, 772235)
    >>> _parse_type("NONE")
    >>> _parse_type("Y")
    True
    >>> _parse_type("N")
    False
    >>> _parse_type('Plain String')
    'Plain String'
    >>> _parse_type('"Quoted String"')
    'Quoted String'
    """

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
               yesno,
               none,
               str]

    for parser in parsers:
        try:
            return parser(s.strip('"'))
        except ValueError:
            pass
    raise ValueError


def _load_mtl(filename, root='L1_METADATA_FILE', pairs=r'(\w+)\s=\s(.*)'):
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
                    tree[key.lower()] = _parse_type(value)

    tree = {}
    with open(filename, 'r') as fo:
        parse(fo.readlines(), tree)

    return tree[root]


def _read_mtl_band_filenames(mtl_):
    """
    Read the list of bands from an mtl dictionary.
    :type mtl_: dict of (str, obj)
    :rtype: dict of (str, str)

    >>> sorted(_read_mtl_band_filenames({'PRODUCT_METADATA': {
    ...    'file_name_band_9': "LC81010782014285LGN00_B9.TIF",
    ...    'file_name_band_11': "LC81010782014285LGN00_B11.TIF",
    ...    'file_name_band_quality': "LC81010782014285LGN00_BQA.TIF"
    ... }}).items())
    [('11', 'LC81010782014285LGN00_B11.TIF'), ('9', 'LC81010782014285LGN00_B9.TIF'), ('quality', \
'LC81010782014285LGN00_BQA.TIF')]
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
    >>> import pathlib
    >>> _read_bands({'PRODUCT_METADATA': {
    ...     'file_name_band_9': "LC81010782014285LGN00_B9.TIF"}
    ... }, folder_path=pathlib.Path('product/'))
    {'9': BandMetadata(path=PosixPath('product/LC81010782014285LGN00_B9.TIF'), number='9')}
    """
    bs = _read_mtl_band_filenames(mtl_)

    # TODO: shape, size, md5
    return dict([(number, ptype.BandMetadata(path=folder_path / filename, number=number))
                 for (number, filename) in bs.items()])


def _get_file(path, file_pattern, mandatory=True):
    found = list(path.rglob(file_pattern))

    if not found:
        if mandatory:
            raise RuntimeError('Not found: %r in %s' % (file_pattern, path))
        return

    if len(found) > 1:
        raise RuntimeError('%s results found for pattern %r in %s' % (len(found), file_pattern, path))

    return found[0]


def _find_parent_file(path, pattern, max_levels=3):
    found = list(path.glob(pattern))
    if found:
        return found[0]

    if max_levels <= 1:
        return None

    # (pathlib "parent" of the root dir is the root dir.)
    return _find_parent_file(path.parent, pattern, max_levels=max_levels - 1)


def _remove_missing(dict_):
    """
    Remove fields from the dict whose values are None.

    Returns a new dict.
    :type dict_: dict
    :rtype dict

    >>> _remove_missing({'cpf': 'L7CPF20050101_20050331.09', 'rlut': None})
    {'cpf': 'L7CPF20050101_20050331.09'}
    >>> sorted(
    ...     _remove_missing({
    ...         'cpf': 'L7CPF20050101_20050331.09',
    ...         'bpf_oli': 'LO8BPF20140127130115_20140127144056.01'
    ...     }).items()
    ...  )
    [('bpf_oli', 'LO8BPF20140127130115_20140127144056.01'), \
('cpf', 'L7CPF20050101_20050331.09')]
    >>> _remove_missing({})
    {}
    """
    return {k: v for k, v in dict_.items() if v is not None}


def _get_ancillary_metadata(mtl_doc, wo_doc, mtl_name_offset, order_dir_offset):
    file_name = _get(mtl_doc, *mtl_name_offset)
    if not file_name:
        return None

    file_search_directory = _get_node_text(order_dir_offset, wo_doc) if wo_doc else None

    if not file_search_directory or not file_search_directory.exists():
        _LOG.warning('No path found to locate ancillary file %s', file_name)
        return ptype.AncillaryMetadata(name=file_name)

    if file_search_directory.is_file():
        # They specified an exact file to Pinkmatter rather than a search directory.
        file_path = file_search_directory
    else:
        file_path = _get_file(file_search_directory, file_name)

    _LOG.info('Found ancillary path %s', file_path)
    return ptype.AncillaryMetadata.from_file(file_path)


def _get_node_text(offset, parsed_doc):
    xml_node = parsed_doc.findall(offset)
    file_search_directory = Path(xml_node[0].text)
    return file_search_directory


def _populate_ortho_from_files(base_folder, md, mtl_path, work_order):
    if not md:
        md = ptype.DatasetMetadata()
    if not base_folder:
        base_folder = mtl_path.parent

    _LOG.info('Reading MTL %r', mtl_path)
    mtl_doc = _load_mtl(str(mtl_path.absolute()))

    work_order_doc = None
    if work_order:
        _LOG.info('Reading work order %r', work_order)
        work_order_doc = etree.parse(str(work_order))

    md = _populate_from_mtl_dict(md, mtl_doc, base_folder)
    md.lineage.ancillary.update(
        _remove_missing({
            'cpf': _get_ancillary_metadata(
                mtl_doc, work_order_doc,
                mtl_name_offset=('PRODUCT_METADATA', 'cpf_name'),
                order_dir_offset='./L0RpProcessing/CalibrationFile'
            ),
            'bpf_oli': _get_ancillary_metadata(
                mtl_doc, work_order_doc,
                mtl_name_offset=('PRODUCT_METADATA', 'bpf_name_oli'),
                order_dir_offset='./L1Processing/BPFOliFile'
            ),
            'bpf_tirs': _get_ancillary_metadata(
                mtl_doc, work_order_doc,
                mtl_name_offset=('PRODUCT_METADATA', 'bpf_name_tirs'),
                order_dir_offset='./L1Processing/BPFTirsFile'
            ),
            'rlut': _get_ancillary_metadata(
                mtl_doc, work_order_doc,
                mtl_name_offset=('PRODUCT_METADATA', 'rlut_file_name'),
                order_dir_offset='./L1Processing/RlutFile'
            )
        })
    )

    return md


def _populate_extent(md, product_md):
    # Extent
    if not md.extent:
        md.extent = ptype.ExtentMetadata()
    date = _get(product_md, 'date_acquired')
    center_time = _get(product_md, 'scene_center_time')
    _LOG.debug('Center date/time: %r, %r', date, center_time)
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


def _populate_grid_spatial(md, mtl_):
    product_md = _get(mtl_, 'PRODUCT_METADATA')
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


def _populate_image(md, mtl_):
    product_md = _get(mtl_, 'PRODUCT_METADATA')
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


def _populate_lineage(md, mtl_):
    product_md = _get(mtl_, 'PRODUCT_METADATA')
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
        _remove_missing({
            'cpf': _get(product_md, 'cpf_name'),
            'bpf_oli': _get(product_md, 'bpf_name_oli'),
            'bpf_tirs': _get(product_md, 'bpf_name_tirs'),
            'rlut': _get(product_md, 'rlut_file_name')
        })
    )


def _populate_from_mtl_dict(md, mtl_, folder):
    """

    :param mtl_: Parsed mtl file
    :param folder: Folder containing imagery (and mtl). For fixing relative paths in the MTL.
    :type md: eodatasets.type.DatasetMetadata
    :type mtl_: dict of (str, obj)
    :rtype: eodatasets.type.DatasetMetadata
    """
    if not md.usgs:
        md.usgs = ptype.UsgsMetadata()
    md.usgs.scene_id = _get(mtl_, 'METADATA_FILE_INFO', 'landsat_scene_id')
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

    _populate_extent(md, product_md)
    _populate_grid_spatial(md, mtl_)
    _populate_image(md, mtl_)
    _populate_lineage(md, mtl_)

    return md
