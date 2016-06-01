# coding=utf-8
from __future__ import absolute_import

import datetime
import fnmatch
import logging
import re
import xml.etree.cElementTree as etree

import dateutil.parser
from pathlib import Path

import eodatasets.type as ptype
from .util import parse_type

_LOG = logging.getLogger(__name__)


def populate_ortho(md, base_folder, additional_files):
    """
    Find any relevant Ortho metadata files for the given dataset and populate it.

    :type md: eodatasets.type.DatasetMetadata
    :type base_folder: pathlib.Path
    :type additional_files: tuple[Path]
    :rtype: eodatasets.type.DatasetMetadata
    """
    mtl_path = _get_mtl(base_folder)
    work_order = _find_one('work_order.xml', additional_files) or _find_parent_file(base_folder, 'work_order.xml')
    lpgs_out = _find_one('lpgs_out.xml', additional_files) or _find_parent_file(base_folder, 'lpgs_out.xml')

    # In the same folder as the MTL is an XML file with start/stop times. An "EODS_DATASET", but output by Pinkmatter?
    pseudo_eods_metadata = list(mtl_path.parent.glob('L*.xml'))

    return _populate_ortho_from_files(
        base_folder, md,
        mtl_path=mtl_path,
        work_order_path=work_order,
        lpgs_out_path=lpgs_out,
        pseudo_eods_metadata=pseudo_eods_metadata[0] if pseudo_eods_metadata else None
    )


def _xml_val(doc):
    from_dt = doc.findall("./EXEXTENT/TEMPORALEXTENTFROM")[0].text
    return from_dt


def _find_one(pattern, files):
    """
    :type files: tuple[pathlib.Path]
    :rtype pathlib.Path
    >>> str(_find_one('*.txt', (Path('/tmp/asdf.txt'),)))
    '/tmp/asdf.txt'
    >>> _find_one('*.txt', (Path('/tmp/asdf.tif'),))
    """
    files = [f for f in files if fnmatch.fnmatch(f.name, pattern)]
    if files:
        return files[0]
    return None


def _get_mtl(base_folder):
    return _get_file(base_folder, '*_MTL.txt')


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
                    tree[key.lower()] = parse_type(value)

    tree = {}
    with open(str(filename), 'r') as fo:
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
        unique_names = set(p.name for p in found)
        if len(unique_names) > 1:
            raise RuntimeError('%s unique results found for pattern %r in %s' % (len(unique_names), file_pattern, path))

        _LOG.warning('Duplicate ancillary %r in %s', unique_names.pop(), path)

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


def _get_ancillary_metadata(mtl_doc, wo_doc, mtl_name_offset=None, order_dir_offset=None, properties_offsets=None):
    #: :type: Path
    specified_path = _get_node_text(order_dir_offset, wo_doc, Path) if order_dir_offset and wo_doc else None
    used_file_name = _get(mtl_doc, *mtl_name_offset) if mtl_name_offset and mtl_doc else None

    # Read any properties of the ancillary file form the MTL.
    properties = {}
    if mtl_doc and properties_offsets:
        for property_name, offset in properties_offsets.items():
            val = _get(mtl_doc, *offset)
            if val:
                properties[property_name] = val

    if not specified_path or not specified_path.exists():
        _LOG.warning('No path found to locate ancillary file %s', used_file_name)
        # If there's no information of the ancillary, don't bother.
        if (not used_file_name) and (not properties):
            return None
        return ptype.AncillaryMetadata(name=used_file_name, properties=properties)

    if specified_path.is_file():
        # They specified an exact file to Pinkmatter rather than a search directory.
        file_path = specified_path
    else:
        file_path = _get_file(specified_path, used_file_name)


    _LOG.info('Found ancillary path %s', file_path)
    return ptype.AncillaryMetadata.from_file(file_path, properties=properties)


def _get_node_text(offset, parsed_doc, type_):
    xml_node = parsed_doc.findall(offset)
    if not xml_node:
        _LOG.debug('XML doesn’t contain offset %r', offset)
        return None

    return type_(str(xml_node[0].text).strip())


def _populate_ortho_from_files(base_folder, md, mtl_path, work_order_path,
                               lpgs_out_path=None, pseudo_eods_metadata=None):
    if not md:
        md = ptype.DatasetMetadata()
    if not base_folder:
        base_folder = mtl_path.parent

    _LOG.info('Reading MTL %r', mtl_path)
    mtl_doc = _load_mtl(mtl_path.absolute())

    _LOG.info('Reading work order %r', work_order_path)
    work_order_doc = _load_xml(work_order_path) if work_order_path else None

    md = _populate_from_mtl_dict(md, mtl_doc, base_folder)

    ancil_files = _get_ancil_files(mtl_doc, work_order_doc)
    md.lineage.ancillary.update(ancil_files)

    if lpgs_out_path:
        _LOG.info('Reading lpgs_out: %r', lpgs_out_path)
        lpgs_out_doc = _load_xml(lpgs_out_path)
        pinkmatter_version = lpgs_out_doc.findall('./Version')[0].text

        md.lineage.machine.note_software_version('pinkmatter', str(pinkmatter_version))
        # We could read the processing hostname, start & stop times too. Do we care? We get it elsewhere.

    if pseudo_eods_metadata:
        doc = _load_xml(pseudo_eods_metadata)
        from_dt = _get_node_text("./EXEXTENT/TEMPORALEXTENTFROM", doc, dateutil.parser.parse)
        md.extent.from_dt = md.extent.from_dt or from_dt
        to_dt = _get_node_text("./EXEXTENT/TEMPORALEXTENTTO", doc, dateutil.parser.parse)
        md.extent.to_dt = md.extent.to_dt or to_dt

    return md


def _load_xml(path):
    return etree.parse(str(path))


def _get_ancil_files(mtl_doc, work_order_doc):
    ancil_files = _remove_missing({
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
        ),
        'ephemeris': _get_ancillary_metadata(
            mtl_doc, work_order_doc,
            mtl_name_offset=None,
            order_dir_offset='./L1Processing/EphemerisFile',
            properties_offsets={
                'type': ('PRODUCT_METADATA', 'ephemeris_type')
            }
        ),
        'tirs_ssm_position': _get_ancillary_metadata(
            mtl_doc, work_order_doc,
            mtl_name_offset=None,
            order_dir_offset='./L1Processing/TirsSsmPositionFile',
            properties_offsets={
                'model': ('IMAGE_ATTRIBUTES', 'tirs_ssm_model'),
                'position_status': ('IMAGE_ATTRIBUTES', 'tirs_ssm_position_status')
            }
        )
    })
    return ancil_files


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
