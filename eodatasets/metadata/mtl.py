import datetime
from gaip.mtl import load_mtl
import eodatasets.type as ptype
import logging
from pathlib import Path

_LOG = logging.getLogger(__name__)


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
    ... }, 'LANDSAT_8', 'OLI_TIRS', folder_path=Path('product/'))
    {'9': BandMetadata(path=PosixPath('product/LC81010782014285LGN00_B9.TIF'), type=u'atmosphere', label=u'Cirrus', number='9', cell_size=25.0)}
    """
    bs = _read_mtl_band_filenames(mtl_)

    # TODO: shape, size, md5
    return dict([
        (
            number,
            ptype.BandMetadata(path=folder_path / filename, number=number)
        )
        for (number, filename) in bs.items()])


def populate_from_mtl(md, mtl_path):
    """

    :type md: eodatasets.type.DatasetMetadata
    :type mtl_path: Path
    :rtype: eodatasets.type.DatasetMetadata
    """
    if not md:
        md = ptype.DatasetMetadata()

    mtl_ = load_mtl(str(mtl_path.absolute()))
    return populate_from_mtl_dict(md, mtl_, mtl_path.parent)


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
    md.product_type = _get(mtl_, 'PRODUCT_METADATA', 'data_type')

    # md.size_bytes=None,
    satellite_id = _get(mtl_, 'PRODUCT_METADATA', 'spacecraft_id')
    if not md.platform:
        md.platform = ptype.PlatformMetadata()
    md.platform.code = satellite_id

    md.format_ = ptype.FormatMetadata(name=_get(mtl_, 'PRODUCT_METADATA', 'output_format'))

    sensor_id = _get(mtl_, 'PRODUCT_METADATA', 'sensor_id')
    if not md.instrument:
        md.instrument = ptype.InstrumentMetadata()
    md.instrument.name = sensor_id
    # type
    # operation mode

    if not md.acquisition:
        md.acquisition = ptype.AcquisitionMetadata()

    md.acquisition.groundstation = ptype.GroundstationMetadata(code=_get(mtl_, "METADATA_FILE_INFO", "station_id"))
    # md.acquisition.groundstation.antenna_coord
    # aos, los, groundstation, heading, platform_orbit

    # Extent
    if not md.extent:
        md.extent = ptype.ExtentMetadata()
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
    if not md.grid_spatial:
        md.grid_spatial = ptype.GridSpatialMetadata()
    if not md.grid_spatial.projection:
        md.grid_spatial.projection = ptype.ProjectionMetadata()

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

    md.lineage.ancillary.update({
        'cpf': ptype.AncillaryMetadata(name=_get(product_md, 'cpf_name')),
        'bpf_oli': ptype.AncillaryMetadata(name=_get(product_md, 'bpf_name_oli')),
        'bpf_tirs': ptype.AncillaryMetadata(name=_get(product_md, 'bpf_name_tirs')),
        'rlut': ptype.AncillaryMetadata(name=_get(product_md, 'rlut_file_name'))
    })

    return md
