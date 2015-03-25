import datetime

from gaip.mtl import load_mtl
import eodatasets.type as ptype


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
            return None

        s = s[k]
    return s


def read_mtl(path, md=None):
    """

    :param path:
    :type md: eodatasets.type.DatasetMetadata
    :return:
    """

    if not md:
        md = ptype.DatasetMetadata()

    mtl_ = load_mtl(path)

    # md.id_=None,
    # md.ga_label=None,
    md.usgs_dataset_id = _get(mtl_, 'METADATA_FILE_INFO', 'LANDSAT_SCENE_ID') or md.usgs_dataset_id
    md.creation_dt = _get(mtl_, 'METADATA_FILE_INFO', 'FILE_DATE')
    # md.product_type=None,

    # md.size_bytes=None,
    md.platform.code = _get(mtl_, 'PRODUCT_METADATA', 'SPACECRAFT_ID')

    md.instrument.name = _get(mtl_, 'PRODUCT_METADATA', 'SENSOR_ID')
    # type
    # operation mode

    # md.format_=None,

    md.acquisition.groundstation.code = _get(mtl_, "METADATA_FILE_INFO", "STATIONID")
    # md.acquisition.groundstation.antenna_coord
    # aos, los, groundstation, heading, platform_orbit

    # Extent
    product_md = _get(mtl_, 'PRODUCT_METADATA')
    date = _get(product_md, 'DATE_ACQUIRED')
    center_time = _get(product_md, 'SCENE_CENTER_TIME')
    md.extent.center_dt = datetime.datetime.combine(date, center_time)
    # md.extent.reference_system = ?

    md.extent.coord = ptype.Polygon(
        ul=ptype.Coord(lat=_get(product_md, 'CORNER_UL_LAT_PRODUCT'), lon=_get(product_md, 'CORNER_UL_LON_PRODUCT')),
        ur=ptype.Coord(lat=_get(product_md, 'CORNER_UR_LAT_PRODUCT'), lon=_get(product_md, 'CORNER_UR_LON_PRODUCT')),
        ll=ptype.Coord(lat=_get(product_md, 'CORNER_LL_LAT_PRODUCT'), lon=_get(product_md, 'CORNER_LL_LON_PRODUCT')),
        lr=ptype.Coord(lat=_get(product_md, 'CORNER_LR_LAT_PRODUCT'), lon=_get(product_md, 'CORNER_LR_LON_PRODUCT')),
    )
    # from_dt=None,
    # to_dt=None

    # We don't have a single set of dimensions. Depends on the band?
    # md.grid_spatial.dimensions = []   
    md.grid_spatial.projection.geo_ref_points = ptype.Polygon(
        ul=ptype.Point(x=_get(product_md, 'CORNER_UL_PROJECTION_X_PRODUCT'),
                       y=_get(product_md, 'CORNER_UL_PROJECTION_Y_PRODUCT')),
        ur=ptype.Point(x=_get(product_md, 'CORNER_UR_PROJECTION_X_PRODUCT'),
                       y=_get(product_md, 'CORNER_UR_PROJECTION_Y_PRODUCT')),
        ll=ptype.Point(x=_get(product_md, 'CORNER_LL_PROJECTION_X_PRODUCT'),
                       y=_get(product_md, 'CORNER_LL_PROJECTION_Y_PRODUCT')),
        lr=ptype.Point(x=_get(product_md, 'CORNER_LR_PROJECTION_X_PRODUCT'),
                       y=_get(product_md, 'CORNER_LR_PROJECTION_Y_PRODUCT'))
    )
    # centre_point=None,
    projection_md = _get(mtl_, 'PROJECTION_PARAMETERS')
    md.grid_spatial.projection.datum = _get(projection_md, 'DATUM')
    md.grid_spatial.projection.ellipsoid = _get(projection_md, 'ELLIPSOID')


    # Where does this come from? 'UL' etc.
    # point_in_pixel=None,
    md.grid_spatial.projection.map_projection = _get(projection_md, 'MAP_PROJECTION')
    # resampling_option=None,
    md.grid_spatial.projection.map_projection = _get(projection_md, 'MAP_PROJECTION')
    md.grid_spatial.projection.datum = _get(projection_md, 'DATUM')
    md.grid_spatial.projection.ellipsoid = _get(projection_md, 'ELLIPSOID')
    md.grid_spatial.projection.zone = _get(projection_md, 'UTM_ZONE')

    # md.grid_spatial.projection. = _get(projection_md, 'ORIENTATION') # "NORTH_UP"
    # md.grid_spatial.projection. = _get(projection_md, 'RESAMPLING_OPTION') # "CUBIC_CONVOLUTION"

    # No browse image
    # md.browse=None,

    image_md = _get(mtl_, 'IMAGE_ATTRIBUTES')

    md.image.satellite_ref_point_start = ptype.Point(
        _get(product_md, 'WRS_PATH'),
        _get(product_md, 'WRS_ROW')
    )

    md.image.cloud_cover_percentage = _get(image_md, 'CLOUD_COVER')
    md.image.sun_elevation = _get(image_md, 'SUN_ELEVATION')
    md.image.sun_azimuth = _get(image_md, 'SUN_AZIMUTH')

    md.image.ground_control_points_model = _get(image_md, 'GROUND_CONTROL_POINTS_MODEL')
    # md.image. = _get(image_md, 'EARTH_SUN_DISTANCE')
    md.image.geometric_rmse_model = _get(image_md, 'GEOMETRIC_RMSE_MODEL')
    md.image.geometric_rmse_model_y = _get(image_md, 'GEOMETRIC_RMSE_MODEL_Y')
    md.image.geometric_rmse_model_x = _get(image_md, 'GEOMETRIC_RMSE_MODEL_X')

    md.image.bands.update({
        ''
    })

    # Example "LPGS_2.3.0"
    soft_v = _get(image_md, 'METADATA_FILE_INFO', 'PROCESSING_SOFTWARE_VERSION')
    md.lineage.algorithm.name, md.lineage.algorithm.version = soft_v.split('_')

    md.lineage.algorithm.parameters = {} # ? TODO

    md.lineage.ancillary.update({
        'cpf': ptype.AncillaryMetadata(name=_get(product_md, 'CPF_NAME')),
        'bpf_oli': ptype.AncillaryMetadata(name=_get(product_md, 'BPF_NAME_OLI')),
        'bpf_tirs': ptype.AncillaryMetadata(name=_get(product_md, 'BPF_NAME_TIRS')),
        'rlut': ptype.AncillaryMetadata(name=_get(product_md, 'RLUT_FILE_NAME'))
    })
    # md.image=None,
    # md.lineage=None):


if __name__ == '__main__':
    import doctest

    doctest.testmod(type)

    mtl_path = '/Users/jeremyhooke/ops/package-eg/LS8_OLITIRS_OTH_P51_GALPGS01-032_101_078_20141012/scene01' \
               '/LC81010782014285LGN00_MTL.txt'
    read_mtl(mtl_path)
