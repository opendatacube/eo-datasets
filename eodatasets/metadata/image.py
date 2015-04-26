# coding=utf-8
from __future__ import absolute_import

from osgeo import gdal, osr

import eodatasets.type as ptype


def _get_extent(gt, cols, rows):
    """ Return the corner coordinates from a geotransform

    :param gt: geotransform (as given by gdal)
    :type gt: (float, float, float, float, float, float)
    :param cols: number of columns in the dataset
    :type cols: int
    :param rows: number of rows in the dataset
    :type rows: int
    :rtype: ptype.CoordPolygon

    >>> gt = (397000.0, 25.0, 0.0, 7236000.0, 0.0, -25.0)
    >>> cols = 9121
    >>> rows = 8881
    >>> _get_extent(gt, cols, rows)
    PointPolygon(\
ul=Point(x=397000.0, y=7236000.0), \
ur=Point(x=625025.0, y=7236000.0), \
ll=Point(x=397000.0, y=7013975.0), \
lr=Point(x=625025.0, y=7013975.0)\
)
    """

    def _get_point(gt, px, py):
        x = gt[0] + (px * gt[1]) + (py * gt[2])
        y = gt[3] + (px * gt[4]) + (py * gt[5])
        return ptype.Point(x, y)

    return ptype.PointPolygon(
        ul=_get_point(gt, 0, 0),
        ll=_get_point(gt, 0, rows),
        ur=_get_point(gt, cols, 0),
        lr=_get_point(gt, cols, rows),
    )


def reproject_coords(coords, source_spatial_ref):
    """
    Reproject a list of x,y coordinates.

    :type coords:     ptype.PointPolygon
    :type src_srs:  C{osr.SpatialReference}
    :param src_srs: Source spatial reference
    :param tgt_srs: Target spatial reference
    :rtype:         ptype.CoordPolygon
    :return:        Projected coords

    >>> c = ptype.PointPolygon(
    ...             ul=ptype.Point(x=397000.0, y=7236000.0),
    ...             ur=ptype.Point(x=625025.0, y=7236000.0),
    ...             ll=ptype.Point(x=397000.0, y=7013975.0),
    ...             lr=ptype.Point(x=625025.0, y=7013975.0)
    ... )
    >>> reproject_coords(c, _GDA_94)
    CoordPolygon(\
ul=Coord(lat=133.9794200916146, lon=-24.9879409027833), \
ur=Coord(lat=136.23877887843614, lon=-24.98628436689941), \
ll=Coord(lat=133.96195524174112, lon=-26.992472356949904), \
lr=Coord(lat=136.25997551867962, lon=-26.990662556011216)\
)
    """
    transform = osr.CoordinateTransformation(source_spatial_ref, source_spatial_ref.CloneGeogCS())

    def _reproject_point(p):
        x, y, height = transform.TransformPoint(p.x, p.y)
        return ptype.Coord(x, y)

    return _map_polygon(_reproject_point, coords)


def _map_polygon(f, poly, poly_cls=ptype.CoordPolygon):
    """
    Map all values of a polygon.

    :type f: object -> object
    :type poly: ptype.Polygon
    :rtype: ptype.Polygon
    """
    return poly_cls(ul=f(poly.ul), ur=f(poly.ur), ll=f(poly.ll), lr=f(poly.lr))


def _get_gdal_image_coords(i):
    """
    :type i: osgeo.gdal.Dataset
    :rtype: ptype.PointPolygon
    """
    return _get_extent(i.GetGeoTransform(), i.RasterXSize, i.RasterYSize)


def populate_from_image_metadata(md):
    """
    Populate by extracting metadata from existing band files.

    :type md: eodatasets.type.DatasetMetadata
    :rtype: eodatasets.type.DatasetMetadata
    """

    for band_id, band in md.image.bands.items():
        i = gdal.Open(str(band.path))

        spacial_ref = osr.SpatialReference(i.GetProjectionRef())

        spacial_ref.GetUTMZone()

        # Extract actual image coords
        # md.grid_spatial.projection.
        band.shape = ptype.Point(i.RasterXSize, i.RasterYSize)

        # TODO separately: create standardised WGS84 coords. for md.extent
        # wkt_contents = spacial_ref.ExportToPrettyWkt()
        # TODO: if srs IsGeographic()? Otherwise srs IsProjected()?
        if not md.grid_spatial:
            md.grid_spatial = ptype.GridSpatialMetadata()

        if not md.grid_spatial.projection:
            md.grid_spatial.projection = ptype.ProjectionMetadata()

        md.grid_spatial.projection.geo_ref_points = _get_gdal_image_coords(i)

        # ?
        md.grid_spatial.projection.datum = 'GDA94'
        md.grid_spatial.projection.ellipsoid = 'GRS80'

        # TODO: DATUM/Reference system etc.

        # TODO: Extent

        # Get positional info, projection etc.

        # Is projection/etc same as previous?
        #  -- If all match, set on wider image.
        i = None

    return md


_GDA_94 = osr.SpatialReference()
_GDA_94.ImportFromWkt('PROJCS["GDA94 / MGA zone 53",GEOGCS["GDA94",'
                      'DATUM["Geocentric_Datum_of_Australia_1994",'
                      'SPHEROID["GRS 1980",6378137,298.2572221010002,'
                      'AUTHORITY["EPSG","7019"]],'
                      'AUTHORITY["EPSG","6283"]],'
                      'PRIMEM["Greenwich",0],'
                      'UNIT["degree",0.0174532925199433],AUTHORITY["EPSG","4283"]],'
                      'PROJECTION["Transverse_Mercator"],'
                      'PARAMETER["latitude_of_origin",0],PARAMETER["central_meridian",135],'
                      'PARAMETER["scale_factor",0.9996],PARAMETER["false_easting",500000],'
                      'PARAMETER["false_northing",10000000],'
                      'UNIT["metre",1,AUTHORITY["EPSG","9001"]],AUTHORITY["EPSG","28353"]]'
                      )

