from __future__ import absolute_import

import logging
import sys

import rasterio
import rasterio.features
import shapely.affinity
import shapely.geometry
import shapely.ops

_LOG = logging.getLogger(__name__)


def valid_region(images, mask_value=None):
    """
    Deprecated valid_region method.

    Used by the legacy prepare scripts. Newer ones will
    presumably use the DatasetAssembler api instead.
    """
    try:
        from scipy import ndimage
    except ImportError:
        sys.stderr.write(
            "eodatasets3 has not been installed with the ancillary extras. \n"
            "    Try `pip install eodatasets3[ancillary]\n"
        )
        raise
    mask = None

    if not images:
        _LOG.warning("No images: empty region")
        return None

    for fname in images:
        with rasterio.open(str(fname), "r") as ds:
            transform = ds.transform
            img = ds.read(1)

            if mask_value is not None:
                new_mask = img & mask_value == mask_value
            else:
                new_mask = img != ds.nodata

            if mask is None:
                mask = new_mask
            else:
                mask |= new_mask

    # apply a fill holes filter; reduces run time of the union function
    # when there are lots of holes in the data eg NBART, PQ, and Landsat 7
    mask = ndimage.binary_fill_holes(mask)

    shapes = rasterio.features.shapes(mask.astype("uint8"), mask=mask)
    shape = shapely.ops.unary_union(
        [shapely.geometry.shape(shape) for shape, val in shapes if val == 1]
    )

    # convex hull
    geom = shape.convex_hull

    # buffer by 1 pixel
    geom = geom.buffer(1, join_style=3, cap_style=3)

    # simplify with 1 pixel radius
    geom = geom.simplify(1)

    # intersect with image bounding box
    geom = geom.intersection(shapely.geometry.box(0, 0, mask.shape[1], mask.shape[0]))

    # transform from pixel space into CRS space
    geom = shapely.affinity.affine_transform(
        geom,
        (
            transform.a,
            transform.b,
            transform.d,
            transform.e,
            transform.xoff,
            transform.yoff,
        ),
    )

    output = shapely.geometry.mapping(geom)
    output["coordinates"] = _to_lists(output["coordinates"])
    return output


def _to_lists(x):
    """
    Returns lists of lists when given tuples of tuples
    """
    if isinstance(x, tuple):
        return [_to_lists(el) for el in x]

    return x
