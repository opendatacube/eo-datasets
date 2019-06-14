# coding=utf-8
from __future__ import absolute_import

import logging
import math
import os
import shutil
import tempfile
from subprocess import check_call

import numpy
import pathlib
from osgeo import gdal, gdalconst


GDAL_CACHE_MAX_MB = 512

_LOG = logging.getLogger(__name__)


def run_command(command, work_dir):
    _LOG.debug("Running %r", command)
    check_call(command, cwd=work_dir)
    _LOG.debug("Finished %s", command[0])


# This method comes from the old ULA codebase and should be cleaned up eventually.
# pylint: disable=too-many-locals,invalid-name
def _calculate_scale_offset(nodata, band):

    """
    This method comes from the old ULA codebase.
    """
    nbits = gdal.GetDataTypeSize(band.DataType)
    df_scale_dst_min, df_scale_dst_max = 0.0, 255.0
    if nbits == 16:
        count = 32767 + nodata
        histogram = band.GetHistogram(-32767, 32767, 65536)
    else:
        count = 0
        histogram = band.GetHistogram()
    df_scale_src_min = count
    total = 0
    cliplower = int(0.01 * (sum(histogram) - histogram[count]))
    clipupper = int(0.99 * (sum(histogram) - histogram[count]))
    while total < cliplower and count < len(histogram) - 1:
        count += 1
        total += int(histogram[count])
        df_scale_src_min = count
    if nbits == 16:
        count = 32767 + nodata
    else:
        count = 0
    total = 0
    df_scale_src_max = count
    while total < clipupper and count < len(histogram) - 1:
        count += 1
        total += int(histogram[count])
        df_scale_src_max = count
    if nbits == 16:
        df_scale_src_min -= 32768
        df_scale_src_max -= 32768

    # Determine gain and offset
    diff_ = df_scale_src_max - df_scale_src_min

    # From the old Jobmanager codebase: avoid divide by zero caused by some stats.
    if diff_ == 0:
        _LOG.warning("dfScaleSrc Min and Max are equal! Applying correction")
        diff_ = 1

    df_scale = (df_scale_dst_max - df_scale_dst_min) / diff_
    df_offset = -1 * df_scale_src_min * df_scale + df_scale_dst_min

    return df_scale, df_offset


# This method comes from the old ULA codebase and should be cleaned up eventually.
# pylint: disable=too-many-locals
def _create_thumbnail(
    red_file,
    green_file,
    blue_file,
    output_path,
    x_constraint=None,
    nodata=-999,
    work_dir=None,
    overwrite=True,
):
    """
    Create JPEG thumbnail image using individual R, G, B images.

    This method comes from the old ULA codebase.

    :param red_file: red band data file
    :param green_file: green band data file
    :param blue_file: blue band data file
    :param output_path: thumbnail file to write to.
    :param x_constraint: thumbnail width (if not full resolution)
    :param nodata: null/fill data value
    :param work_dir: temp/work directory to use.
    :param overwrite: overwrite existing thumbnail?

    Thumbnail height is adjusted automatically to match the aspect ratio
    of the input images.

    """
    nodata = int(nodata)

    # GDAL calls need absolute paths.
    thumbnail_path = pathlib.Path(output_path).absolute()

    if thumbnail_path.exists() and not overwrite:
        _LOG.warning("File already exists. Skipping creation of %s", thumbnail_path)
        return None, None, None

    # thumbnail_image = os.path.abspath(thumbnail_image)

    out_directory = str(thumbnail_path.parent)
    work_dir = (
        os.path.abspath(work_dir)
        if work_dir
        else tempfile.mkdtemp(prefix=".thumb-tmp", dir=out_directory)
    )
    try:
        # working files
        file_to = os.path.join(work_dir, "rgb.vrt")
        warp_to_file = os.path.join(work_dir, "rgb-warped.vrt")
        outtif = os.path.join(work_dir, "thumbnail.tif")

        # Build the RGB Virtual Raster at full resolution
        run_command(
            [
                "gdalbuildvrt",
                "-overwrite",
                "-separate",
                file_to,
                str(red_file),
                str(green_file),
                str(blue_file),
            ],
            work_dir,
        )
        assert os.path.exists(file_to), "VRT must exist"

        # Determine the pixel scaling to get the correct width thumbnail
        vrt = gdal.Open(file_to)
        intransform = vrt.GetGeoTransform()
        inpixelx = intransform[1]
        # inpixely = intransform[5]
        inrows = vrt.RasterYSize
        incols = vrt.RasterXSize

        # If a specific resolution is asked for.
        if x_constraint:
            outresx = inpixelx * incols / x_constraint
            _LOG.info("Input pixel res %r, output pixel res %r", inpixelx, outresx)

            outrows = int(math.ceil((float(inrows) / float(incols)) * x_constraint))

            run_command(
                [
                    "gdalwarp",
                    "--config",
                    "GDAL_CACHEMAX",
                    str(GDAL_CACHE_MAX_MB),
                    "-of",
                    "VRT",
                    "-tr",
                    str(outresx),
                    str(outresx),
                    "-r",
                    "near",
                    "-overwrite",
                    file_to,
                    warp_to_file,
                ],
                work_dir,
            )
        else:
            # Otherwise use a full resolution browse image.
            outrows = inrows
            x_constraint = incols
            warp_to_file = file_to
            outresx = inpixelx

        _LOG.debug(
            "Current GDAL cache max %rMB. Setting to %rMB",
            gdal.GetCacheMax() / 1024 / 1024,
            GDAL_CACHE_MAX_MB,
        )
        gdal.SetCacheMax(GDAL_CACHE_MAX_MB * 1024 * 1024)

        # Open VRT file to array
        vrt = gdal.Open(warp_to_file)
        driver = gdal.GetDriverByName("GTiff")
        outdataset = driver.Create(outtif, x_constraint, outrows, 3, gdalconst.GDT_Byte)

        # Loop through bands and apply Scale and Offset
        for band_number in (1, 2, 3):
            band = vrt.GetRasterBand(band_number)

            scale, offset = _calculate_scale_offset(nodata, band)

            # Apply gain and offset
            outdataset.GetRasterBand(band_number).WriteArray(
                (numpy.ma.masked_less_equal(band.ReadAsArray(), nodata) * scale)
                + offset
            )
            _LOG.debug("Scale %r, offset %r", scale, offset)

        # Must close datasets to flush to disk.
        # noinspection PyUnusedLocal
        outdataset = None
        # noinspection PyUnusedLocal
        vrt = None

        # GDAL Create doesn't support JPEG so we need to make a copy of the GeoTIFF
        run_command(
            [
                "gdal_translate",
                "--config",
                "GDAL_CACHEMAX",
                str(GDAL_CACHE_MAX_MB),
                "-of",
                "JPEG",
                outtif,
                str(thumbnail_path),
            ],
            work_dir,
        )

        _LOG.debug("Cleaning work files")
    finally:
        # Clean up work files
        if os.path.exists(work_dir):
            shutil.rmtree(work_dir)

    # Newer versions of GDAL create aux files due to the histogram. Clean them up.
    for f in (red_file, blue_file, green_file):
        f = pathlib.Path(f)
        aux_file = f.with_name(f.name + ".aux.xml")
        if aux_file.exists():
            _LOG.info("Cleaning aux: %s", aux_file)
            os.remove(str(aux_file.absolute()))

    return x_constraint, outrows, outresx
