import glob
import os, logging, subprocess, math, errno, re
import  gdal, gdalconst
import numpy.ma as ma


logger = logging.getLogger('root.' + __name__)


def create_thumbnail(red_file, green_file, blue_file, thumbnail_image,
                     outcols=1024, nodata=-999, work_dir=None, overwrite=True):
    """Create JPEG thumbnail image using individual R, G, B images.
    Arguments:
        red_file: red band data file
        green_file: green band data file
        blue_file: blue band data file
        thumbnail_image: Name of thumbnail image file
        outcols: thumbnail image width
        nodata: null/fill data value
    Thumbnail height is adjusted automatically to match the aspect ratio
    of the input images.
    """

    # working files
    file_to = "RGB.vrt"
    warp_to_file = "RGBwarped.vrt"
    outtif = "thumbnail.tif"

    if work_dir:
        file_to = os.path.join(work_dir, file_to)
        warp_to_file = os.path.join(work_dir, warp_to_file)
        outtif = os.path.join(work_dir, outtif)

    # Build the RGB Virtual Raster at full resolution
    subprocess.call(["gdalbuildvrt", "-overwrite", "-separate", file_to, red_file, green_file, blue_file], cwd=work_dir)

    # Determine the pixel scaling to get the correct width thumbnail
    vrt = gdal.Open(file_to)
    intransform = vrt.GetGeoTransform()
    inpixelx = intransform[1]
    #inpixely = intransform[5]
    inrows = vrt.RasterYSize
    incols = vrt.RasterXSize
    #print inrows,incols
    outresx = inpixelx*incols/outcols
    outrows = int(math.ceil((float(inrows)/float(incols))*outcols))
    #print outresx, outcols, outrows

    if (overwrite or not os.path.exists(thumbnail_image)):
        subprocess.call(["gdalwarp", "-of", "VRT", "-tr", str(outresx), str(outresx), "-r", "near", "-overwrite", file_to, warp_to_file], cwd=work_dir)

        # Open VRT file to array
        vrt = gdal.Open(warp_to_file)
        bands = (1,2,3)
        driver = gdal.GetDriverByName ("GTiff")
        outdataset = driver.Create(outtif,outcols,outrows, 3, gdalconst.GDT_Byte)
        #rgb_composite = numpy.zeros((outrows,outcols,3))

        # Loop through bands and apply Scale and Offset
        for bandnum, band in enumerate(bands):
            vrtband = vrt.GetRasterBand(band)
            vrtband_array = vrtband.ReadAsArray()
            nbits=gdal.GetDataTypeSize(vrtband.DataType)
            #print nbits
            dfScaleDstMin,dfScaleDstMax=0.0,255.0

            # Determine scale limits
            #dfScaleSrcMin = dfBandMean - 2.58*(dfBandStdDev)
            #dfScaleSrcMax = dfBandMean + 2.58*(dfBandStdDev)

            if (nbits == 16):
                count = 32767 + int(nodata)
                histogram = vrtband.GetHistogram(-32767, 32767, 65536)
            else:
                count = 0
                histogram = vrtband.GetHistogram()
            total = 0

            cliplower = int(0.01*(sum(histogram)-histogram[count]))
            clipupper = int(0.99*(sum(histogram)-histogram[count]))
            #print sum(histogram)
            #print cliplower,clipupper
            #print histogram[31768]
            while total < cliplower and count < len(histogram)-1:
                count = count+1
                total = int(histogram[count])+total
                dfScaleSrcMin = count
            #print "total",total
            if (nbits == 16):
                count = 32767 + int(nodata)
            else: count = 0
            #print "count for max",count
            total = 0
            while total < clipupper and count < len(histogram)-1:
                count = count+1
                #print count,clipupper,total
                total = int(histogram[count])+total
                dfScaleSrcMax = count

            if (nbits == 16):
                dfScaleSrcMin = dfScaleSrcMin - 32768
                dfScaleSrcMax = dfScaleSrcMax - 32768

            # Determine gain and offset
            dfScale = (dfScaleDstMax - dfScaleDstMin) / (dfScaleSrcMax - dfScaleSrcMin)
            dfOffset = -1 * dfScaleSrcMin * dfScale + dfScaleDstMin

            # Apply gain and offset
            outdataset.GetRasterBand(band).WriteArray((ma.masked_less_equal(vrtband_array, int(nodata))*dfScale)+dfOffset)

        outdataset = None

        # GDAL Create doesn't support JPEG so we need to make a copy of the GeoTIFF
        subprocess.call(["gdal_translate", "-of", "JPEG", outtif, thumbnail_image])

    else:
        logger.warning('File already exists. Skipping creation of %s', thumbnail_image)

    # Clean up work files
    for f in [file_to, warp_to_file, outtif]:
        try:
            os.unlink(f)
        except OSError, e:
            if e.errno != errno.ENOENT:
                raise

    return (outcols, outrows)


if __name__ == '__main__':
    logging.basicConfig()
    logger.setLevel(logging.INFO)
    #     red_band=7,
    # green_band=5,
    # blue_band=1
    _dir = os.path.expanduser('~/ops/package-eg/LS8_OLITIRS_OTH_P51_GALPGS01-032_101_078_20141012/scene01')
    create_thumbnail(
        glob.glob(_dir+'/*_B7.TIF')[0],
        glob.glob(_dir+'/*_B5.TIF')[0],
        glob.glob(_dir+'/*_B1.TIF')[0],
        thumbnail_image='test-thumb.jpg',
        work_dir=os.path.abspath('out-tmp')
    )