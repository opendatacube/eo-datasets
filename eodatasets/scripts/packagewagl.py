#!/usr/bin/env python
# pylint: disable=too-many-locals

import os
import re
import subprocess
import tempfile
import uuid
from enum import Enum
from functools import partial
from os.path import join as pjoin, basename, dirname, exists
from pathlib import Path
from posixpath import join as ppjoin
from subprocess import check_call
from typing import Dict, Any, Tuple, Sequence, Union

import attr
import h5py
import numpy
import numpy as np
import rasterio
import yaml
from affine import Affine
from click import secho
from rasterio.enums import Resampling
from skimage.exposure import rescale_intensity
from yaml.representer import Representer

from eodatasets.prepare import serialise
from eodatasets.prepare.model import DatasetDoc
from eodatasets.verify import PackageChecksum

# from wagl.acquisition import acquisitions, AcquisitionsContainer

# from wagl.acquisition import Acquisition

EUGL_VERSION = "DO_SOMETHING_HERE"

FMASK_VERSION = "DO_SOMETHING_HERE2"
FMASK_REPO_URL = "https://bitbucket.org/chchrsc/python-fmask"

TESP_VERSION = "DO_SOMETHING_HERE"
TESP_REPO_URL = "https://github.com/OpenDataCubePipelines/tesp"

os.environ["CPL_ZIP_ENCODING"] = "UTF-8"


@attr.s(auto_attribs=True, frozen=True)
class GeoBox:
    shape: Tuple = None
    origin: Tuple[float, float] = None
    pixelsize: Tuple[int, int] = None
    crs_wkt: str = None

    @classmethod
    def from_dataset(cls, dataset):
        raise NotImplementedError(f"We want {type(dataset)}")

    @classmethod
    def from_rio(cls, dataset):
        return cls(
            shape=dataset.shape,
            origin=(dataset.transform[2], dataset.transform[5]),
            pixelsize=(dataset.res),
            crs_wkt=dataset.crs.wkt,
        )

    @classmethod
    def from_h5(cls, dataset):
        transform = tuple(dataset.attrs["geotransform"])
        return cls(
            shape=dataset.shape,
            origin=(transform[0], transform[3]),
            crs_wkt=dataset.attrs["crs_wkt"],
            pixelsize=(abs(transform[1]), abs(transform[5])),
        )

    @property
    def transform(self):
        return Affine(
            self.pixelsize[0], 0, self.origin[0], 0, -self.pixelsize[1], self.origin[1]
        )

    @property
    def x_size(self):
        """The x-axis size."""
        return self.shape[1]

    @property
    def y_size(self):
        """The y-axis size."""
        return self.shape[0]


class DatasetName(Enum):
    """
    Defines the dataset names or format descriptors, that are used
    for creating and accessing throughout the code base.
    """

    # wagl.ancillary
    COORDINATOR = "COORDINATOR"
    DEWPOINT_TEMPERATURE = "DEWPOINT-TEMPERATURE"
    TEMPERATURE_2M = "TEMPERATURE-2METRE"
    SURFACE_PRESSURE = "SURFACE-PRESSURE"
    SURFACE_GEOPOTENTIAL = "SURFACE-GEOPOTENTIAL-HEIGHT"
    SURFACE_RELATIVE_HUMIDITY = "SURFACE-RELATIVE-HUMIDITY"
    GEOPOTENTIAL = "GEO-POTENTIAL"
    TEMPERATURE = "TEMPERATURE"
    RELATIVE_HUMIDITY = "RELATIVE-HUMIDITY"
    ATMOSPHERIC_PROFILE = "ATMOSPHERIC-PROFILE"
    AEROSOL = "AEROSOL"
    WATER_VAPOUR = "WATER-VAPOUR"
    OZONE = "OZONE"
    ELEVATION = "ELEVATION"
    BRDF_FMT = "BRDF-{parameter}-{band_name}"
    ECMWF_PATH_FMT = pjoin("{product}", "{year}", "tif", "{product}_*.tif")

    # wagl.longitude_latitude_arrays
    LON = "LONGITUDE"
    LAT = "LATITUDE"

    # wagl.satellite_solar_angles
    SATELLITE_VIEW = "SATELLITE-VIEW"
    SATELLITE_AZIMUTH = "SATELLITE-AZIMUTH"
    SOLAR_ZENITH = "SOLAR-ZENITH"
    SOLAR_ZENITH_CHANNEL = "SOLAR-ZENITH-CHANNEL"
    SOLAR_AZIMUTH = "SOLAR-AZIMUTH"
    RELATIVE_AZIMUTH = "RELATIVE-AZIMUTH"
    TIME = "TIMEDELTA"
    CENTRELINE = "CENTRELINE"
    BOXLINE = "BOXLINE"
    SPHEROID = "SPHEROID"
    ORBITAL_ELEMENTS = "ORBITAL-ELEMENTS"
    SATELLITE_MODEL = "SATELLITE-MODEL"
    SATELLITE_TRACK = "SATELLITE-TRACK"
    GENERIC = "GENERIC"

    # wagl.incident_exiting_angles
    INCIDENT = "INCIDENT"
    AZIMUTHAL_INCIDENT = "AZIMUTHAL-INCIDENT"
    EXITING = "EXITING"
    AZIMUTHAL_EXITING = "AZIMUTHAL-EXITING"
    RELATIVE_SLOPE = "RELATIVE-SLOPE"

    # wagl.reflectance
    REFLECTANCE_FMT = "REFLECTANCE/{product}/{band_name}"

    # wagl.temperature
    TEMPERATURE_FMT = "THERMAL/{product}/{band_name}"

    # wagl.terrain_shadow_masks
    SELF_SHADOW = "SELF-SHADOW"
    CAST_SHADOW_FMT = "CAST-SHADOW-{source}"
    COMBINED_SHADOW = "COMBINED-TERRAIN-SHADOW"

    # wagl.slope_aspect
    SLOPE = "SLOPE"
    ASPECT = "ASPECT"

    # wagl.dsm
    DSM = "DSM"
    DSM_SMOOTHED = "DSM-SMOOTHED"

    # wagl.interpolation
    INTERPOLATION_FMT = "{coefficient}/{band_name}"

    # wagl.modtran
    MODTRAN_INPUT = "MODTRAN-INPUT-DATA"
    FLUX = "FLUX"
    ALTITUDES = "ALTITUDES"
    SOLAR_IRRADIANCE = "SOLAR-IRRADIANCE"
    UPWARD_RADIATION_CHANNEL = "UPWARD-RADIATION-CHANNEL"
    DOWNWARD_RADIATION_CHANNEL = "DOWNWARD-RADIATION-CHANNEL"
    CHANNEL = "CHANNEL"
    NBAR_COEFFICIENTS = "NBAR-COEFFICIENTS"
    SBT_COEFFICIENTS = "SBT-COEFFICIENTS"

    # wagl.pq
    PQ_FMT = "PIXEL-QUALITY/{produt}/PIXEL-QUALITY"

    # metadata
    METADATA = "METADATA"
    CURRENT_METADATA = "CURRENT"
    NBAR_YAML = "METADATA/NBAR-METADATA"
    PQ_YAML = "METADATA/PQ-METADATA"
    SBT_YAML = "METADATA/SBT-METADATA"


class GroupName(Enum):
    """
    Defines the group names or format descriptors, that are used
    for creating and accessing throughout the code base.
    """

    LON_LAT_GROUP = "LONGITUDE-LATITUDE"
    SAT_SOL_GROUP = "SATELLITE-SOLAR"
    ANCILLARY_GROUP = "ANCILLARY"
    ANCILLARY_AVG_GROUP = "AVERAGED-ANCILLARY"
    ATMOSPHERIC_INPUTS_GRP = "ATMOSPHERIC-INPUTS"
    ATMOSPHERIC_RESULTS_GRP = "ATMOSPHERIC-RESULTS"
    COEFFICIENTS_GROUP = "ATMOSPHERIC-COEFFICIENTS"
    INTERP_GROUP = "INTERPOLATED-ATMOSPHERIC-COEFFICIENTS"
    ELEVATION_GROUP = "ELEVATION"
    SLP_ASP_GROUP = "SLOPE-ASPECT"
    INCIDENT_GROUP = "INCIDENT-ANGLES"
    EXITING_GROUP = "EXITING-ANGLES"
    REL_SLP_GROUP = "RELATIVE-SLOPE"
    SHADOW_GROUP = "SHADOW-MASKS"
    STANDARD_GROUP = "STANDARDISED-PRODUCTS"


def find(h5_obj, dataset_class=""):
    """
    Given an h5py `Group`, `File` (opened file id; fid),
    recursively list all objects or optionally only list
    `h5py.Dataset` objects matching a given class, for example:

        * IMAGE
        * TABLE
        * SCALAR

    :param h5_obj:
        A h5py `Group` or `File` object to use as the
        entry point from which to start listing the contents.

    :param dataset_class:
        A `str` containing a CLASS name identifier, eg:

        * IMAGE
        * TABLE
        * SCALAR

        Default is an empty string `''`.

    :return:
        A `list` containing the pathname to all matching objects.
    """

    def _find(items, dataset_class, name, obj):
        """
        An internal utility to find objects matching `dataset_class`.
        """
        if obj.attrs.get("CLASS") == dataset_class:
            items.append(name)

    items = []
    h5_obj.visititems(partial(_find, items, dataset_class))

    return items


def provider_reference_info(granule, wagl_tags):
    """
    Extracts provider reference metadata
    Supported platforms are:
        * LANDSAT
        * SENTINEL2
    :param granule:
        A string referring to the name of the capture

    :return:
        Dictionary; contains satellite reference if identified
    """
    provider_info = {}
    matches = None
    if "LANDSAT" in wagl_tags["source_datasets"]["platform_id"]:
        matches = re.match(r"L\w\d(?P<reference_code>\d{6}).*", granule)
    elif "SENTINEL_2" in wagl_tags["source_datasets"]["platform_id"]:
        matches = re.match(r".*_T(?P<reference_code>\d{1,2}[A-Z]{3})_.*", granule)

    if matches:
        provider_info.update(**matches.groupdict())
    return provider_info


def merge_metadata(
    level1_tags: DatasetDoc, wagl_tags, granule, image_paths, **antecedent_tags
):
    """
    Combine the metadata from input sources and output
    into a single ARD metadata yaml.
    """

    platform: str = level1_tags.properties["eo:platform"]
    instrument: str = level1_tags.properties["eo:instrument"]

    # TODO have properly defined product types for the ARD product
    ptype = {
        "LANDSAT-5": "L5ARD",
        "LANDSAT-7": "L7ARD",
        "LANDSAT-8": "L8ARD",
        "SENTINEL-2A": "S2MSIARD",
        "SENTINEL-2B": "S2MSIARD",
    }

    # TODO: resolve common software version for fmask and gqa
    software_versions = wagl_tags["software_versions"]

    software_versions["eodatasets"] = {
        "repo_url": TESP_REPO_URL,
        "version": TESP_VERSION,
    }

    # for Landsat, from_dt and to_dt in ARD-METADATA is populated from max and min timedelta values
    if platform.startswith("landsat"):

        # pylint: disable=too-many-function-args
        def interpret_landsat_temporal_extent():
            """
            Landsat imagery only provides a center datetime; a time range can be derived
            from the timedelta dataset
            """

            center_dt = np.datetime64(level1_tags.datetime)
            from_dt = center_dt + np.timedelta64(
                int(float(wagl_tags.pop("timedelta_min")) * 1000000), "us"
            )
            to_dt = center_dt + np.timedelta64(
                int(float(wagl_tags.pop("timedelta_max")) * 1000000), "us"
            )

            level2_extent = {
                "center_dt": "{}Z".format(center_dt),
                "geometry": level1_tags.geometry.__geo_interface__,
                "from_dt": "{}Z".format(from_dt),
                "to_dt": "{}Z".format(to_dt),
            }

            return level2_extent

        level2_extent = interpret_landsat_temporal_extent()
    else:
        level2_extent = level1_tags.geometry.__geo_interface__
        level2_extent["center_dt"] = level1_tags.datetime

    # TODO: extend yaml document to include fmask and gqa yamls
    merged_yaml = {
        "algorithm_information": wagl_tags["algorithm_information"],
        "system_information": wagl_tags["system_information"],
        "id": str(uuid.uuid4()),
        "processing_level": "Level-2",
        "product_type": ptype[platform],
        "platform": {"code": platform},
        "instrument": {"name": instrument},
        "format": {"name": "GeoTIFF"},
        "tile_id": granule,
        "extent": level2_extent,
        "grid_spatial": level1_tags.geometry.__geo_interface__,
        "image": {"bands": image_paths},
        "lineage": {
            "ancillary": wagl_tags["ancillary"],
            "source_datasets": {"level1": [level1_tags.id]},
        },
    }

    # Configured to handle gqa and fmask antecedent tasks
    for task_name, task_md in antecedent_tags.items():
        if "software_versions" in task_md:
            for key, value in task_md.pop("software_versions").items():
                software_versions[key] = value  # This fails on key conflicts

        # Check for valid metadata after merging the software versions
        if task_md:
            merged_yaml[task_name] = task_md

    provider_info = provider_reference_info(granule, wagl_tags)
    if provider_info:
        merged_yaml["provider"] = provider_info

    merged_yaml["software_versions"] = software_versions

    return merged_yaml


def contiguity(fname) -> Tuple[numpy.ndarray, GeoBox]:
    """
    Write a contiguity mask file based on the intersection of valid data pixels across all
    bands from the input file and returns with the geobox of the source dataset
    """
    with rasterio.open(fname) as ds:
        geobox = GeoBox.from_rio(ds)
        yblock, xblock = ds.block_shapes[0]
        ones = np.ones((ds.height, ds.width), dtype="uint8")
        for band in ds.indexes:
            ones &= ds.read(band) > 0

    return ones, geobox


def quicklook(fname, out_fname, src_min, src_max, out_min=0, out_max=255):
    """
    Generate a quicklook image applying a linear contrast enhancement.
    Outputs will be converted to Uint8.
    If the input image has a valid no data value, the no data will
    be set to 0 in the output image.
    Any non-contiguous data across the colour domain, will be set to
    zero.
    The output is a tiled GeoTIFF with JPEG compression, utilising the
    YCBCR colour model, as well as a mask band.
    This routine attempts to minimise memory consumption, as such
    it reads data as needed on-the-fly, and doesn't retain all colour
    bands in memory.
    The same can't be said for writing to disk as this'll be in
    rasterio's code. The pixel interleaved nature of JPEG compression
    might require (on rasterio's end) to have all bands in memory.
    Extra code could be written to do I/O utilising the same output
    tile size in order to only have (y_size, x_size, bands) in memory
    at a time.

    :param fname:
        A `str` containing the file pathname to an image containing
        the relevant data to extract.

    :param out_fname:
        A `str` containing the file pathname to where the quicklook
        image will be saved.

    :param src_min:
        An integer/float containing the minimum data value to be
        used as input into contrast enhancement.

    :param src_max:
        An integer/float containing the maximum data value to be
        used as input into the contrast enhancement.

    :param out_min:
        An integer specifying the minimum output value that `src_min`
        will be scaled to. Default is 0.

    :param out_max:
        An integer specifying the maximum output value that `src_max`
        will be scaled to. Default is 255.

    :return:
        None; The output will be written directly to disk.
        The output datatype will be `UInt8`.
    """
    with rasterio.open(fname) as ds:

        # no data locations
        nulls = numpy.zeros((ds.height, ds.width), dtype="bool")
        for band in range(1, 4):
            nulls |= ds.read(band) == ds.nodata

        kwargs = {
            "driver": "GTiff",
            "height": ds.height,
            "width": ds.width,
            "count": 3,
            "dtype": "uint8",
            "crs": ds.crs,
            "transform": ds.transform,
            "nodata": 0,
            "compress": "jpeg",
            "photometric": "YCBCR",
            "tiled": "yes",
        }

        # Only set blocksize on larger imagery; enables reduced resolution processing
        if ds.height > 512 and ds.width > 512:
            kwargs["blockxsize"] = 512
            kwargs["blockysize"] = 512

        with rasterio.open(out_fname, "w", **kwargs) as out_ds:
            for band in range(1, 4):
                scaled = rescale_intensity(
                    ds.read(band),
                    in_range=(src_min, src_max),
                    out_range=(out_min, out_max),
                )
                scaled = scaled.astype("uint8")
                scaled[nulls] = 0

                out_ds.write(scaled, band)


def _gls_version(ref_fname):
    # TODO a more appropriate method of version detection and/or population of metadata
    if "GLS2000_GCP_SCENE" in ref_fname:
        gls_version = "GLS_v1"
    else:
        gls_version = "GQA_v3"

    return gls_version


yaml.add_representer(numpy.int8, Representer.represent_int)
yaml.add_representer(numpy.uint8, Representer.represent_int)
yaml.add_representer(numpy.int16, Representer.represent_int)
yaml.add_representer(numpy.uint16, Representer.represent_int)
yaml.add_representer(numpy.int32, Representer.represent_int)
yaml.add_representer(numpy.uint32, Representer.represent_int)
yaml.add_representer(numpy.int, Representer.represent_int)
yaml.add_representer(numpy.int64, Representer.represent_int)
yaml.add_representer(numpy.uint64, Representer.represent_int)
yaml.add_representer(numpy.float, Representer.represent_float)
yaml.add_representer(numpy.float32, Representer.represent_float)
yaml.add_representer(numpy.float64, Representer.represent_float)
yaml.add_representer(numpy.ndarray, Representer.represent_list)

ALIAS_FMT = {
    "LAMBERTIAN": "lambertian_{}",
    "NBAR": "nbar_{}",
    "NBART": "nbart_{}",
    "SBT": "sbt_{}",
}
LEVELS = [8, 16, 32]
FILENAME_TIF_BAND = re.compile(
    r"(?P<prefix>(?:.*_)?)(?P<band_name>B[0-9][A0-9]|B[0-9]*|B[0-9a-zA-z]*)"
    r"(?P<extension>\....)"
)
PRODUCT_SUITE_FROM_GRANULE = re.compile("(L1[GTPCS]{1,2})")
ARD = "ARD"
QA = "QA"
SUPPS = "SUPPLEMENTARY"


def _l1_to_ard(granule):
    return re.sub(PRODUCT_SUITE_FROM_GRANULE, ARD, granule)


def run_command(command: Sequence[Union[str, Path]], work_dir: Path):
    check_call([str(s) for s in command], cwd=str(work_dir))


def _clean(alias):
    """
    A quick fix for cleaning json unfriendly alias strings.
    """
    replace = {"-": "_", "[": "", "]": ""}
    for k, v in replace.items():
        alias = alias.replace(k, v)

    return alias.lower()


def get_cogtif_options(
    shape: Tuple[float, float], overviews=True, blockxsize=None, blockysize=None
):
    """ Returns write_img options according to the source imagery provided
    :param overviews:
        (boolean) sets overview flags in gdal config options
    :param blockxsize:
        (int) override the derived base blockxsize in cogtif conversion
    :param blockysize:
        (int) override the derived base blockysize in cogtif conversion

    returns a dict {'options': {}, 'config_options': {}}
    """

    # TODO Standardizing the Sentinel-2's overview tile size with external inputs

    options = {"compress": "deflate", "zlevel": 4}
    config_options = {}

    # Fallback to 512 value
    blockysize = blockysize or 512
    blockxsize = blockxsize or 512

    if shape[0] <= 512 and shape[1] <= 512:
        # Do not set block sizes for small imagery
        pass
    elif shape[1] <= 512:
        options["blockysize"] = min(blockysize, 512)
        # Set blockxsize to power of 2 rounded down
        options["blockxsize"] = int(2 ** (blockxsize.bit_length() - 1))
        # gdal does not like a x blocksize the same as the whole dataset
        if options["blockxsize"] == blockxsize:
            options["blockxsize"] = int(options["blockxsize"] / 2)
    else:
        if shape[1] == blockxsize:
            # dataset does not have an internal tiling layout
            # set the layout to a 512 block size
            blockxsize = 512
            blockysize = 512
            if overviews:
                config_options["GDAL_TIFF_OVR_BLOCKSIZE"] = blockxsize

        options["blockxsize"] = blockxsize
        options["blockysize"] = blockysize
        options["tiled"] = "yes"

    if overviews:
        options["copy_src_overviews"] = "yes"

    return {"options": options, "config_options": config_options}


def generate_tiles(samples, lines, xtile=None, ytile=None):
    """
    Generates a list of tile indices for a 2D array.

    :param samples:
        An integer expressing the total number of samples in an array.

    :param lines:
        An integer expressing the total number of lines in an array.

    :param xtile:
        (Optional) The desired size of the tile in the x-direction.
        Default is all samples

    :param ytile:
        (Optional) The desired size of the tile in the y-direction.
        Default is min(100, lines) lines.

    :return:
        Each tuple in the generator contains
        ((ystart,yend),(xstart,xend)).

    Example:

        >>> from wagl.tiling import generate_tiles
        >>> tiles = generate_tiles(8624, 7567, xtile=1000, ytile=400)
        >>> for tile in tiles:
        >>>     ystart = int(tile[0][0])
        >>>     yend = int(tile[0][1])
        >>>     xstart = int(tile[1][0])
        >>>     xend = int(tile[1][1])
        >>>     xsize = int(xend - xstart)
        >>>     ysize = int(yend - ystart)
        >>>     # When used to read data from disk
        >>>     subset = gdal_indataset.ReadAsArray(xstart, ystart, xsize, ysize)
        >>>     # The same method can be used to write to disk.
        >>>     gdal_outdataset.WriteArray(array, xstart, ystart)
        >>>     # A rasterio dataset
        >>>     subset = rio_ds.read([4, 3, 2], window=tile)
        >>>     # Or simply move the tile window across an array
        >>>     subset = array[ystart:yend,xstart:xend] # 2D
        >>>     subset = array[:,ystart:yend,xstart:xend] # 3D
    """

    def create_tiles(samples, lines, xstart, ystart):
        """
        Creates a generator object for the tiles.
        """
        for ystep in ystart:
            if ystep + ytile < lines:
                yend = ystep + ytile
            else:
                yend = lines
            for xstep in xstart:
                if xstep + xtile < samples:
                    xend = xstep + xtile
                else:
                    xend = samples
                yield ((ystep, yend), (xstep, xend))

    # check for default or out of bounds
    if xtile is None or xtile < 0:
        xtile = samples
    if ytile is None or ytile < 0:
        ytile = min(100, lines)

    xstart = numpy.arange(0, samples, xtile)
    ystart = numpy.arange(0, lines, ytile)

    tiles = create_tiles(samples, lines, xstart, ystart)

    return tiles


def write_img(
    array,
    filename,
    driver="GTiff",
    geobox=None,
    nodata=None,
    tags=None,
    options=None,
    levels=None,
    resampling=Resampling.nearest,
    config_options=None,
):
    """
    Writes a 2D/3D image to disk using rasterio.

    :param array:
        A 2D/3D NumPy array.

    :param filename:
        A string containing the output file name.

    :param driver:
        A string containing a GDAL compliant image driver. Default is
        'GTiff'.

    :param geobox:
        An instance of a GriddedGeoBox object.

    :param nodata:
        A value representing the no data value for the array.

    :param tags:
        A dictionary of dataset-level metadata.

    :param options:
        A dictionary containing other dataset creation options.
        See creation options for the respective GDAL formats.

    :param levels:
        build overviews/pyramids according to levels

    :param resampling:
        If levels is set, build overviews using a resampling method
        from `rasterio.enums.Resampling`
        Default is `Resampling.nearest`.

    :param config_options:
        A dictionary containing the options to configure GDAL's
        environment's default configurations

    :notes:
        If array is an instance of a `h5py.Dataset`, then the output
        file will include blocksizes based on the `h5py.Dataset's`
        chunks. To override the blocksizes, specify them using the
        `options` keyword. Eg {'blockxsize': 512, 'blockysize': 512}.
    """
    # Get the datatype of the array
    dtype = array.dtype.name

    # Check for excluded datatypes
    excluded_dtypes = ["int64", "int8", "uint64"]
    if dtype in excluded_dtypes:
        msg = "Datatype not supported: {dt}".format(dt=dtype)
        raise TypeError(msg)

    # convert any bools to uin8
    if dtype == "bool":
        array = np.uint8(array)
        dtype = "uint8"

    ndims = array.ndim
    dims = array.shape

    # Get the (z, y, x) dimensions (assuming BSQ interleave)
    if ndims == 2:
        samples = dims[1]
        lines = dims[0]
        bands = 1
    elif ndims == 3:
        samples = dims[2]
        lines = dims[1]
        bands = dims[0]
    else:
        raise IndexError(
            "Input array is not of 2 or 3 dimensions. Got {dims}".format(dims=ndims)
        )

    # If we have a geobox, then retrieve the geotransform and projection
    if geobox is not None:
        transform = geobox.transform
        projection = geobox.crs_wkt
    else:
        transform = None
        projection = None

    # compression predictor choices
    predictor = {
        "int8": 2,
        "uint8": 2,
        "int16": 2,
        "uint16": 2,
        "int32": 2,
        "uint32": 2,
        "int64": 2,
        "uint64": 2,
        "float32": 3,
        "float64": 3,
    }

    kwargs = {
        "count": bands,
        "width": samples,
        "height": lines,
        "crs": projection,
        "transform": transform,
        "dtype": dtype,
        "driver": driver,
        "nodata": nodata,
        "predictor": predictor[dtype],
    }

    if isinstance(array, h5py.Dataset):
        # TODO: if array is 3D get x & y chunks
        if array.chunks[1] == array.shape[1]:
            # GDAL doesn't like tiled or blocksize options to be set
            # the same length as the columns (probably true for rows as well)
            array = array[:]
        else:
            y_tile, x_tile = array.chunks
            tiles = generate_tiles(samples, lines, x_tile, y_tile)

            if "tiled" in options:
                kwargs["blockxsize"] = options.pop("blockxsize", x_tile)
                kwargs["blockysize"] = options.pop("blockysize", y_tile)

    # the user can override any derived blocksizes by supplying `options`
    # handle case where no options are provided
    options = options or {}
    for key in options:
        kwargs[key] = options[key]

    def _rasterio_write_raster(filename):
        """
        This is a wrapper around rasterio writing tiles to
        enable writing to a temporary location before rearranging
        the overviews within the file by gdal when required
        """
        with rasterio.open(filename, "w", **kwargs) as outds:
            if bands == 1:
                if isinstance(array, h5py.Dataset):
                    for tile in tiles:
                        idx = (
                            slice(tile[0][0], tile[0][1]),
                            slice(tile[1][0], tile[1][1]),
                        )
                        outds.write(array[idx], 1, window=tile)
                else:
                    outds.write(array, 1)
            else:
                if isinstance(array, h5py.Dataset):
                    for tile in tiles:
                        idx = (
                            slice(tile[0][0], tile[0][1]),
                            slice(tile[1][0], tile[1][1]),
                        )
                        subs = array[:, idx[0], idx[1]]
                        for i in range(bands):
                            outds.write(subs[i], i + 1, window=tile)
                else:
                    for i in range(bands):
                        outds.write(array[i], i + 1)
            if tags is not None:
                outds.update_tags(**tags)

            # overviews/pyramids to disk
            if levels:
                outds.build_overviews(levels, resampling)

    if not levels:
        # write directly to disk without rewriting with gdal
        _rasterio_write_raster(filename)
    else:
        with tempfile.TemporaryDirectory() as tmpdir:
            out_fname = pjoin(tmpdir, basename(filename))

            # first write to a temporary location
            _rasterio_write_raster(out_fname)
            # Creates the file at filename with the configured options
            # Will also move the overviews to the start of the file
            cmd = [
                "gdal_translate",
                "-co",
                "{}={}".format("PREDICTOR", predictor[dtype]),
            ]

            for key, value in options.items():
                cmd.extend(["-co", "{}={}".format(key, value)])

            if config_options:
                for key, value in config_options.items():
                    cmd.extend(["--config", "{}".format(key), "{}".format(value)])

            cmd.extend([out_fname, filename])
            subprocess.check_call(cmd, cwd=dirname(filename))


def write_tif_from_dataset(
    dataset: h5py.Dataset,
    out_fname,
    options,
    config_options,
    overviews=True,
    nodata=None,
    geobox=None,
):
    """
    Method to write a h5 dataset or numpy array to a tif file
    :param dataset:
        h5 dataset containing a numpy array or numpy array
        Dataset will map to the raster data

    :param out_fname:
        destination of the tif

    :param options:
        dictionary of options provided to gdal

    :param config_options:
        dictionary of configurations provided to gdal

    :param overviews:
        boolean flag to create overviews
        default (True)

    returns the out_fname param
    """
    if hasattr(dataset, "chunks"):
        data = dataset[:]
    else:
        data = dataset

    if nodata is None and hasattr(dataset, "attrs"):
        nodata = dataset.attrs.get("no_data_value")
    if geobox is None:
        geobox = GeoBox.from_h5(dataset)

    # path existence
    if not exists(dirname(out_fname)):
        os.makedirs(dirname(out_fname))

    write_img(
        data,
        out_fname,
        levels=LEVELS,
        nodata=nodata,
        geobox=geobox,
        resampling=Resampling.average,
        options=options,
        config_options=config_options,
    )

    return out_fname


def write_tif_from_file(
    input_image: Path,
    out_fname: str,
    options: Dict[str, Any],
    config_options: Dict[str, Any],
    overviews: bool = True,
):
    """
    Compatible interface for writing (cog)tifs from a source file
    :param input_image:
        path to the source file

    :param out_fname:
        destination of the tif

    :param options:
        dictionary of options provided to gdal

    :param config_options:
        dictionary of configurations provided to gdal

    :param overviews:
        boolean flag to create overviews
        default (True)

    returns the out_fname param
    """

    with tempfile.TemporaryDirectory(
        dir=dirname(out_fname), prefix="cogtif-"
    ) as tmpdir:
        command = ["gdaladdo", "-clean", input_image]
        run_command(command, tmpdir)
        if overviews:
            command = ["gdaladdo", "-r", "mode", input_image]
            command.extend([str(l) for l in LEVELS])
            run_command(command, tmpdir)
        command = ["gdal_translate", "-of", "GTiff"]

        for key, value in options.items():
            command.extend(["-co", "{}={}".format(key, value)])

        if config_options:
            for key, value in config_options.items():
                command.extend(["--config", "{}".format(key), "{}".format(value)])

        command.extend([input_image, out_fname])

        run_command(command, input_image.parent)

    return out_fname


def _versions(gverify_executable):
    gverify_version = gverify_executable.split("_")[-1]
    base_info = {
        "software_versions": {
            "eugl": {
                "version": EUGL_VERSION,
                "repo_url": "git@github.com:OpenDataCubePipelines/eugl.git",
            }
        }
    }
    base_info["software_versions"]["fmask"] = {
        "version": FMASK_VERSION,
        "repo_url": FMASK_REPO_URL,
    }
    base_info["software_versions"]["gverify"] = {"version": gverify_version}
    return base_info


def get_img_dataset_info(dataset: h5py.Dataset, path: Path, layer=1):
    """
    Returns metadata for raster datasets
    """
    geobox = GeoBox.from_h5(dataset)
    return {
        "path": path,
        "layer": layer,
        "info": {
            "width": geobox.x_size,
            "height": geobox.y_size,
            "geotransform": list(geobox.transform.to_gdal()),
        },
    }


def unpack_products(product_list, level1: DatasetDoc, h5group, outdir):
    """
    Unpack and package the NBAR and NBART products.
    """
    # listing of all datasets of IMAGE CLASS type
    img_paths = find(h5group, "IMAGE")

    # relative paths of each dataset for ODC metadata doc
    rel_paths = {}

    # TODO pass products through from the scheduler rather than hard code
    print(" ".join(level1.measurements.keys()))
    for product in product_list:
        secho(f"\n\nStarting {product}", fg="blue")
        for pathname in [p for p in img_paths if "/{}/".format(product) in p]:
            secho(f"\n\nPath {pathname}", fg="blue")
            dataset = h5group[pathname]
            for k in dataset.attrs:
                print(f"\t{k}: {dataset.attrs.get(k)}")

            band_name = dataset.attrs["alias"].lower().replace("-", "")
            base_fname = basename(level1.measurements[band_name].path)

            match_dict = FILENAME_TIF_BAND.match(base_fname).groupdict()
            fname = "{}{}_{}{}".format(
                match_dict.get("prefix"),
                product,
                match_dict.get("band_name"),
                match_dict.get("extension"),
            )
            rel_path = pjoin(product, _l1_to_ard(fname))
            out_fname = pjoin(outdir, rel_path)

            _cogtif_args = get_cogtif_options(dataset.shape, overviews=True)
            write_tif_from_dataset(dataset, out_fname, **_cogtif_args)

            # alias name for ODC metadata doc
            alias = _clean(ALIAS_FMT[product].format(dataset.attrs["alias"]))

            # Band Metadata
            rel_paths[alias] = get_img_dataset_info(dataset, rel_path)

    # retrieve metadata
    scalar_paths = find(h5group, "SCALAR")
    pathnames = [pth for pth in scalar_paths if "NBAR-METADATA" in pth]

    def tags():
        result = yaml.load(h5group[pathnames[0]][()])
        for path in pathnames[1:]:
            other = yaml.load(h5group[path][()])
            result["ancillary"].update(other["ancillary"])
        return result

    return tags(), rel_paths


def unpack_supplementary(granule, h5group: h5py.Group, outdir, cogtif_args):
    """
    Unpack the angles + other supplementary datasets produced by wagl.
    Currently only the mode resolution group gets extracted.
    """

    def _write(
        dataset_names, h5_group, granule_id, basedir, cogtif=False, cogtif_args=None
    ):
        """
        An internal util for serialising the supplementary
        H5Datasets to tif.
        """
        fmt = "{}_{}.TIF"
        paths = {}
        for dname in dataset_names:
            rel_path = pjoin(basedir, fmt.format(granule_id, dname.replace("-", "_")))
            out_fname = pjoin(outdir, rel_path)
            dset = h5_group[dname]
            alias = _clean(dset.attrs["alias"])
            paths[alias] = get_img_dataset_info(dset, rel_path)
            write_tif_from_dataset(dset, out_fname, **cogtif_args)

        return paths

    res_grps = [g for g in h5group.keys() if g.startswith("RES-GROUP-")]
    if len(res_grps) != 1:
        raise NotImplementedError(f"expected one res group, got {res_grps!r}")
    [res_grp] = res_grps

    grn_id = _l1_to_ard(granule)

    # relative paths of each dataset for ODC metadata doc
    rel_paths = {}

    # satellite and solar angles
    grp = h5group[ppjoin(res_grp, GroupName.SAT_SOL_GROUP.value)]
    dnames = [
        DatasetName.SATELLITE_VIEW.value,
        DatasetName.SATELLITE_AZIMUTH.value,
        DatasetName.SOLAR_ZENITH.value,
        DatasetName.SOLAR_AZIMUTH.value,
        DatasetName.RELATIVE_AZIMUTH.value,
        DatasetName.TIME.value,
    ]
    paths = _write(dnames, grp, grn_id, SUPPS, cogtif=False, cogtif_args=cogtif_args)
    for key in paths:
        rel_paths[key] = paths[key]

    # timedelta data
    timedelta_data = grp[DatasetName.TIME.value]

    # incident angles
    grp = h5group[ppjoin(res_grp, GroupName.INCIDENT_GROUP.value)]
    dnames = [DatasetName.INCIDENT.value, DatasetName.AZIMUTHAL_INCIDENT.value]
    paths = _write(dnames, grp, grn_id, SUPPS, cogtif=False, cogtif_args=cogtif_args)
    for key in paths:
        rel_paths[key] = paths[key]

    # exiting angles
    grp = h5group[ppjoin(res_grp, GroupName.EXITING_GROUP.value)]
    dnames = [DatasetName.EXITING.value, DatasetName.AZIMUTHAL_EXITING.value]
    paths = _write(dnames, grp, grn_id, SUPPS, cogtif=False, cogtif_args=cogtif_args)
    for key in paths:
        rel_paths[key] = paths[key]

    # relative slope
    grp = h5group[ppjoin(res_grp, GroupName.REL_SLP_GROUP.value)]
    dnames = [DatasetName.RELATIVE_SLOPE.value]
    paths = _write(dnames, grp, grn_id, SUPPS, cogtif=False, cogtif_args=cogtif_args)
    for key in paths:
        rel_paths[key] = paths[key]

    # terrain shadow
    grp = h5group[ppjoin(res_grp, GroupName.SHADOW_GROUP.value)]
    dnames = [DatasetName.COMBINED_SHADOW.value]
    paths = _write(dnames, grp, grn_id, QA, cogtif=True, cogtif_args=cogtif_args)
    for key in paths:
        rel_paths[key] = paths[key]

    # TODO do we also include slope and aspect?

    return rel_paths, timedelta_data


def create_contiguity(product_list, level1: DatasetDoc, granule, outdir, cogtif_args):
    """
    Create the contiguity (all pixels valid) dataset.
    """
    # TODO: Actual res?
    res = level1.properties["eo:gsd"]

    grn_id = _l1_to_ard(granule)

    nbar_contiguity = None
    # relative paths of each dataset for ODC metadata doc
    rel_paths = {}

    with tempfile.TemporaryDirectory(dir=outdir, prefix="contiguity-") as tmpdir:
        for product in product_list:
            search_path = pjoin(outdir, product)
            fnames = [
                str(f)
                for f in Path(search_path).glob("*.TIF")
                if "QUICKLOOK" not in str(f)
            ]

            # quick work around for products that aren't being packaged
            if not fnames:
                continue

            # output filename
            base_fname = "{}_{}_CONTIGUITY.TIF".format(grn_id, product)
            rel_path = pjoin(QA, base_fname)
            out_fname = pjoin(outdir, rel_path)

            if not exists(dirname(out_fname)):
                os.makedirs(dirname(out_fname))

            alias = ALIAS_FMT[product].format("contiguity")

            # temp vrt
            tmp_fname = pjoin(tmpdir, "{}.vrt".format(product))
            cmd = [
                "gdalbuildvrt",
                "-resolution",
                "user",
                "-tr",
                str(res),
                str(res),
                "-separate",
                tmp_fname,
            ]
            cmd.extend(fnames)
            run_command(cmd, tmpdir)

            # contiguity mask for nbar product
            contiguity_data, geobox = contiguity(tmp_fname)
            write_tif_from_dataset(
                contiguity_data, out_fname, geobox=geobox, **cogtif_args
            )

            if base_fname.endswith("NBAR_CONTIGUITY.TIF"):
                nbar_contiguity = contiguity_data
            del contiguity_data

            with rasterio.open(out_fname) as ds:
                rel_paths[alias] = get_img_dataset_info(ds, rel_path)

    return rel_paths, nbar_contiguity


def create_checksum(outdir):
    """
    Create the checksum file.
    """
    out_fname = pjoin(outdir, "checksum.sha1")
    c = PackageChecksum()
    c.add(outdir)
    c.write(out_fname)
    return out_fname


def package(
    l1_path: Path,
    antecedents: Dict[str, Path],
    outdir: str,
    granule: str,
    products=("NBAR", "NBART", "LAMBERTIAN", "SBT"),
):
    """
    Package an L2 product.

    :param l1_path:
        A string containing the full file pathname to the Level-1
        dataset.

    :param antecedents:
        A dictionary describing antecedent task outputs
        (currently supporting wagl, eugl-gqa, eugl-fmask)
        to package.

    :param outdir:
        A string containing the full file pathname to the directory
        that will contain the packaged Level-2 datasets.

    :param granule:
        The identifier for the granule

    :param products:
        A list of imagery products to include in the package.
        Defaults to all products.

    :return:
        None; The packages will be written to disk directly.
    """

    antecedent_metadata = {}
    # get sensor platform

    level1 = serialise.from_path(l1_path)

    with h5py.File(antecedents["wagl"], "r") as fid:
        grn_id = _l1_to_ard(granule)
        out_path = pjoin(outdir, grn_id)

        if not exists(out_path):
            os.makedirs(out_path)

        # TODO: pan band?
        cogtif_args = get_cogtif_options(
            level1.grids[level1.measurements["blue"].grid].shape
        )

        for i in fid.items():
            print(repr(i))
        # unpack the standardised products produced by wagl
        wagl_tags, img_paths = unpack_products(
            products, level1, h5group=fid[granule], outdir=out_path
        )

        # unpack supplementary datasets produced by wagl
        supp_paths, timedelta_data = unpack_supplementary(
            granule, fid[granule], out_path, cogtif_args
        )

        # add in supplementary paths
        for key in supp_paths:
            img_paths[key] = supp_paths[key]

        # file based globbing, so can't have any other tifs on disk
        qa_paths, contiguity_ones_mask = create_contiguity(
            products, level1, granule, out_path, cogtif_args
        )

        # masking the timedelta_data with contiguity mask to get max and min timedelta within the NBAR product
        # footprint for Landsat sensor. For Sentinel sensor, it inherits from level 1 yaml file
        if level1.properties["eo:platform"].startswith("landsat"):
            valid_timedelta_data = numpy.ma.masked_where(
                contiguity_ones_mask == 0, timedelta_data
            )
            wagl_tags["timedelta_min"] = numpy.ma.min(valid_timedelta_data)
            wagl_tags["timedelta_max"] = numpy.ma.max(valid_timedelta_data)

        # add in qa paths
        # for key in qa_paths:
        #     img_paths[key] = qa_paths[key]

        # fmask cogtif conversion
        if "fmask" in antecedents:
            rel_path = pjoin(QA, "{}_FMASK.TIF".format(grn_id))
            fmask_cogtif_out = pjoin(out_path, rel_path)

            # Get cogtif args with overviews

            cogtif_args["options"]["predictor"] = 2
            write_tif_from_file(antecedents["fmask"], fmask_cogtif_out, **cogtif_args)

            antecedent_metadata["fmask"] = {"fmask_version": "TODO"}

            with rasterio.open(fmask_cogtif_out) as ds:
                img_paths["fmask"] = get_img_dataset_info(ds, rel_path)

        # create_quicklook(products, container, out_path)
        # create_readme(out_path)

        # merge all the yaml documents
        if "gqa" in antecedents:
            with antecedents["gqa"].open() as fl:
                antecedent_metadata["gqa"] = yaml.load(fl)
        else:
            antecedent_metadata["gqa"] = {
                "error_message": "GQA has not been configured for this product"
            }

        tags = merge_metadata(
            level1, wagl_tags, granule, img_paths, **antecedent_metadata
        )

        with open(pjoin(out_path, "ARD-METADATA.yaml"), "w") as src:
            yaml.dump(tags, src, default_flow_style=False, indent=4)

        # finally the checksum
        create_checksum(out_path)


def run():
    package(
        l1_path=Path(
            "/home/jez/dea/eo-datasets/wagltest/LT05_L1TP_091084_19930707_20170118_01_T1.yaml"
        ),
        antecedents={
            "wagl": Path(
                "/home/jez/dea/eo-datasets/wagltest/LT50910841993188ASA00.wagl.h5"
            ),
            # 'eugl-gqa',
            # #'eugl-fmask'},
        },
        outdir=os.path.abspath("./wagl-out"),
        granule="LT50910841993188ASA00",
    )


if __name__ == "__main__":
    run()
