#!/usr/bin/env python

import os
import re
import subprocess
import tempfile
import uuid
from copy import deepcopy
from enum import Enum
from functools import partial
from os.path import join as pjoin, basename
from pathlib import Path
from posixpath import join as ppjoin
from subprocess import check_call
from typing import Dict, Any, Tuple, Sequence, Union, List, Generator

import attr
import h5py
import numpy
import numpy as np
import rasterio
import yaml
from affine import Affine
from click import secho
from rasterio.enums import Resampling
from yaml.representer import Representer

from eodatasets.prepare import serialise
from eodatasets.prepare.model import DatasetDoc
from eodatasets.verify import PackageChecksum

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

    def get_img_dataset_info(self, path: Path, layer=1):
        return {
            "path": path.absolute(),
            "layer": layer,
            "info": {
                "width": self.x_size,
                "height": self.y_size,
                "geotransform": list(self.transform.to_gdal()),
            },
        }


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


def find(h5_obj: h5py.Group, dataset_class="") -> List[str]:
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


def provider_reference_info(granule: str, wagl_tags: Dict) -> Dict:
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
    level1_tags: DatasetDoc,
    wagl_tags: Dict,
    granule: str,
    measurements: Dict,
    **antecedent_tags,
) -> Dict:
    """
    Combine the metadata from input sources and output
    into a single ARD metadata yaml.
    """

    platform: str = level1_tags.properties["eo:platform"]
    instrument: str = level1_tags.properties["eo:instrument"]

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
        "product_type": "ard",
        "platform": {"code": platform},
        "instrument": {"name": instrument},
        "format": {"name": "GeoTIFF"},
        "tile_id": granule,
        "extent": level2_extent,
        "grid_spatial": level1_tags.geometry.__geo_interface__,
        "image": {"bands": measurements},
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


def contiguity(fname: Path) -> Tuple[numpy.ndarray, GeoBox]:
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


def _gls_version(ref_fname: str) -> str:
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

LEVELS = [8, 16, 32]
FILENAME_TIF_BAND = re.compile(
    r"(?P<prefix>(?:.*_)?)(?P<band_name>B[0-9][A0-9]|B[0-9]*|B[0-9a-zA-z]*)"
    r"(?P<extension>\....)"
)
PRODUCT_SUITE_FROM_GRANULE = re.compile("(L1[GTPCS]{1,2})")
ARD = "ARD"
QA = "QA"
SUPPS = "SUPPLEMENTARY"


def _l1_to_ard(granule: str) -> str:
    return re.sub(PRODUCT_SUITE_FROM_GRANULE, ARD, granule)


def run_command(command: Sequence[Union[str, Path]], work_dir: Path) -> None:
    check_call([str(s) for s in command], cwd=str(work_dir))


def get_cogtif_options(
    shape: Tuple[float, float],
    overviews: bool = True,
    blockxsize: int = None,
    blockysize: int = None,
) -> Dict:
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


def generate_tiles(
    samples: int, lines: int, xtile: int = None, ytile: int = None
) -> Generator[Tuple[Tuple[int, int], Tuple[int, int]], None, None]:
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

    >>> import pprint
    >>> tiles = generate_tiles(1624, 1567, xtile=1000, ytile=400)
    >>> pprint.pprint(list(tiles))
    [((0, 400), (0, 1000)),
     ((0, 400), (1000, 1624)),
     ((400, 800), (0, 1000)),
     ((400, 800), (1000, 1624)),
     ((800, 1200), (0, 1000)),
     ((800, 1200), (1000, 1624)),
     ((1200, 1567), (0, 1000)),
     ((1200, 1567), (1000, 1624))]
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
    array: numpy.ndarray,
    filename: Path,
    driver="GTiff",
    geobox: GeoBox = None,
    nodata: int = None,
    tags: Dict = None,
    options: Dict = None,
    levels: Sequence[int] = None,
    resampling=Resampling.nearest,
    config_options: Dict = None,
) -> None:
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

    def _rasterio_write_raster(filename: Path):
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
            out_fname = Path(tmpdir) / filename.name

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
            subprocess.check_call(cmd, cwd=str(filename.parent))


def write_tif_from_dataset(
    dataset: h5py.Dataset,
    out_fname: Path,
    options: Dict,
    config_options: Dict,
    nodata: int = None,
    geobox: GeoBox = None,
) -> Path:
    """
    Method to write a h5 dataset or numpy array to a tif file
    :param dataset:
        h5 dataset containing a numpy array or numpy array
        Dataset will map to the raster data

    :param options:
        dictionary of options provided to gdal

    :param config_options:
        dictionary of configurations provided to gdal

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

    out_fname.parent.mkdir(exist_ok=True)

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
    out_fname: Path,
    options: Dict[str, Any],
    config_options: Dict[str, Any],
    overviews: bool = True,
) -> Path:
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

    with tempfile.TemporaryDirectory(dir=out_fname.parent, prefix="cogtif-") as tmpdir:
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


def _versions(gverify_executable: str):
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


def unpack_products(
    product_list: Sequence[str], level1: DatasetDoc, h5group: h5py.Group, outdir: Path
) -> Tuple[Dict, Dict]:
    """
    Unpack and package the NBAR and NBART products.
    """
    # listing of all datasets of IMAGE CLASS type
    img_paths = find(h5group, "IMAGE")
    for p in img_paths:
        print(p)

    # relative paths of each dataset for ODC metadata doc
    measurements = {}

    # TODO pass products through from the scheduler rather than hard code
    for product in product_list:
        secho(f"\n\nStarting {product}", fg="blue")
        for pathname in [p for p in img_paths if "/{}/".format(product) in p]:
            secho(f"\n\nPath {pathname}", fg="blue")
            dataset = h5group[pathname]

            band_name = _clean_alias(dataset)
            base_fname = basename(level1.measurements[band_name.replace("_", "")].path)

            match_dict = FILENAME_TIF_BAND.match(base_fname).groupdict()
            out_fname = (
                outdir
                / product
                / _l1_to_ard(
                    "{}{}_{}{}".format(
                        match_dict.get("prefix"),
                        product,
                        match_dict.get("band_name"),
                        match_dict.get("extension"),
                    )
                )
            )

            _cogtif_args = get_cogtif_options(dataset.shape, overviews=True)
            write_tif_from_dataset(dataset, out_fname, **_cogtif_args)

            # alias name for ODC metadata doc
            alias = f"{product.lower()}_{band_name}"

            # Band Metadata
            measurements[alias] = GeoBox.from_h5(dataset).get_img_dataset_info(
                out_fname
            )

    # retrieve metadata
    scalar_paths = find(h5group, "SCALAR")
    pathnames = [pth for pth in scalar_paths if "NBAR-METADATA" in pth]

    tags = yaml.load(h5group[pathnames[0]][()])
    for path in pathnames[1:]:
        other = yaml.load(h5group[path][()])
        tags["ancillary"].update(other["ancillary"])

    return tags, measurements


def _clean_alias(dataset: h5py.Dataset):
    return dataset.attrs["alias"].lower().replace("-", "_")


def unpack_supplementary(
    granule: str, h5group: h5py.Group, outdir: Path, cogtif_args: Dict
):
    """
    Unpack the angles + other supplementary datasets produced by wagl.
    Currently only the mode resolution group gets extracted.
    """

    def _write(
        dataset_names: Sequence[str],
        h5_group: h5py.Group,
        granule_id: str,
        basedir: str,
        cogtif=False,
        cogtif_args=None,
    ):
        """
        An internal util for serialising the supplementary
        H5Datasets to tif.
        """
        paths = {}
        for dname in dataset_names:
            out_fname = (
                outdir
                / basedir
                / "{}_{}.TIF".format(granule_id, dname.replace("-", "_"))
            )
            dset = h5_group[dname]
            alias = _clean_alias(dset)
            paths[alias] = GeoBox.from_h5(dset).get_img_dataset_info(out_fname)
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


def create_contiguity(
    product_list: Sequence[str],
    level1: DatasetDoc,
    granule: str,
    outdir: Path,
    cogtif_args: Dict,
):
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
            search_path = outdir / product
            fnames = [
                str(f) for f in search_path.glob("*.TIF") if "QUICKLOOK" not in str(f)
            ]

            # quick work around for products that aren't being packaged
            if not fnames:
                continue

            out_fname = outdir / QA / "{}_{}_CONTIGUITY.TIF".format(grn_id, product)
            out_fname.parent.mkdir(exist_ok=True)

            alias = f"{product.lower()}_contiguity"

            # temp vrt
            tmp_fname = Path(tmpdir) / f"{product}.vrt"
            run_command(
                [
                    "gdalbuildvrt",
                    "-resolution",
                    "user",
                    "-tr",
                    str(res),
                    str(res),
                    "-separate",
                    tmp_fname,
                    *fnames,
                ],
                tmpdir,
            )

            # contiguity mask for nbar product
            contiguity_data, geobox = contiguity(tmp_fname)
            write_tif_from_dataset(
                contiguity_data, out_fname, geobox=geobox, **cogtif_args
            )

            if product.lower() == "nbar":
                nbar_contiguity = contiguity_data
            del contiguity_data

            with rasterio.open(out_fname) as ds:
                rel_paths[alias] = GeoBox.from_rio(ds).get_img_dataset_info(out_fname)

    return rel_paths, nbar_contiguity


def package(
    l1_path: Path,
    antecedents: Dict[str, Path],
    outdir: Path,
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
        out_path = outdir / grn_id
        out_path.mkdir(parents=True, exist_ok=True)

        # TODO: pan band?
        cogtif_args = get_cogtif_options(
            level1.grids[level1.measurements["blue"].grid].shape
        )

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
        for key in qa_paths:
            img_paths[key] = qa_paths[key]

        # fmask cogtif conversion
        if "fmask" in antecedents:
            fmask_cogtif_out = out_path / QA / f"{grn_id}_FMASK.TIF"

            # Get cogtif args with overviews
            fmask_cogtif_args = deepcopy(cogtif_args)
            fmask_cogtif_args["options"]["predictor"] = 2
            write_tif_from_file(
                antecedents["fmask"], fmask_cogtif_out, **fmask_cogtif_args
            )

            antecedent_metadata["fmask"] = {"fmask_version": "TODO"}

            with rasterio.open(fmask_cogtif_out) as ds:
                img_paths["fmask"] = GeoBox.from_rio(ds).get_img_dataset_info(
                    fmask_cogtif_out
                )

        # merge all the yaml documents
        if "gqa" in antecedents:
            with antecedents["gqa"].open() as fl:
                antecedent_metadata["gqa"] = yaml.safe_load(fl)
        else:
            antecedent_metadata["gqa"] = {
                "error_message": "GQA has not been configured for this product"
            }

        tags = merge_metadata(
            level1, wagl_tags, granule, img_paths, **antecedent_metadata
        )

        with (out_path / "ARD-METADATA.yaml").open("w") as src:
            yaml.dump(tags, src, default_flow_style=False, indent=4)

        c = PackageChecksum()
        c.add_file(Path(out_path))
        c.write(Path(out_path) / "checksum.sha1")


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
            # 'eugl-fmask',
        },
        outdir=Path("./wagl-out").absolute(),
        granule="LT50910841993188ASA00",
    )


if __name__ == "__main__":
    run()
