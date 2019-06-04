#!/usr/bin/env python

import os
import re
import subprocess
import tempfile
from functools import partial
from os.path import basename
from pathlib import Path
from posixpath import join as ppjoin
from subprocess import check_call
from typing import Dict, Any, Tuple, Sequence, Union, List, Generator

import h5py
import numpy
import numpy as np
import rasterio
import yaml
from click import secho
from rasterio.enums import Resampling
from yaml.representer import Representer

import eodatasets
from eodatasets.prepare.assemble import DatasetAssembler, GridSpec
from eodatasets.prepare import serialise
from eodatasets.prepare.model import DatasetDoc, GridDoc

EUGL_VERSION = "DO_SOMETHING_HERE"

FMASK_VERSION = "DO_SOMETHING_HERE2"
FMASK_REPO_URL = "https://bitbucket.org/chchrsc/python-fmask"

TESP_VERSION = eodatasets.__version__
TESP_REPO_URL = "https://github.com/GeoscienceAustralia/eo-datasets"

os.environ["CPL_ZIP_ENCODING"] = "UTF-8"

LEVELS = [8, 16, 32]
FILENAME_TIF_BAND = re.compile(
    r"(?P<prefix>(?:.*_)?)(?P<band_name>B[0-9][A0-9]|B[0-9]*|B[0-9a-zA-z]*)"
    r"(?P<extension>\....)"
)
PRODUCT_SUITE_FROM_GRANULE = re.compile("(L1[GTPCS]{1,2})")
ARD = "ARD"
QA = "QA"
SUPPS = "SUPPLEMENTARY"


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
            print(repr(type(obj)))

    items = []
    h5_obj.visititems(partial(_find, items, dataset_class))

    return items


def provider_reference_info(p: DatasetAssembler, granule: str):
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
    matches = None
    if p.platform.startwith("landsat"):
        matches = re.match(r"L\w\d(?P<reference_code>\d{6}).*", granule)
    elif p.platform.startwith("sentinel-2"):
        matches = re.match(r".*_T(?P<reference_code>\d{1,2}[A-Z]{3})_.*", granule)

    if matches:
        [reference_code] = matches.groups()
        # TODO name properly
        p["odc:reference_code"] = reference_code


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
    geobox: GridDoc = None,
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
        projection = None
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
    geobox: GridDoc = None,
):
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
        geobox = GridSpec.from_h5(dataset)

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

    # return MeasurementDoc(path=out_fname, grid=geobox)


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
        run_command(["gdaladdo", "-clean", input_image], tmpdir)
        if overviews:
            run_command(
                ["gdaladdo", "-r", "mode", input_image, *[str(l) for l in LEVELS]],
                tmpdir,
            )

        command = ["gdal_translate", "-of", "GTiff"]
        for key, value in options.items():
            command.extend(["-co", "{}={}".format(key, value)])

        if config_options:
            for key, value in config_options.items():
                command.extend(["--config", "{}".format(key), "{}".format(value)])

        command.extend([input_image, out_fname])
        run_command(command, input_image.parent)

    return out_fname


def unpack_products(
    p: DatasetAssembler,
    product_list: Sequence[str],
    level1: DatasetDoc,
    h5group: h5py.Group,
    outdir: Path,
) -> None:
    """
    Unpack and package the NBAR and NBART products.
    """
    # listing of all datasets of IMAGE CLASS type
    img_paths = find(h5group, "IMAGE")

    # TODO pass products through from the scheduler rather than hard code
    for product in product_list:
        secho(f"\n\nStarting {product}", fg="blue")
        for pathname in [p for p in img_paths if "/{}/".format(product) in p]:
            secho(f"Path {pathname}", fg="blue")
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
            p.write_measurement_h5(band_name, dataset)

    pathnames = [pth for pth in (find(h5group, "SCALAR")) if "NBAR-METADATA" in pth]

    tags = yaml.load(h5group[pathnames[0]][()])
    for path in pathnames[1:]:
        other = yaml.load(h5group[path][()])
        tags["ancillary"].update(other["ancillary"])

    p.extend_user_metadata("wagl", tags)


def _clean_alias(dataset: h5py.Dataset):
    return dataset.attrs["alias"].lower().replace("-", "_")


def unpack_supplementary(p: DatasetAssembler, h5group: h5py.Group):
    """
    Unpack the angles + other supplementary datasets produced by wagl.
    Currently only the mode resolution group gets extracted.
    """

    def _write(dataset_names: Sequence[str], h5_group: h5py.Group, basedir: str):
        """
        An internal util for serialising the supplementary
        H5Datasets to tif.
        """
        for dname in dataset_names:
            p.write_measurement_h5(f"{basedir}/{dname}", h5_group[dname])

    res_grps = [g for g in h5group.keys() if g.startswith("RES-GROUP-")]
    if len(res_grps) != 1:
        raise NotImplementedError(f"expected one res group, got {res_grps!r}")
    [res_grp] = res_grps

    grn_id = ""

    # satellite and solar angles
    grp = h5group[ppjoin(res_grp, "SATELLITE-SOLAR")]

    _write(
        [
            "SATELLITE-VIEW",
            "SATELLITE-AZIMUTH",
            "SOLAR-ZENITH",
            "SOLAR-AZIMUTH",
            "RELATIVE-AZIMUTH",
            "TIMEDELTA",
        ],
        grp,
        SUPPS,
    )

    # timedelta data
    timedelta_data = grp["TIMEDELTA"]

    # incident angles

    _write(
        ["INCIDENT", "AZIMUTHAL-INCIDENT"],
        h5group[ppjoin(res_grp, "INCIDENT-ANGLES")],
        SUPPS,
    )

    # exiting angles

    _write(
        ["EXITING", "AZIMUTHAL-EXITING"],
        h5group[ppjoin(res_grp, "EXITING-ANGLES")],
        SUPPS,
    )

    # relative slope

    _write(["RELATIVE-SLOPE"], h5group[ppjoin(res_grp, "RELATIVE-SLOPE")], SUPPS)

    # terrain shadow
    # TODO: this one had cogtif=True? (but was unused in `_write()`)

    _write(["COMBINED-TERRAIN-SHADOW"], h5group[ppjoin(res_grp, "SHADOW-MASKS")], QA)

    # TODO do we also include slope and aspect?

    return timedelta_data


def create_contiguity(
    p: DatasetAssembler,
    product_list: Sequence[str],
    level1: DatasetDoc,
    granule: str,
    outdir: Path,
    timedelta_data,
):
    """
    Create the contiguity (all pixels valid) dataset.
    """
    # TODO: Actual res?
    res = level1.properties["eo:gsd"]

    grn_id = _l1_to_ard(granule)

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
            # contiguity(p, tmp_fname, update_timerange=product.lower() == "nbar")

            # def contiguity(p: DatasetAssembler, product_name: str, fname: Path):
            """
            Write a contiguity mask file based on the intersection of valid data pixels across all
            bands from the input file and returns with the geobox of the source dataset
            """
            with rasterio.open(tmp_fname) as ds:
                geobox = GridSpec.from_rio(ds)
                ones = np.ones((ds.height, ds.width), dtype="uint8")
                for band in ds.indexes:
                    ones &= ds.read(band) > 0

                p.write_measurement_numpy(f"{product.lower()}_contiguity", ones, geobox)

            # masking the timedelta_data with contiguity mask to get max and min timedelta within the NBAR product
            # footprint for Landsat sensor. For Sentinel sensor, it inherits from level 1 yaml file
            if level1.properties["eo:platform"].startswith("landsat"):
                valid_timedelta_data = numpy.ma.masked_where(ones == 0, timedelta_data)

                center_dt = np.datetime64(level1.datetime)
                from_dt = center_dt + np.timedelta64(
                    int(float(numpy.ma.min(valid_timedelta_data)) * 1000000), "us"
                )
                to_dt = center_dt + np.timedelta64(
                    int(float(numpy.ma.max(valid_timedelta_data)) * 1000000), "us"
                )
                p.datetime_range = (from_dt, to_dt)


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

    level1 = serialise.from_path(l1_path)

    with h5py.File(antecedents["wagl"], "r") as fid:
        out_path = outdir / _l1_to_ard(granule)
        out_path.mkdir(parents=True, exist_ok=True)
        with DatasetAssembler(out_path) as p:
            p.add_source_dataset(level1, auto_inherit_properties=True)

            # TODO: pan band?
            # cogtif_args = get_cogtif_options(
            #     level1.grids[level1.measurements["blue"].grid].shape
            # )

            # unpack the standardised products produced by wagl
            unpack_products(p, products, level1, h5group=fid[granule], outdir=out_path)

            # unpack supplementary datasets produced by wagl
            timedelta_data = unpack_supplementary(p, fid[granule])

            # file based globbing, so can't have any other tifs on disk
            create_contiguity(p, products, level1, granule, out_path, timedelta_data)

            # fmask cogtif conversion
            if "fmask" in antecedents:

                # TODO: this one has different predictor settings?
                fmask_cogtif_args_predictor = 2

                p.write_measurement("qa/fmask", antecedents["fmask"])

                # The processing version should be supplied somewhere in their metadata.
                p.note_software_version("fmask_repo", "TODO")

            # merge all the yaml documents
            if "gqa" in antecedents:
                with antecedents["gqa"].open() as fl:
                    p.extend_user_metadata("gqa", yaml.safe_load(fl))

            # TODO better identifiers
            p.note_software_version("eugl", EUGL_VERSION)
            p.note_software_version(FMASK_REPO_URL, FMASK_VERSION)
            # p.note_software_version('gverify', gverify_version)
            p.note_software_version(TESP_REPO_URL, TESP_VERSION)

            # TODO there's probably a real one.
            p["dea:processing_level"] = "Level-2"
            provider_reference_info(p, granule)

            p.finish()


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
