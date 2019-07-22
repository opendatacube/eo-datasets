import os
import shlex
import tempfile
from collections import defaultdict
from pathlib import Path
from subprocess import check_call
from typing import Tuple, Dict, List, Sequence, Optional, Iterable
from typing import Union, Generator

import attr
import h5py
import numpy
import numpy as np
import rasterio
import rasterio.features
import shapely
import shapely.affinity
import shapely.ops
from affine import Affine
from rasterio.crs import CRS
from rasterio.enums import Resampling
from scipy import ndimage
from shapely.geometry.base import BaseGeometry, CAP_STYLE, JOIN_STYLE
from skimage.exposure import rescale_intensity

from eodatasets3.model import GridDoc, MeasurementDoc, DatasetDoc, FileFormat

DEFAULT_OVERVIEWS = (8, 16, 32)


def run_command(
    command: Sequence[Union[str, Path]], work_dir: Path, echo=False
) -> None:
    if echo:
        print(" ".join(shlex.quote(str(s)) for s in command))
    check_call([str(s) for s in command], cwd=str(work_dir))


@attr.s(auto_attribs=True, slots=True, hash=True, frozen=True)
class GridSpec:
    shape: Tuple[int, int]
    transform: Affine

    crs: CRS = attr.ib(
        metadata=dict(doc_exclude=True), default=None, hash=False, cmp=False
    )

    @classmethod
    def from_dataset_doc(cls, ds: DatasetDoc, grid="default") -> "GridSpec":
        g = ds.grids[grid]

        if ds.crs.startswith("epsg:"):
            crs = CRS.from_epsg(ds.crs[5:])
        else:
            crs = CRS.from_wkt(ds.crs)

        return GridSpec(g.shape, g.transform, crs=crs)

    @classmethod
    def from_rio(cls, dataset: rasterio.DatasetReader) -> "GridSpec":
        return cls(shape=dataset.shape, transform=dataset.transform, crs=dataset.crs)

    @property
    def resolution_yx(self):
        return abs(self.transform[4]), abs(self.transform[0])


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


def _common_suffix(names: Iterable[str]) -> str:
    return os.path.commonprefix([s[::-1] for s in names])[::-1]


def _find_a_common_name(names: Sequence[str]) -> Optional[str]:
    """
    If we have a list of band names, can we find a nice name for the group of them?

    (used when naming the grid for a set of bands)

    >>> _find_a_common_name(['nbar_blue', 'nbar_red'])
    'nbar'
    >>> _find_a_common_name(['nbar_band08', 'nbart_band08'])
    'band08'
    >>> _find_a_common_name(['nbar:band08', 'nbart:band08'])
    'band08'
    >>> _find_a_common_name(['panchromatic'])
    'panchromatic'
    >>> _find_a_common_name(['nbar_panchromatic'])
    'nbar_panchromatic'
    >>> # It's ok to find nothing.
    >>> _find_a_common_name(['nbar_blue', 'nbar_red', 'qa'])
    >>> _find_a_common_name(['a', 'b'])
    """

    # If all measurements have a common prefix (like 'nbar_') it makes a nice grid name.
    options = [
        s.strip("_:") for s in (os.path.commonprefix(names), _common_suffix(names))
    ]
    # Pick the longest candidate.
    options.sort(key=len, reverse=True)
    return options[0] or None


class MeasurementRecord:
    """
    Record the information for measurements/images to later write out to metadata.
    """

    def __init__(self):
        # The measurements grouped by their grid.
        # (value is band_name->Path)
        self._measurements_per_grid: Dict[GridSpec, Dict[str, Path]] = defaultdict(dict)
        # Valid data mask per grid, in pixel coordinates.
        self.mask_by_grid: Dict[GridSpec, numpy.ndarray] = {}

    def record_image(
        self,
        name: str,
        grid: GridSpec,
        path: Union[Path, str],
        img: numpy.ndarray,
        nodata=None,
        expand_valid_data=True,
    ):
        for measurements in self._measurements_per_grid.values():
            if name in measurements:
                raise ValueError(
                    f"Duplicate addition of band called {name!r}. "
                    f"Original at {measurements[name]} and now {path}"
                )

        self._measurements_per_grid[grid][name] = path
        if expand_valid_data:
            self._expand_valid_data_mask(grid, img, nodata)

    def _expand_valid_data_mask(self, grid: GridSpec, img: numpy.ndarray, nodata):
        mask = self.mask_by_grid.get(grid)
        new_mask = img != nodata
        if mask is None:
            mask = new_mask
        else:
            mask |= new_mask
        self.mask_by_grid[grid] = mask

    def as_geo_docs(self) -> Tuple[CRS, Dict[str, GridDoc], Dict[str, MeasurementDoc]]:
        """Calculate combined geo information for metadata docs"""

        # Order grids from most to fewest measurements.
        # PyCharm's typing seems to get confused by the sorted() call.
        # noinspection PyTypeChecker
        grids_by_frequency: List[Tuple[GridSpec, Dict[str, Path]]] = sorted(
            self._measurements_per_grid.items(), key=lambda k: len(k[1]), reverse=True
        )

        grid_docs: Dict[str, GridDoc] = {}
        measurement_docs: Dict[str, MeasurementDoc] = {}
        crs = grids_by_frequency[0][0].crs
        for i, (grid, measurements) in enumerate(grids_by_frequency):
            # TODO: CRS equality is tricky. This may not work.
            #       We're assuming a group of measurements specify their CRS
            #       the same way if they are the same.
            if grid.crs != crs:
                raise ValueError(
                    f"Measurements have different CRSes in the same dataset:\n"
                    f"\t{crs.to_string()!r}\n"
                    f"\t{grid.crs.to_string()!r}\n"
                )

            # The grid with the most measurements.
            if i == 0:
                grid_name = "default"
            else:
                grid_name = _find_a_common_name(list(measurements.keys()))
                # If another grid already has this name: TODO: make both grid names more specific?
                if grid_name in grid_docs:
                    raise NotImplementedError(
                        f"Clashing grid names. Needs a recalculation. "
                        f"Name {grid_name!r}, but have {tuple(grid_docs.keys())!r}"
                    )
                # There was no common prefix. Just concat all band names.
                # Perhaps we just fallback to enumeration in these weird cases. grid a, grid b etc....
                if not grid_name:
                    grid_name = "_".join(measurements.keys())

            grid_docs[grid_name] = GridDoc(grid.shape, grid.transform)

            for measurement_name, measurement_path in measurements.items():
                # No measurement groups in the doc: we replace with underscores.
                measurement_name = measurement_name.replace(":", "_")

                measurement_docs[measurement_name] = MeasurementDoc(
                    path=measurement_path,
                    grid=grid_name if grid_name != "default" else None,
                )
        return crs, grid_docs, measurement_docs

    def valid_data(self) -> BaseGeometry:
        geoms = []
        for grid, mask in self.mask_by_grid.items():
            mask = ndimage.binary_fill_holes(mask)

            shape = shapely.ops.unary_union(
                [
                    shapely.geometry.shape(shape)
                    for shape, val in rasterio.features.shapes(
                        mask.astype("uint8"), mask=mask
                    )
                    if val == 1
                ]
            )

            # convex hull
            geom = shape.convex_hull

            # buffer by 1 pixel
            geom = geom.buffer(
                1, cap_style=CAP_STYLE.square, join_style=JOIN_STYLE.bevel
            )

            # simplify with 1 pixel radius
            geom = geom.simplify(1)

            # intersect with image bounding box
            geom = geom.intersection(
                shapely.geometry.box(0, 0, mask.shape[1], mask.shape[0])
            )

            # transform from pixel space into CRS space
            geom = shapely.affinity.affine_transform(
                geom,
                (
                    grid.transform.a,
                    grid.transform.b,
                    grid.transform.d,
                    grid.transform.e,
                    grid.transform.xoff,
                    grid.transform.yoff,
                ),
            )
            geoms.append(geom)

        return shapely.ops.unary_union(geoms)

    def iter_paths(self) -> Generator[Tuple[GridSpec, str, Path], None, None]:
        """All current measurement paths on disk"""
        for grid, measurements in self._measurements_per_grid.items():
            for band_name, path in measurements.items():
                yield grid, band_name, path


@attr.s(auto_attribs=True)
class WriteResult:
    # path: Path

    # The value to put in 'odc:file_format' metadata field.
    file_format: FileFormat

    # size_bytes: int


class FileWrite:
    """
    Write COGs from arrays / files.

    This code is derived from the old eugl packaging code and can probably be improved.
    """

    PREDICTOR_DEFAULTS = {
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

    def __init__(
        self, gdal_options: Dict = None, gdal_config_options: Dict = None
    ) -> None:
        super().__init__()

        self.options = gdal_options or {}
        self.config_options = {
            # Suppress aux.xml files.
            "GDAL_PAM_ENABLED": "NO"
        }
        self.config_options.update(gdal_config_options or {})

    @classmethod
    def from_existing(
        cls,
        shape: Tuple[int, int],
        overviews: bool = True,
        blocksize_yx: Optional[Tuple[int, int]] = None,
        compress="deflate",
        zlevel=4,
    ) -> "FileWrite":
        """ Returns write_img options according to the source imagery provided
        :param overviews:
            (boolean) sets overview flags in gdal config options
        :param blockxsize:
            (int) override the derived base blockxsize in cogtif conversion
        :param blockysize:
            (int) override the derived base blockysize in cogtif conversion

        """
        options = {"compress": compress, "zlevel": zlevel}
        config_options = {}

        y_size, x_size = blocksize_yx or (512, 512)
        # Do not set block sizes for small imagery
        if shape[0] < y_size and shape[1] < x_size:
            pass
        else:
            options["blockxsize"] = x_size
            options["blockysize"] = y_size
            options["tiled"] = "yes"

            if overviews:
                config_options["GDAL_TIFF_OVR_BLOCKSIZE"] = x_size

        if overviews:
            options["copy_src_overviews"] = "yes"

        return FileWrite(options, config_options)

    def write_from_ndarray(
        self,
        array: numpy.ndarray,
        out_filename: Path,
        geobox: GridSpec = None,
        nodata: int = None,
        overview_resampling=Resampling.nearest,
        overviews: Optional[Tuple[int, ...]] = DEFAULT_OVERVIEWS,
    ) -> WriteResult:
        """
        Writes a 2D/3D image to disk using rasterio.

        :param array:
            A 2D/3D NumPy array.

        :param out_filename:
            A string containing the output file name.

        :param geobox:
            An instance of a GriddedGeoBox object.

        :param nodata:
            A value representing the no data value for the array.

        :param overview_resampling:
            If levels is set, build overviews using a resampling method
            from `rasterio.enums.Resampling`
            Default is `Resampling.nearest`.

        :notes:
            If array is an instance of a `h5py.Dataset`, then the output
            file will include blocksizes based on the `h5py.Dataset's`
            chunks. To override the blocksizes, specify them using the
            `options` keyword. Eg {'blockxsize': 512, 'blockysize': 512}.
        """
        if out_filename.exists():
            # Sanity check. Our measurements should have different names...
            raise RuntimeError(
                f"measurement output file already exists? {out_filename}"
            )

        # TODO: Old packager never passed in tags. Perhaps we want some?
        tags = {}

        dtype = array.dtype.name

        # Check for excluded datatypes
        excluded_dtypes = ["int64", "int8", "uint64"]
        if dtype in excluded_dtypes:
            raise TypeError("Datatype not supported: {dt}".format(dt=dtype))

        # convert any bools to uin8
        if dtype == "bool":
            array = np.uint8(array)
            dtype = "uint8"

        ndims = array.ndim
        shape = array.shape

        # Get the (z, y, x) dimensions (assuming BSQ interleave)
        if ndims == 2:
            samples = shape[1]
            lines = shape[0]
            bands = 1
        elif ndims == 3:
            samples = shape[2]
            lines = shape[1]
            bands = shape[0]
        else:
            raise IndexError(
                "Input array is not of 2 or 3 dimensions. Got {dims}".format(dims=ndims)
            )

        transform = None
        projection = None
        if geobox is not None:
            transform = geobox.transform
            projection = geobox.crs

        rio_args = {
            "count": bands,
            "width": samples,
            "height": lines,
            "crs": projection,
            "transform": transform,
            "dtype": dtype,
            "driver": "GTiff",
            "predictor": self.PREDICTOR_DEFAULTS[dtype],
        }
        if nodata is not None:
            rio_args["nodata"] = nodata

        if isinstance(array, h5py.Dataset):
            # TODO: if array is 3D get x & y chunks
            if array.chunks[1] == array.shape[1]:
                # GDAL doesn't like tiled or blocksize options to be set
                # the same length as the columns (probably true for rows as well)
                array = array[:]
            else:
                y_tile, x_tile = array.chunks
                tiles = generate_tiles(samples, lines, x_tile, y_tile)

                if "tiled" in self.options:
                    rio_args["blockxsize"] = self.options.get("blockxsize", x_tile)
                    rio_args["blockysize"] = self.options.get("blockysize", y_tile)

        # the user can override any derived blocksizes by supplying `options`
        # handle case where no options are provided
        for key in self.options:
            rio_args[key] = self.options[key]

        # Write to temp directory first so we can add levels afterwards with gdal.
        with tempfile.TemporaryDirectory(
            dir=out_filename.parent, prefix=".band_write"
        ) as tmpdir:
            unstructured_image = Path(tmpdir) / out_filename.name
            """
            This is a wrapper around rasterio writing tiles to
            enable writing to a temporary location before rearranging
            the overviews within the file by gdal when required
            """
            with rasterio.open(unstructured_image, "w", **rio_args) as outds:
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
                if overviews:
                    outds.build_overviews(overviews, overview_resampling)

            if overviews:
                # Move the overviews to the start of the file, as required to be COG-compliant.
                run_command(
                    [
                        "gdal_translate",
                        "-q",
                        "-co",
                        "{}={}".format("PREDICTOR", self.PREDICTOR_DEFAULTS[dtype]),
                        *self._gdal_cli_config(),
                        unstructured_image,
                        out_filename,
                    ],
                    out_filename.parent,
                )
            else:
                unstructured_image.rename(out_filename)

        return WriteResult(file_format=FileFormat.GeoTIFF)

    def _gdal_cli_config(self, option_whitelist=None, config_whitelist=None):
        args = []

        for key, value in self.options.items():
            if option_whitelist is None or key in option_whitelist:
                args.extend(["-co", "{}={}".format(key, value)])
        for key, value in self.config_options.items():
            if config_whitelist is None or key in config_whitelist:
                args.extend(["--config", "{}".format(key), "{}".format(value)])
        return args

    def create_thumbnail(self, rgb: Tuple[Path, Path, Path], out: Path):
        with tempfile.TemporaryDirectory(dir=out.parent, prefix=".thumbgen-") as tmpdir:
            tmpdir = Path(tmpdir)
            # initial vrt of required rgb bands
            vrt_file = tmpdir / "input_bands.vrt"
            run_command(
                ["gdalbuildvrt", "-q", "-separate", "-overwrite", vrt_file, *rgb],
                tmpdir,
            )

            # quicklook with contrast scaling
            stretched_file = tmpdir / "stretched.tif"
            quicklook(
                vrt_file, out_fname=stretched_file, src_min=1, src_max=3500, out_min=1
            )

            # warp to Lon/Lat WGS84
            warped_file = tmpdir / "warped.tif"

            run_command(
                [
                    "gdalwarp",
                    "-q",
                    *self._gdal_cli_config(),
                    "-t_srs",
                    "EPSG:4326",
                    "-co",
                    "COMPRESS=JPEG",
                    "-co",
                    "PHOTOMETRIC=YCBCR",
                    "-co",
                    "TILED=YES",
                    stretched_file,
                    warped_file,
                ],
                tmpdir,
            )

            warped_file = tmpdir / "warped.tif"

            # create the thumbnail
            run_command(
                [
                    "gdal_translate",
                    "-q",
                    *self._gdal_cli_config(),
                    "-of",
                    "JPEG",
                    "-outsize",
                    "10%",
                    "10%",
                    warped_file,
                    out,
                ],
                tmpdir,
            )


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
