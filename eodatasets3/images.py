import string

import attr
import numpy
import os
import rasterio
import rasterio.features
import shapely
import shapely.affinity
import shapely.ops
import sys
import tempfile
import xarray
from affine import Affine
from collections import defaultdict
from pathlib import Path
from rasterio import DatasetReader
from rasterio.coords import BoundingBox
from rasterio.crs import CRS
from rasterio.enums import Resampling
from rasterio.io import DatasetWriter
from rasterio.shutil import copy as rio_copy
from rasterio.warp import calculate_default_transform, reproject
from shapely.geometry.base import CAP_STYLE, JOIN_STYLE, BaseGeometry
from typing import (
    Dict,
    Generator,
    Iterable,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
    Set,
)

from eodatasets3.model import DatasetDoc, GridDoc, MeasurementDoc
from eodatasets3.properties import FileFormat


DEFAULT_OVERVIEWS = (8, 16, 32)

try:
    import h5py
except ImportError:
    h5py = None


@attr.s(auto_attribs=True, slots=True, hash=True, frozen=True)
class GridSpec:
    """
    >>> g = GridSpec(shape=(7721, 7621),
    ...              transform=Affine(30.0, 0.0, 241485.0, 0.0, -30.0, -2281485.0),
    ...              crs=CRS.from_epsg(32656))
    >>> # Numbers copied from equivalent rio dataset.bounds call.
    >>> g.bounds
    BoundingBox(left=241485.0, bottom=-2513115.0, right=470115.0, top=-2281485.0)
    >>> g.resolution_yx
    (30.0, 30.0)
    """

    shape: Tuple[int, int]
    transform: Affine

    crs: CRS = attr.ib(
        metadata=dict(doc_exclude=True), default=None, hash=False, eq=False
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

    @classmethod
    def from_odc_xarray(cls, dataset: xarray.Dataset) -> "GridSpec":
        shape = set(v.shape for v in dataset.data_vars.values()).pop()
        return cls(
            shape=shape,
            transform=dataset.geobox.transform,
            crs=CRS.from_wkt(dataset.geobox.crs.crs_str),
        )

    @property
    def bounds(self):
        """
        Get bounding box.

        """
        return BoundingBox(
            *(self.transform * (0, self.shape[0]))
            + (self.transform * (self.shape[1], 0))
        )


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


def _find_a_common_name(
    group_of_names: Sequence[str], all_possible_names: Set[str] = None
) -> Optional[str]:
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
    >>> # If a name is taken by non-group memebers, it shouldn't be chosen
    >>> # (There's an 'nbar' prefix outside of the group, so shouldn't be found)
    >>> all_names = {'nbar_blue', 'nbar_red', 'nbar_green', 'nbart_blue'}
    >>> _find_a_common_name(['nbar_blue', 'nbar_red'], all_possible_names=all_names)
    >>> _find_a_common_name(['nbar_blue', 'nbar_red', 'nbar_green'], all_possible_names=all_names)
    'nbar'
    """
    options = []

    non_group_names = (all_possible_names or set()).difference(group_of_names)

    # If all measurements have a common prefix (like 'nbar_') it makes a nice grid name.
    prefix = os.path.commonprefix(group_of_names)
    if not any(name.startswith(prefix) for name in non_group_names):
        options.append(prefix)

    suffix = _common_suffix(group_of_names)
    if not any(name.endswith(suffix) for name in non_group_names):
        options.append(suffix)

    if not options:
        return None

    options = [s.strip("_:") for s in options]
    # Pick the longest candidate.
    options.sort(key=len, reverse=True)
    return options[0] or None


@attr.s(auto_attribs=True, slots=True)
class _MeasurementLocation:
    path: Path
    layer: str = None


_Measurements = Dict[str, _MeasurementLocation]


class MeasurementRecord:
    """
    Record the information for measurements/images to later write out to metadata.
    """

    def __init__(self):
        # The measurements grouped by their grid.
        # (value is band_name->Path)
        self._measurements_per_grid: Dict[GridSpec, _Measurements] = defaultdict(dict)
        # Valid data mask per grid, in pixel coordinates.
        self.mask_by_grid: Dict[GridSpec, numpy.ndarray] = {}

    def record_image(
        self,
        name: str,
        grid: GridSpec,
        path: Union[Path, str],
        img: numpy.ndarray,
        layer: Optional[str] = None,
        nodata=None,
        expand_valid_data=True,
    ):
        for measurements in self._measurements_per_grid.values():
            if name in measurements:
                raise ValueError(
                    f"Duplicate addition of band called {name!r}. "
                    f"Original at {measurements[name]} and now {path}"
                )

        self._measurements_per_grid[grid][name] = _MeasurementLocation(path, layer)
        if expand_valid_data:
            self._expand_valid_data_mask(grid, img, nodata)

    def _expand_valid_data_mask(self, grid: GridSpec, img: numpy.ndarray, nodata):
        if nodata is None:
            nodata = 0

        mask = self.mask_by_grid.get(grid)
        if mask is None:
            mask = img != nodata
        else:
            mask |= img != nodata
        self.mask_by_grid[grid] = mask

    def _as_named_grids(self) -> Dict[str, Tuple[GridSpec, _Measurements]]:
        """Get our grids with sensible (hopefully!), names."""

        # Order grids from most to fewest measurements.
        # PyCharm's typing seems to get confused by the sorted() call.
        # noinspection PyTypeChecker
        grids_by_frequency: List[Tuple[GridSpec, _Measurements]] = sorted(
            self._measurements_per_grid.items(), key=lambda k: len(k[1]), reverse=True
        )

        # The largest group is the default.
        default_grid = grids_by_frequency.pop(0)

        named_grids = {"default": default_grid}

        # No other grids? Nothing to do!
        if not grids_by_frequency:
            return named_grids

        # First try to name them via common prefixes, suffixes etc.
        all_measurement_names = set(self.iter_names())
        for grid, measurements in grids_by_frequency:
            if len(measurements) == 1:
                grid_name = "_".join(measurements.keys())
            else:
                grid_name = _find_a_common_name(
                    list(measurements.keys()), all_possible_names=all_measurement_names
                )
                if not grid_name:
                    # Nothing useful found!
                    break

            if grid_name in named_grids:
                # Clash of names! This strategy wont work.
                break

            named_grids[grid_name] = (grid, measurements)
        else:
            # We finished without a clash.
            return named_grids

        # Otherwise, try resolution names:
        named_grids = {"default": default_grid}
        for grid, measurements in grids_by_frequency:
            res_y, res_x = grid.resolution_yx
            if res_x > 1:
                res_x = int(res_x)
            grid_name = f"{res_x}"
            if grid_name in named_grids:
                # Clash of names! This strategy wont work.
                break

            named_grids[grid_name] = (grid, measurements)
        else:
            # We finished without a clash.
            return named_grids

        # No strategies worked!
        # Enumerated, alphabetical letter names. Grid 'a', Grid 'b', etc...
        grid_names = list(string.ascii_letters)
        if len(grids_by_frequency) > len(grid_names):
            raise NotImplementedError(
                f"More than {len(grid_names)} grids that cannot be named!"
            )
        return {
            "default": default_grid,
            **{
                grid_names[i]: (grid, measurements)
                for i, (grid, measurements) in enumerate(grids_by_frequency)
            },
        }

    def as_geo_docs(self) -> Tuple[CRS, Dict[str, GridDoc], Dict[str, MeasurementDoc]]:
        """Calculate combined geo information for metadata docs"""

        if not self._measurements_per_grid:
            return None, None, None

        grid_docs: Dict[str, GridDoc] = {}
        measurement_docs: Dict[str, MeasurementDoc] = {}
        crs = None
        for grid_name, (grid, measurements) in self._as_named_grids().items():
            # Validate assumption: All grids should have same CRS
            if crs is None:
                crs = grid.crs
            # TODO: CRS equality is tricky. This may not work.
            #       We're assuming a group of measurements specify their CRS
            #       the same way if they are the same.
            elif grid.crs != crs:
                raise ValueError(
                    f"Measurements have different CRSes in the same dataset:\n"
                    f"\t{crs.to_string()!r}\n"
                    f"\t{grid.crs.to_string()!r}\n"
                )

            grid_docs[grid_name] = GridDoc(grid.shape, grid.transform)

            for measurement_name, measurement_path in measurements.items():
                # No measurement groups in the doc: we replace with underscores.
                measurement_name = measurement_name.replace(":", "_")

                measurement_docs[measurement_name] = MeasurementDoc(
                    path=measurement_path.path,
                    layer=measurement_path.layer,
                    grid=grid_name if grid_name != "default" else None,
                )
        return crs, grid_docs, measurement_docs

    def consume_and_get_valid_data(self) -> BaseGeometry:
        """
        Consume the stored grids and produce the valid data for them.

        (they are consumed in order to to minimise peak memory usage)
        """

        def valid_shape(shape):
            if shape.is_valid:
                return shape
            return shape.buffer(0)

        geoms = []
        while self.mask_by_grid:
            grid, mask = self.mask_by_grid.popitem()
            mask = mask.astype("uint8")
            shape = shapely.ops.unary_union(
                [
                    valid_shape(shapely.geometry.shape(shape))
                    for shape, val in rasterio.features.shapes(mask)
                    if val == 1
                ]
            )
            shape_y, shape_x = mask.shape
            del mask

            # convex hull
            geom = shape.convex_hull

            # buffer by 1 pixel
            geom = geom.buffer(
                1, cap_style=CAP_STYLE.square, join_style=JOIN_STYLE.bevel
            )

            # simplify with 1 pixel radius
            geom = geom.simplify(1)

            # intersect with image bounding box
            geom = geom.intersection(shapely.geometry.box(0, 0, shape_x, shape_y))

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

    def iter_names(self) -> Generator[str, None, None]:
        """All known measurement names"""
        for grid, measurements in self._measurements_per_grid.items():
            for band_name, _ in measurements.items():
                yield band_name

    def iter_paths(self) -> Generator[Tuple[GridSpec, str, Path], None, None]:
        """All current measurement paths on disk"""
        for grid, measurements in self._measurements_per_grid.items():
            for band_name, meas_path in measurements.items():
                yield grid, band_name, meas_path.path


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
        self,
        gdal_options: Dict = None,
        overview_blocksize: Optional[int] = None,
    ) -> None:
        super().__init__()
        self.options = gdal_options or {}
        self.overview_blocksize = overview_blocksize

    @classmethod
    def from_existing(
        cls,
        shape: Tuple[int, int],
        overviews: bool = True,
        blocksize_yx: Optional[Tuple[int, int]] = None,
        overview_blocksize: Optional[int] = None,
        compress="deflate",
        zlevel=4,
    ) -> "FileWrite":
        """Returns write_img options according to the source imagery provided
        :param overviews:
            (boolean) sets overview flags in gdal config options
        :param blockxsize:
            (int) override the derived base blockxsize in cogtif conversion
        :param blockysize:
            (int) override the derived base blockysize in cogtif conversion

        """
        options = {"compress": compress, "zlevel": zlevel}

        y_size, x_size = blocksize_yx or (512, 512)
        # Do not set block sizes for small imagery
        if shape[0] < y_size and shape[1] < x_size:
            pass
        else:
            options["blockxsize"] = x_size
            options["blockysize"] = y_size
            options["tiled"] = "yes"

        if overviews:
            options["copy_src_overviews"] = "yes"

        return FileWrite(options, overview_blocksize=overview_blocksize)

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
            array = numpy.uint8(array)
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

        if h5py is not None and isinstance(array, h5py.Dataset):
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
                    if h5py is not None and isinstance(array, h5py.Dataset):
                        for tile in tiles:
                            idx = (
                                slice(tile[0][0], tile[0][1]),
                                slice(tile[1][0], tile[1][1]),
                            )
                            outds.write(array[idx], 1, window=tile)
                    else:
                        outds.write(array, 1)
                else:
                    if h5py is not None and isinstance(array, h5py.Dataset):
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
                with rasterio.Env(
                    GDAL_TIFF_OVR_BLOCKSIZE=self.overview_blocksize or 512
                ):
                    rio_copy(
                        unstructured_image,
                        out_filename,
                        **{"copy_src_overviews": True, **rio_args},
                    )
            else:
                unstructured_image.rename(out_filename)

        return WriteResult(file_format=FileFormat.GeoTIFF)

    def create_thumbnail(
        self,
        rgb: Tuple[Path, Path, Path],
        out: Path,
        out_scale=10,
        resampling=Resampling.average,
        static_stretch: Tuple[int, int] = None,
        percentile_stretch: Tuple[int, int] = (2, 98),
        compress_quality: int = 85,
        input_geobox: GridSpec = None,
    ):
        """
        Generate a thumbnail jpg image using the given three paths as red,green, blue.

        A linear stretch is performed on the colour. By default this is a dynamic 2% stretch
        (the 2% and 98% percentile values of the input). The static_stretch parameter will
        override this with a static range of values.

        If the input image has a valid no data value, the no data will
        be set to 0 in the output image.

        Any non-contiguous data across the colour domain, will be set to
        zero.
        """
        # No aux.xml file with our jpeg.
        with rasterio.Env(GDAL_PAM_ENABLED=False):
            with tempfile.TemporaryDirectory(
                dir=out.parent, prefix=".thumbgen-"
            ) as tmpdir:
                tmp_quicklook_path = Path(tmpdir) / "quicklook.tif"

                # We write an intensity-scaled, reprojected version of the dataset at full res.
                # Then write a scaled JPEG verison. (TODO: can we do it in one step?)
                ql_grid = _write_quicklook(
                    rgb,
                    tmp_quicklook_path,
                    resampling,
                    static_range=static_stretch,
                    percentile_range=percentile_stretch,
                    input_geobox=input_geobox,
                )
                out_crs = ql_grid.crs

                # Scale and write as JPEG to the output.
                (
                    thumb_transform,
                    thumb_width,
                    thumb_height,
                ) = calculate_default_transform(
                    out_crs,
                    out_crs,
                    ql_grid.shape[1],
                    ql_grid.shape[0],
                    *ql_grid.bounds,
                    dst_width=ql_grid.shape[1] // out_scale,
                    dst_height=ql_grid.shape[0] // out_scale,
                )
                thumb_args = dict(
                    driver="JPEG",
                    quality=compress_quality,
                    height=thumb_height,
                    width=thumb_width,
                    count=3,
                    dtype="uint8",
                    nodata=0,
                    transform=thumb_transform,
                    crs=out_crs,
                )
                with rasterio.open(tmp_quicklook_path, "r") as ql_ds:
                    ql_ds: DatasetReader
                    with rasterio.open(out, "w", **thumb_args) as thumb_ds:
                        thumb_ds: DatasetWriter
                        for index in thumb_ds.indexes:
                            thumb_ds.write(
                                ql_ds.read(
                                    index,
                                    out_shape=(thumb_height, thumb_width),
                                    resampling=resampling,
                                ),
                                index,
                            )

    def create_thumbnail_singleband(
        self,
        in_file: Path,
        out_file: Path,
        bit: int = None,
        lookup_table: Dict[int, Tuple[int, int, int]] = None,
    ):
        """
        Write out a JPG thumbnail from a singleband image.
        This takes in a path to a valid raster dataset and writes
        out a file with only the values of the bit (integer) as white
        """
        if bit is not None and lookup_table is not None:
            raise ValueError(
                "Please set either bit or lookup_table, and not both of them"
            )
        if bit is None and lookup_table is None:
            raise ValueError(
                "Please set either bit or lookup_table, you haven't set either of them"
            )

        with rasterio.open(in_file) as dataset:
            data = dataset.read()

            if bit is not None:
                out_data = numpy.copy(data)
                out_data[data != bit] = 0
                stretch = [0, bit]
            if lookup_table is not None:
                out_data = [
                    numpy.full_like(data, 0),
                    numpy.full_like(data, 0),
                    numpy.full_like(data, 0),
                ]
                stretch = [0, 255]

                for value, rgb in lookup_table.items():
                    for index in range(3):
                        out_data[index][data == value] = rgb[index]

        meta = dataset.meta
        meta["driver"] = "GTiff"

        with tempfile.TemporaryDirectory() as temp_dir:
            if bit:
                # Only use one file, three times
                temp_file = Path(temp_dir) / "temp.tif"

                with rasterio.open(temp_file, "w", **meta) as tmpdataset:
                    tmpdataset.write(out_data)
                self.create_thumbnail(
                    [temp_file, temp_file, temp_file],
                    out_file,
                    static_stretch=stretch,
                )
            else:
                # Use three different files
                temp_files = [Path(temp_dir) / f"temp_{i}.tif" for i in range(3)]

                for i in range(3):
                    with rasterio.open(temp_files[i], "w", **meta) as tmpdataset:
                        tmpdataset.write(out_data[i])
                self.create_thumbnail(temp_files, out_file, static_stretch=stretch)


def _write_quicklook(
    rgb: Sequence[Path],
    dest_path: Path,
    resampling: Resampling,
    static_range: Tuple[int, int],
    percentile_range: Tuple[int, int] = (2, 98),
    input_geobox: GridSpec = None,
) -> GridSpec:
    """
    Write an intensity-scaled wgs84 image using the given files as bands.
    """
    if input_geobox is None:
        with rasterio.open(rgb[0]) as ds:
            input_geobox = GridSpec.from_rio(ds)

    out_crs = CRS.from_epsg(4326)
    (
        reprojected_transform,
        reprojected_width,
        reprojected_height,
    ) = calculate_default_transform(
        input_geobox.crs,
        out_crs,
        input_geobox.shape[1],
        input_geobox.shape[0],
        *input_geobox.bounds,
    )
    reproj_grid = GridSpec(
        (reprojected_height, reprojected_width), reprojected_transform, crs=out_crs
    )
    ql_write_args = dict(
        driver="GTiff",
        dtype="uint8",
        count=len(rgb),
        width=reproj_grid.shape[1],
        height=reproj_grid.shape[0],
        transform=reproj_grid.transform,
        crs=reproj_grid.crs,
        nodata=0,
        tiled="yes",
    )

    # Only set blocksize on larger imagery; enables reduced resolution processing
    if reproj_grid.shape[0] > 512:
        ql_write_args["blockysize"] = 512
    if reproj_grid.shape[1] > 512:
        ql_write_args["blockxsize"] = 512

    with rasterio.open(dest_path, "w", **ql_write_args) as ql_ds:
        ql_ds: DatasetWriter

        # Calculate combined nodata mask
        valid_data_mask = numpy.ones(input_geobox.shape, dtype="bool")
        calculated_range = read_valid_mask_and_value_range(
            valid_data_mask, _iter_images(rgb), percentile_range
        )

        for band_no, (image, nodata) in enumerate(_iter_images(rgb), start=1):
            reprojected_data = numpy.zeros(reproj_grid.shape, dtype=numpy.uint8)
            reproject(
                rescale_intensity(
                    image,
                    image_null_mask=~valid_data_mask,
                    in_range=(static_range or calculated_range),
                    out_range=(1, 255),
                    out_dtype=numpy.uint8,
                ),
                reprojected_data,
                src_crs=input_geobox.crs,
                src_transform=input_geobox.transform,
                src_nodata=0,
                dst_crs=reproj_grid.crs,
                dst_nodata=0,
                dst_transform=reproj_grid.transform,
                resampling=resampling,
                num_threads=2,
            )
            ql_ds.write(reprojected_data, band_no)
            del reprojected_data

    return reproj_grid


LazyImages = Iterable[Tuple[numpy.ndarray, int]]


def _iter_images(rgb: Sequence[Path]) -> LazyImages:
    """
    Lazily load a series of single-band images from a path.

    Yields the image array and nodata value.
    """
    for path in rgb:
        with rasterio.open(path) as ds:
            ds: DatasetReader
            if ds.count != 1:
                raise NotImplementedError(
                    "multi-band measurement files aren't yet supported"
                )
            yield ds.read(1), ds.nodata


def read_valid_mask_and_value_range(
    valid_data_mask: numpy.ndarray,
    images: LazyImages,
    calculate_percentiles: Optional[Tuple[int, int]] = None,
) -> Optional[Tuple[int, int]]:
    """
    Read the given images, filling in a valid data mask and optional pixel percentiles.
    """
    calculated_range = (-sys.maxsize - 1, sys.maxsize)
    for array, nodata in images:
        valid_data_mask &= array != nodata

        if calculate_percentiles is not None:
            the_data = array[valid_data_mask]
            # Check if there's a non-empty array first
            if the_data.any():
                low, high = numpy.percentile(
                    the_data, calculate_percentiles, interpolation="nearest"
                )
                calculated_range = (
                    max(low, calculated_range[0]),
                    min(high, calculated_range[1]),
                )

    return calculated_range


def rescale_intensity(
    image: numpy.ndarray,
    in_range: Tuple[int, int],
    out_range: Optional[Tuple[int, int]] = None,
    image_nodata: int = None,
    image_null_mask: numpy.ndarray = None,
    out_dtype=numpy.uint8,
    out_nodata=0,
) -> numpy.ndarray:
    """
    Based on scikit-image's rescale_intensity, but does fewer copies/allocations of the array.

    (and it saves us bringing in the entire dependency for one small method)
    """
    if image_null_mask is None:
        if image_nodata is None:
            raise ValueError("Must specify either a null mask or a nodata val")
        image_null_mask = image == image_nodata

    imin, imax = in_range
    omin, omax = out_range or (numpy.iinfo(out_dtype).min, numpy.iinfo(out_dtype).max)

    # The intermediate calculation will need floats.
    # We'll convert to it immediately to avoid modifying the input array
    image = image.astype(numpy.float64)

    numpy.clip(image, imin, imax, out=image)
    image -= imin
    image /= float(imax - imin)
    image *= omax - omin
    image += omin
    image = image.astype(out_dtype)
    image[image_null_mask] = out_nodata
    return image
