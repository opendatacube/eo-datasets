from collections import defaultdict
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Tuple, Dict, Optional, Union, Iterable, List
from uuid import UUID

import affine
import attr
import rasterio
import rasterio.features
import shapely
import shapely.affinity
import shapely.ops

from shapely.geometry.base import BaseGeometry


class FileFormat(Enum):
    GeoTIFF = 1
    NetCDF = 2


@attr.s(auto_attribs=True, slots=True)
class Product:
    name: str


@attr.s(auto_attribs=True, slots=True, hash=True, frozen=True)
class Grid:
    shape: Tuple[int, int]
    transform: Tuple[float, float, float, float, float, float, float, float, float]


@attr.s(auto_attribs=True, slots=True)
class Measurement:
    path: str
    band: Optional[str] = None
    layer: Optional[str] = None
    grid: Optional[str] = "default"


@attr.s(auto_attribs=True, slots=True)
class Dataset:
    id: UUID
    product: Product

    datetime: datetime

    file_format: FileFormat

    crs: str

    geometry: BaseGeometry

    grids: Dict[str, Grid]

    measurements: Dict[str, Measurement]

    lineage: Dict[str, Tuple[UUID]]

    properties: Dict[str, Union[str, int, float]]

    # io_driver_data: Dict = None
    user_data: Dict = None

    # replaces: Optional[UUID] = None

    def to_doc(self):
        d = attr.asdict(self)
        d["geometry"] = shapely.geometry.mapping(self.geometry)
        return d


def resolve_absolute_offset(dataset_path: Path, offset: str, target_path: Optional[Path] = None) -> str:
    """
    Expand a filename (offset) relative to the dataset.

    >>> external_metadata_loc = Path('/tmp/target-metadata.yaml')
    >>> resolve_absolute_offset(
    ...     Path('/tmp/great_test_dataset'),
    ...     'band/my_great_band.jpg',
    ...     external_metadata_loc,
    ... )
    '/tmp/great_test_dataset/band/my_great_band.jpg'
    >>> resolve_absolute_offset(
    ...     Path('/tmp/great_test_dataset.tar.gz'),
    ...     'band/my_great_band.jpg',
    ...     external_metadata_loc,
    ... )
    'tar:/tmp/great_test_dataset.tar.gz!band/my_great_band.jpg'
        >>> resolve_absolute_offset(
    ...     Path('/tmp/great_test_dataset.tar'),
    ...     'band/my_great_band.jpg',
    ... )
    'tar:/tmp/great_test_dataset.tar.gz!band/my_great_band.jpg'
    >>> resolve_absolute_offset(
    ...     Path('/tmp/MY_DATASET'),
    ...     'band/my_great_band.jpg'
    ...     Path('/tmp/MY_DATASET/ga-metadata.yaml'),
    ... )
    'band/my_great_band.jpg'
    """
    dataset_path = dataset_path.absolute()

    if target_path:
        # If metadata is stored inside the dataset, keep paths relative.
        if str(target_path.absolute()).startswith(str(dataset_path)):
            return offset
    # Bands are inside a tar file

    if ".tar" in dataset_path.suffixes:
        return "tar:{}!{}".format(dataset_path, offset)
    else:
        return str(dataset_path / offset)


def valid_region(
    path: Path, measurements: Iterable[Measurement], mask_value=None
) -> Tuple[BaseGeometry, Dict[str, Grid]]:
    mask = None

    if not measurements:
        raise ValueError("No measurements: cannot calculate valid region")

    measurements_by_grid: Dict[Grid, List[Measurement]] = defaultdict(list)

    for measurement in measurements:
        print(f"path: {path}")
        measurement_path = resolve_absolute_offset(path, measurement.path)
        print(measurement_path)
        with rasterio.open(str(measurement_path), "r") as ds:
            transform: affine.Affine = ds.transform
            img = ds.read(1)
            measurements_by_grid[Grid(ds.shape, transform)].append(measurement)

            if mask_value is not None:
                new_mask = img & mask_value == mask_value
            else:
                new_mask = img != ds.nodata
            if mask is None:
                mask = new_mask
            else:
                mask |= new_mask

    grids_by_frequency: List[Tuple[Grid, List[Measurement]]] = sorted(
        measurements_by_grid.items(), key=lambda k: len(k[1])
    )

    def name_grid(grid, measurements: List[Measurement], name=None):
        name = name or "_".join(m.band for m in measurements)
        for m in measurements:
            m.grid = name

        return name, grid

    grids = dict(
        [
            # most frequent is called "default", others use band names.
            name_grid(*(grids_by_frequency[-1]), name="default"),
            *(name_grid(*g) for g in grids_by_frequency[:-1]),
        ]
    )

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
    # output = shapely.geometry.mapping(geom)
    return geom, grids
