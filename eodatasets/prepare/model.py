import itertools
from collections import defaultdict
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Tuple, Dict, Optional, Iterable, List, Any, Union
from uuid import UUID

import affine
import attr
import h5py
import numpy
import rasterio
import rasterio.features
import shapely
import shapely.affinity
import shapely.ops
from rasterio import DatasetReader
from rasterio.crs import CRS
from ruamel.yaml.comments import CommentedMap
from shapely.geometry.base import BaseGeometry

# TODO: these need discussion.
DEA_URI_PREFIX = "https://collections.dea.ga.gov.au"
ODC_DATASET_SCHEMA_URL = "https://schemas.opendatacube.org/dataset"


class FileFormat(Enum):
    GeoTIFF = 1
    NetCDF = 2


@attr.s(auto_attribs=True, slots=True)
class ProductDoc:
    name: str = None
    href: str = None

    @classmethod
    def dea_name(cls, name:str):
        return ProductDoc(
            name=name, href=f"{DEA_URI_PREFIX}/product/{name}"
        )


@attr.s(auto_attribs=True, slots=True, hash=True)
class GridDoc:
    shape: Tuple[int, int]
    transform: Tuple[float, float, float, float, float, float, float, float, float]


@attr.s(auto_attribs=True, slots=True)
class MeasurementDoc:
    path: str
    band: Optional[int] = 1
    layer: Optional[str] = None
    grid: str = "default"

    name: str = attr.ib(metadata=dict(doc_exclude=True), default=None)


@attr.s(auto_attribs=True, slots=True)
class DatasetDoc:
    id: UUID = None
    product: ProductDoc = None
    locations: List[str] = None

    crs: str = None
    geometry: BaseGeometry = None
    grids: Dict[str, GridDoc] = None

    properties: Dict[str, Any] = attr.ib(factory=CommentedMap)

    measurements: Dict[str, MeasurementDoc] = None

    lineage: Dict[str, Tuple[UUID]] = attr.ib(factory=CommentedMap)

    @property
    def producer(self):
        """
        Organisation that produced the data.

        eg. usgs.gov or ga.gov.au
        """
        return self.properties.get("odc:producer")

    @producer.setter
    def producer(self, domain):
        self.properties["odc:producer"] = domain

    @property
    def datetime(self) -> datetime:
        return self.properties.get("datetime")


def resolve_absolute_offset(
    dataset_path: Path, offset: str, target_path: Optional[Path] = None
) -> str:
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
    'tar:/tmp/great_test_dataset.tar!band/my_great_band.jpg'
    >>> resolve_absolute_offset(
    ...     Path('/tmp/MY_DATASET'),
    ...     'band/my_great_band.jpg',
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


class Intern(dict):
    def __missing__(self, key):
        self[key] = key
        return key


def valid_region(
    path: Path, measurements: Iterable[MeasurementDoc], mask_value=None
) -> Tuple[BaseGeometry, Dict[str, GridDoc]]:
    mask = None

    if not measurements:
        raise ValueError("No measurements: cannot calculate valid region")

    measurements_by_grid: Dict[GridDoc, List[MeasurementDoc]] = defaultdict(list)
    mask_by_grid: Dict[GridDoc, numpy.ndarray] = {}

    for measurement in measurements:
        measurement_path = resolve_absolute_offset(path, measurement.path)
        with rasterio.open(str(measurement_path), "r") as ds:
            ds: DatasetReader
            transform: affine.Affine = ds.transform

            if not len(ds.indexes) == 1:
                raise NotImplementedError(
                    f"Only single-band tifs currently supported. File {measurement_path!r}"
                )
            img = ds.read(1)
            grid = GridDoc(shape=ds.shape, transform=transform)
            measurements_by_grid[grid].append(measurement)

            if mask_value is not None:
                new_mask = img & mask_value == mask_value
            else:
                new_mask = img != ds.nodata

            mask = mask_by_grid.get(grid)
            if mask is None:
                mask = new_mask
            else:
                mask |= new_mask
            mask_by_grid[grid] = mask

    grids_by_frequency: List[Tuple[GridDoc, List[MeasurementDoc]]] = sorted(
        measurements_by_grid.items(), key=lambda k: len(k[1])
    )

    def name_grid(grid, measurements: List[MeasurementDoc], name=None):
        name = name or "_".join(m.name for m in measurements)
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

    shapes = itertools.chain(
        *[
            rasterio.features.shapes(mask.astype("uint8"), mask=mask)
            for mask in mask_by_grid.values()
        ]
    )
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
