import shutil
import tempfile
import uuid
import warnings
from collections import defaultdict
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Tuple

import h5py
import numpy
import rasterio
import attr
from rasterio.crs import CRS

from eodatasets.prepare.model import GridDoc, DatasetDoc, ProductDoc
from eodatasets.verify import PackageChecksum


@attr.s(auto_attribs=True, slots=True, hash=True)
class GridSpec:
    shape: Tuple[int, int]
    transform: Tuple[float, float, float, float, float, float, float, float, float]

    crs: CRS = attr.ib(
        metadata=dict(doc_exclude=True), default=None, hash=False, cmp=False
    )

    @classmethod
    def from_rio(cls, dataset: rasterio.DatasetReader) -> "GridSpec":
        return cls(shape=dataset.shape, transform=dataset.transform, crs=dataset.crs)

    @classmethod
    def from_h5(cls, dataset: h5py.Dataset) -> "GridSpec":
        return cls(
            shape=dataset.shape,
            # TODO: length?
            transform=tuple(dataset.attrs["geotransform"]),
            crs=CRS.from_wkt(dataset.attrs["crs_wkt"]),
        )


class DatasetAssembler:
    """
    Assemble an ODC dataset.

    Either write a metadata document referencing existing files (pass in just a metadata_path)
    or specify an output folder.
    """

    @classmethod
    def for_product(self, name:str,
                    output_folder: Optional[Path] = None,
                    metadata_path: Optional[Path] = None,):
        # TODO: lookup? this is dea specific
        p =DatasetAssembler(output_folder=output_folder, metadata_path=metadata_path)
        p._d.product = ProductDoc.dea_name(name)

    def __init__(
        self,
        output_folder: Optional[Path] = None,
        metadata_path: Optional[Path] = None,
        allow_absolute_paths=False,
        naming_conventions="dea",
    ) -> None:
        if not output_folder and not metadata_path:
            raise ValueError(
                "Either an output folder or a metadata path must be specified"
            )

        self._destination_folder = output_folder
        self._metadata_path = metadata_path

        self._checksum = PackageChecksum()

        self._work_path = Path(tempfile.mkdtemp(prefix='.odcdataset-', dir=str(output_folder)))

        self._measurements_per_grid = defaultdict(list)

        self._allow_absolute_paths = allow_absolute_paths

        if naming_conventions != "dea":
            raise NotImplementedError("configurable naming conventions")

        self._user_metadata = dict()

        self._d = DatasetDoc(
            id=uuid.uuid4()
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # The user has already called finish() if everything went right.
        # Clean up.
        self.close()

    def finish(self):
        """Write the dataset to the destination"""
        # write metadata fields:
        self._d.measurements
        self._d.grids
        self._d.crs
        self._d.geometry

        checksum_path = self._work_path / 'package.sha1'
        self._checksum.write(checksum_path)
        checksum_path.chmod(0o664)

        # Match the lower r/w permission bits to the output folder.
        # (Temp directories default to 700 otherwise.)
        self._work_path.chmod(self._destination_folder.stat().st_mode & 0o777)

        self._work_path.rename(self._destination_folder)
        raise NotImplementedError

    def close(self):
        """Cleanup any temporary files, even if dataset has not been written"""
        # TODO: add implicit cleanup like tempfile.TemporaryDirectory?
        shutil.rmtree(self._work_path, ignore_errors=True)

    def add_source_path(self, path: Path, auto_inherit_properties: bool = False):
        """Add source dataset. Copy any relevant properties"""
        raise NotImplementedError

    def add_source_dataset(self, dataset: DatasetDoc, auto_inherit_properties: bool = False):
        """Add source dataset. Copy any relevant properties"""
        raise NotImplementedError

    def write_measurement_h5(self, name: str, g: h5py.Group):
        raise NotImplementedError

    def write_measurement(self, name: str, p: Path):
        # TODO: note the path?
        with rasterio.open(p) as ds:
            self.write_measurement_rio(name, ds)

    def write_measurement_rio(self, name: str, d: rasterio.DatasetReader):
        raise NotImplementedError
        # Add to checksum

    def extend_user_metadata(self, section, d: Dict):
        if section in self._user_metadata:
            raise ValueError(f"metadata section {section} already exists")

        self._user_metadata[section] = deepcopy(d)

    def write_measurement_numpy(
        self, name: str, array: numpy.ndarray, grid_spec: GridSpec
    ):
        raise NotImplementedError

    def note_software_version(self, repository_url, version):
        existing_version = self._user_metadata["software_versions"].get(repository_url)
        if existing_version and existing_version != version:
            raise ValueError(
                f"duplicate setting of software {repository_url!r} with different value "
                f"({existing_version!r} != {version!r}"
            )
        self._user_metadata["software_versions"][repository_url] = version

    @property
    def platform(self):
        return self._d.properties.get("eo:platform")

    @property
    def datetime_range(self):
        return (
            self._d.properties.get("dtr:start_datetime"),
            self._d.properties.get("dtr:end_datetime"),
        )

    def __setitem__(self, key, value):
        if key in self._d.properties:
            warnings.warn(f"overridding property {key!r}")
        self._d.properties[key] = value

    @datetime_range.setter
    def datetime_range(self, val: Tuple[datetime, datetime]):
        # TODO: string type conversion, better validation/errors
        start, end = val
        self._d.properties["dtr:start_datetime"] = start
        self._d.properties["dtr:end_datetime"] = end


def example():
    with DatasetAssembler(Path("my-scenes"), naming_conventions="dea") as p:
        # When we have a main source dataset that our data comes from, we can
        # inherit the basic properties like platform, instrument
        p.add_source_dataset(Path(".yaml"), auto_inherit_properties=True)

        # Does normalisation for common eo properties
        p.platform = "landsat_8"
        p.instrument = "OLIT_TIRS"
        # Any stac properties
        p["eo:gsd"] = 234

        # Will reference existing file if inside base folder, or global use_absolute or local allow_absolute=True
        p.add_measurement("blue", Path())
        p.add_measurement("blue", "")

        # Copy data to a local cogtif using default naming conventions
        p.write_measurement("blue", h5py.Group)
        p.write_measurement("blue", Path("tif"))

        #
        p.finish()
