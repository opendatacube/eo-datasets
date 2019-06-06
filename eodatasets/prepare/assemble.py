import shutil
import tempfile
import uuid
import warnings
from collections import defaultdict
from copy import deepcopy
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Optional, Dict, Tuple, List, Any

import h5py
import numpy
import rasterio
from rasterio.enums import Resampling

from eodatasets.prepare import images, serialise
from eodatasets.prepare.images import GridSpec, FileWrite
from eodatasets.prepare.model import DatasetDoc, GridDoc, MeasurementDoc, ProductDoc
from eodatasets.verify import PackageChecksum

_INHERITABLE_PROPERTIES = {
    "datetime",
    "eo:platform",
    "eo:instrument",
    "eo:gsd",
    "eo:cloud_cover",
    "eo:sun_azimuth",
    "eo:sun_elevation",
    "landsat:collection_number",
    "landsat:landsat_scene_id",
    "landsat:landsat_product_id",
    "landsat:wrs_path",
    "landsat:wrs_row",
    "landsat:collection_category",
}


def nest_properties(d: Dict[str, Any], separator=":") -> Dict[str, Any]:
    """
    Split keys with embedded colons into sub dictionaries.

    Intended for stac-like properties

    >>> nest_properties({'landsat:path':1, 'landsat:row':2, 'clouds':3})
    {'landsat': {'path': 1, 'row': 2}, 'clouds': 3}
    """
    out = defaultdict(dict)
    for key, val in d.items():
        section, *remainder = key.split(separator, 1)
        if remainder:
            [sub_key] = remainder
            out[section][sub_key] = val
        else:
            out[section] = val

    for key, val in out.items():
        if isinstance(val, dict):
            out[key] = nest_properties(val, separator=separator)

    return dict(out)


class IfExists(Enum):
    Skip = 0
    Overwrite = 1
    ThrowError = 2


class AssemblyError(Exception):
    pass


class DatasetAssembler:
    """
    Assemble an ODC dataset.

    Either write a metadata document referencing existing files (pass in just a metadata_path)
    or specify an output folder.
    """

    def __init__(
        self,
        output_folder: Optional[Path] = None,
        metadata_path: Optional[Path] = None,
        # By default, we complain if the output already exists.
        if_exists=IfExists.ThrowError,
        allow_absolute_paths=False,
        naming_conventions="dea",
    ) -> None:
        if not output_folder and not metadata_path:
            raise ValueError(
                "Either an output folder or a metadata path must be specified"
            )

        if output_folder.exists() and if_exists.ThrowError:
            raise AssemblyError(f"Output exists {output_folder.as_posix()!r}")

        self._exists_behaviour = if_exists
        self._destination_folder = output_folder
        self._metadata_path = metadata_path

        self._checksum = PackageChecksum()

        self._work_path = Path(
            tempfile.mkdtemp(prefix=".odcdataset-", dir=str(output_folder.parent))
        )

        # The measurements grouped by their grid.
        # (value is band_name->Path)
        self._measurements_per_grid: Dict[GridSpec, Dict[str, Path]] = defaultdict(dict)

        self._allow_absolute_paths = allow_absolute_paths

        if naming_conventions != "dea":
            raise NotImplementedError("configurable naming conventions")

        self._user_metadata = dict()

        self._lineage: Dict[str, List[uuid.UUID]] = defaultdict(list)

        self.properties = {}

        # TODO generate?
        self.product_name = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # The user has already called finish() if everything went right.
        # Clean up.
        self.close()

    @property
    def _my_label(self):
        # TODO: Generate dataset label
        return f"dataset-label-todo"

    def close(self):
        """Cleanup any temporary files, even if dataset has not been written"""
        # TODO: add implicit cleanup like tempfile.TemporaryDirectory?
        shutil.rmtree(self._work_path, ignore_errors=True)

    def add_source_path(
        self, path: Path, classifier: str = None, auto_inherit_properties: bool = False
    ):
        """Add source dataset using its metadata file path.

        Optionally copy any relevant properties (platform, instrument etc)
        """

        # TODO: if they gave a dataset directory, check the metadata inside?
        self.add_source_dataset(
            serialise.from_path(path),
            classifier=classifier,
            auto_inherit_properties=auto_inherit_properties,
        )

    def add_source_dataset(
        self,
        dataset: DatasetDoc,
        classifier: str = None,
        auto_inherit_properties: bool = False,
    ):
        """Add source dataset.

        Optionally copy any relevant properties (platform, instrument etc)
        """

        if not classifier:
            classifier = dataset.properties["odc:product_family"]
        if not classifier:
            # TODO: This rule is a little obscure to force people to know.
            #       We could somehow figure out the product family from the product?
            raise ValueError(
                "Source dataset doesn't have a 'odc:product_family' property (eg. 'level1', 'fc'), "
                "you must specify a more specific classifier parameter."
            )

        self._lineage[classifier].append(dataset.id)
        if auto_inherit_properties:
            self._inherit_properties_from(dataset)

    def _inherit_properties_from(self, dataset: DatasetDoc):
        for name in _INHERITABLE_PROPERTIES:
            new_val = dataset.properties[name]

            existing_val = self.properties.get(name)
            if existing_val is None:
                self.properties[name] = new_val
            else:
                # Already set. Do nothing.
                if new_val != existing_val:
                    warnings.warn(
                        f"Inheritable property {name!r} is different from current value: "
                        f"{existing_val!r} != {new_val!r}"
                    )

    def write_measurement_h5(self, name: str, g: h5py.Dataset):
        grid = images.GridSpec.from_h5(g)
        out_path = self._measurement_file_path(name)

        FileWrite.from_existing(g.shape).write_tif_from_h5(g, out_path, geobox=grid)
        self._record_image(name, grid, out_path)

    def _measurement_file_path(self, band_name):
        return self._work_path / self.format_name(
            r"{product_name}-0-0_{odc[reference_code]}.tif", dict(name=band_name)
        )

    def _record_image(self, name: str, grid: GridSpec, path: Path):
        # We checksum immediately as the file has *just* been written so it may still
        # be in os/filesystem cache.
        self._checksum.add_file(path)

        for measurements in self._measurements_per_grid.values():
            if name in measurements:
                raise ValueError(
                    f"Duplicate addition of band called {name!r}. "
                    f"Original at {measurements[name]} and now {path}"
                )

        self._measurements_per_grid[grid][name] = path

    def format_name(self, s: str, custom_fields) -> str:
        properties = nest_properties(self.properties)
        return s.format_map(
            {**properties, "product_name": self.product_name, **custom_fields}
        )

    def write_measurement(self, name: str, p: Path):
        with rasterio.open(p) as f:
            grid = images.GridSpec.from_rio(f)

        out_path = self._measurement_file_path(name)

        FileWrite.from_existing(grid.shape).write_from_file(p, out_path)
        self._record_image(name, grid, out_path)

    def extend_user_metadata(self, section, d: Dict):
        if section in self._user_metadata:
            raise ValueError(f"metadata section {section} already exists")

        self._user_metadata[section] = deepcopy(d)

    def write_measurement_numpy(
        self,
        name: str,
        array: numpy.ndarray,
        grid_spec: GridSpec,
        nodata=None,
        overview_resampling=Resampling.nearest,
    ):
        out_path = self._measurement_file_path(name)

        FileWrite.from_existing(array.shape).write_from_ndarray(
            array,
            out_path,
            geobox=grid_spec,
            nodata=nodata,
            overview_resampling=overview_resampling,
        )
        self._record_image(name, grid_spec, out_path)

    def note_software_version(self, repository_url, version):
        existing_version = self._user_metadata.setdefault("software_versions", {}).get(
            repository_url
        )
        if existing_version and existing_version != version:
            raise ValueError(
                f"duplicate setting of software {repository_url!r} with different value "
                f"({existing_version!r} != {version!r}"
            )
        self._user_metadata["software_versions"][repository_url] = version

    def __setitem__(self, key, value):
        if key in self.properties:
            warnings.warn(f"overridding property {key!r}")
        self.properties[key] = value

    @property
    def platform(self) -> str:
        return self.properties["eo:platform"]

    @property
    def instrument(self) -> str:
        return self.properties["eo:instrument"]

    @property
    def platform_abbreviated(self) -> str:
        """Abbreviated form of a satellite, as used in dea product names. eg. 'ls7'."""
        p = self.platform
        if not p.startswith("landsat"):
            raise NotImplementedError(
                f"TODO: implement non-landsat platform abbreviation " f"(got {p!r})"
            )

        return f"ls{p[-1]}"

    @property
    def datetime_range(self):
        return (
            self.properties.get("dtr:start_datetime"),
            self.properties.get("dtr:end_datetime"),
        )

    @datetime_range.setter
    def datetime_range(self, val: Tuple[datetime, datetime]):
        # TODO: string type conversion, better validation/errors
        start, end = val
        self.properties["dtr:start_datetime"] = start
        self.properties["dtr:end_datetime"] = end

    def finish(self):
        """Write the dataset to the destination"""
        # write metadata fields:

        # Order from most to fewest measurements.
        crs, grid_docs, measurement_docs = self._assemble_geo_docs(
            self._measurements_per_grid
        )

        dataset = DatasetDoc(
            id=uuid.uuid4(),
            # TODO: configurable/non-dea naming?
            product=ProductDoc.dea_name(self.product_name),
            properties=self.properties,
            lineage=self._lineage,
            crs=crs.to_epsg() if crs.is_epsg_code else crs.to_wkt(),
            grids=grid_docs,
            measurements=measurement_docs,
        )

        # TODO: Take geometry on addition to package
        # dataset.geometry =

        if dataset.product is None:
            # TODO: Move into customisable naming conventions.
            raise NotImplementedError("product name isn't yet auto-generated ")

        doc = serialise.to_formatted_doc(dataset)
        metadata_path = self._work_path / f"{self._my_label}.odc-dataset.yaml"
        serialise.dump_yaml(metadata_path, doc)
        self._checksum.add_file(metadata_path)

        checksum_path = self._work_path / f"{self._my_label}.sha1"
        self._checksum.write(checksum_path)
        checksum_path.chmod(0o664)

        # Match the lower r/w permission bits to the output folder.
        # (Temp directories default to 700 otherwise.)
        self._work_path.chmod(self._destination_folder.stat().st_mode & 0o777)

        # Now atomically move to final location.
        # Someone else may have created the output while we were working.
        # Try, and then decide how to handle it if so.
        try:
            self._work_path.rename(self._destination_folder)
        except OSError:
            if not self._destination_folder.exists():
                # Some other error?
                raise

            if self._exists_behaviour == IfExists.Skip:
                print(f"Skipping -- exists: {self._destination_folder}")
            elif self._exists_behaviour == IfExists.ThrowError:
                raise
            elif self._exists_behaviour == IfExists.Overwrite:
                raise NotImplementedError("overwriting outputs not yet implemented")
            else:
                raise RuntimeError(
                    f"Unexpected exists behaviour: {self._exists_behaviour}"
                )

    @staticmethod
    def _assemble_geo_docs(measurements_per_grid: Dict[GridSpec, Dict[str, Path]]):

        # PyCharm's typing seems to get confused by the sorted() call.
        # noinspection PyTypeChecker
        grids_by_frequency: List[Tuple[GridSpec, Dict[str, Path]]] = sorted(
            measurements_per_grid.items(), key=lambda k: len(k[1])
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
                grid_name = "_".join(measurements.keys())

            grid_docs[grid_name] = GridDoc(grid.shape, grid.transform)

            for measurement_name, measurement_path in measurements.items():
                measurement_docs[measurement_name] = MeasurementDoc(
                    path=measurement_path,
                    grid=grid_name if grid_name != "default" else None,
                )
        return crs, grid_docs, measurement_docs


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
        p.write_measurement("blue", Path("tif"))

        #
        p.finish()
