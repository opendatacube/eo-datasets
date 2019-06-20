import shutil
import tempfile
import uuid
import warnings
from collections import defaultdict
from copy import deepcopy
from enum import Enum
from pathlib import Path, PurePath
from typing import Dict, List, Optional, Tuple, Generator

import h5py
import numpy
import rasterio
from boltons import iterutils
from rasterio import DatasetReader
from rasterio.enums import Resampling

import eodatasets2
from eodatasets2 import serialise, validate, images
from eodatasets2.images import FileWrite, GridSpec, MeasurementRecord
from eodatasets2.model import (
    DatasetDoc,
    ProductDoc,
    StacPropertyView,
    DeaNamingConventions,
    DEA_URI_PREFIX,
)
from eodatasets2.validate import Level, ValidationMessage
from eodatasets2.verify import PackageChecksum

_INHERITABLE_PROPERTIES = {
    "datetime",
    "eo:cloud_cover",
    "eo:gsd",
    "eo:instrument",
    "eo:platform",
    "eo:sun_azimuth",
    "eo:sun_elevation",
    "landsat:collection_category",
    "landsat:collection_number",
    "landsat:landsat_product_id",
    "landsat:landsat_scene_id",
    "landsat:wrs_path",
    "landsat:wrs_row",
    "odc:reference_code",
}


class IfExists(Enum):
    Skip = 0
    Overwrite = 1
    ThrowError = 2


class AssemblyError(Exception):
    pass


def docpath_set(doc, path, value):
    """
    Set a value in a document using a path (sequence of keys).

    (It's designed to match `boltons.iterutils.get_path()` and related methods)

    >>> d = {'a': 1}
    >>> docpath_set(d, ['a'], 2)
    >>> d
    {'a': 2}
    >>> d = {'a':{'b':{'c': 1}}}
    >>> docpath_set(d, ['a', 'b', 'c'], 2)
    >>> d
    {'a': {'b': {'c': 2}}}
    >>> d = {}
    >>> docpath_set(d, ['a'], 2)
    >>> d
    {'a': 2}
    >>> d = {}
    >>> docpath_set(d, ['a', 'b'], 2)
    Traceback (most recent call last):
    ...
    KeyError: 'a'
    >>> d
    {}
    >>> docpath_set(d, [], 2)
    Traceback (most recent call last):
    ...
    ValueError: Cannot set a value to an empty path
    """
    if not path:
        raise ValueError("Cannot set a value to an empty path")

    d = doc
    for part in path[:-1]:
        d = d[part]

    d[path[-1]] = value


def make_paths_relative(
    doc: Dict, base_directory: PurePath, allow_paths_outside_base=False
):
    """
    Find all pathlib.Path values in a document structure and make them relative to the given path.

    >>> base = PurePath('/tmp/basket')
    >>> doc = {'id': 1, 'fruits': [{'apple': PurePath('/tmp/basket/fruits/apple.txt')}]}
    >>> make_paths_relative(doc, base)
    >>> doc
    {'id': 1, 'fruits': [{'apple': 'fruits/apple.txt'}]}
    >>> # No change if repeated. (relative paths still relative)
    >>> previous = deepcopy(doc)
    >>> make_paths_relative(doc, base)
    >>> doc == previous
    True
    """
    for doc_path, value in iterutils.research(
        doc, lambda p, k, v: isinstance(v, PurePath)
    ):
        value: Path

        if not value.is_absolute():
            continue

        if base_directory not in value.parents:
            if not allow_paths_outside_base:
                raise ValueError(
                    f"Path {value.as_posix()!r} is outside path {base_directory.as_posix()!r} "
                    f"(allow_paths_outside_base={allow_paths_outside_base})"
                )
            continue

        docpath_set(doc, doc_path, str(value.relative_to(base_directory)))


class IncompleteDatasetError(Exception):
    def __init__(self, validation: ValidationMessage) -> None:
        self.validation = validation


class DatasetCompletenessWarning(UserWarning):
    """A non-critical warning for invalid or incomplete metadata"""

    def __init__(self, validation: ValidationMessage) -> None:
        self.validation = validation

    def __str__(self) -> str:
        return str(self.validation)


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
        naming_conventions="default",
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

        self._measurements = MeasurementRecord()

        self._allow_absolute_paths = allow_absolute_paths

        self._user_metadata = dict()

        self._lineage: Dict[str, List[uuid.UUID]] = defaultdict(list)

        self.properties = StacPropertyView()

        if naming_conventions == "default":
            self.names = DeaNamingConventions(self.properties)
        elif naming_conventions == "dea":
            self.names = DeaNamingConventions(self.properties, DEA_URI_PREFIX)
        else:
            raise NotImplementedError("configurable naming conventions")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # The user has already called finish() if everything went right.
        # Clean up.
        self.close()

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

    def _inherit_properties_from(self, source_dataset: DatasetDoc):
        for name in _INHERITABLE_PROPERTIES:
            if name not in source_dataset.properties:
                continue
            new_val = source_dataset.properties[name]

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

    def write_measurement_h5(self, name: str, g: h5py.Dataset, expand_valid_data=True):
        grid = images.GridSpec.from_h5(g)
        out_path = self.names.measurement_file_path(self._work_path, name, "tif")

        if hasattr(g, "chunks"):
            data = g[:]
        else:
            data = g

        nodata = g.attrs.get("no_data_value")

        FileWrite.from_existing(g.shape).write_from_ndarray(
            data,
            out_path,
            geobox=grid,
            nodata=nodata,
            overview_resampling=Resampling.average,
        )
        self._measurements.record_image(
            name, grid, out_path, data, nodata, expand_valid_data=expand_valid_data
        )

        # We checksum immediately as the file has *just* been written so it may still
        # be in os/filesystem cache.
        self._checksum.add_file(out_path)

    def write_measurement(self, name: str, p: Path):
        with rasterio.open(p) as ds:
            self.write_measurement_rio(name, ds)

    def write_measurement_rio(self, name: str, ds: DatasetReader):
        grid = images.GridSpec.from_rio(ds)
        out_path = self.names.measurement_file_path(self._work_path, name, "tif")

        if len(ds.indexes) != 1:
            raise NotImplementedError(
                f"TODO: Multi-band images not currently implemented (have {len(ds.indexes)})"
            )

        array = ds.read(1)
        FileWrite.from_existing(grid.shape).write_from_ndarray(
            array, out_path, grid, ds.nodata
        )

        args = dict(nodata=ds.nodata) if ds.nodata is not None else {}
        self._measurements.record_image(name, grid, out_path, img=array, **args)

        # We checksum immediately as the file has *just* been written so it may still
        # be in os/filesystem cache.
        self._checksum.add_file(out_path)

    def write_measurement_numpy(
        self,
        name: str,
        array: numpy.ndarray,
        grid_spec: GridSpec,
        nodata=None,
        overview_resampling=Resampling.nearest,
    ):
        out_path = self.names.measurement_file_path(self._work_path, name, "tif")

        FileWrite.from_existing(array.shape).write_from_ndarray(
            array,
            out_path,
            geobox=grid_spec,
            nodata=nodata,
            overview_resampling=overview_resampling,
        )
        self._measurements.record_image(
            name, grid_spec, out_path, img=array, nodata=nodata
        )
        # We checksum immediately as the file has *just* been written so it may still
        # be in os/filesystem cache.
        self._checksum.add_file(out_path)

    def extend_user_metadata(self, section, d: Dict):
        if section in self._user_metadata:
            raise ValueError(f"metadata section {section} already exists")

        self._user_metadata[section] = deepcopy(d)

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

    def done(self, validate_correctness=True, sort_bands=True):
        """Write the dataset to the destination"""
        self.note_software_version(
            "https://github.com/GeoscienceAustralia/eo-datasets",
            eodatasets2.__version__,
        )

        # Order from most to fewest measurements.
        crs, grid_docs, measurement_docs = self._measurements.as_geo_docs()

        if sort_bands:
            measurement_docs = dict(sorted(measurement_docs.items()))

        valid_data = self._measurements.valid_data()

        dataset = DatasetDoc(
            id=uuid.uuid4(),
            # TODO: configurable/non-dea naming?
            product=ProductDoc(
                name=self.names.product_name, href=self.names.product_uri
            ),
            crs=f"epsg:{crs.to_epsg()}" if crs.is_epsg_code else crs.to_wkt(),
            geometry=valid_data,
            grids=grid_docs,
            properties=self.properties,
            measurements=measurement_docs,
            lineage=self._lineage,
        )

        doc = serialise.to_formatted_doc(dataset)
        self._write_yaml(
            doc, self.names.metadata_path(self._work_path, suffix="odc-metadata.yaml")
        )

        if validate_correctness:
            for m in validate.validate(doc):
                if m.level in (Level.info, Level.warning):
                    warnings.warn(DatasetCompletenessWarning(m))
                elif m.level == Level.error:
                    raise IncompleteDatasetError(m)
                else:
                    raise RuntimeError(
                        f"Internal error: Unhandled type of message level: {m.level}"
                    )
        self._write_yaml(
            self._user_metadata,
            self.names.metadata_path(self._work_path, kind="proc-info", suffix="yaml"),
            allow_external_paths=True,
        )

        self._checksum.write(self.names.checksum_path(self._work_path))

        # Match the lower r/w permission bits to the output folder.
        # (Temp directories default to 700 otherwise.)
        self._work_path.chmod(self._destination_folder.parent.stat().st_mode & 0o777)

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

        return dataset.id

    def write_thumbnail(
        self,
        red_measurement_name: str,
        green_measurement_name: str,
        blue_measurement_name: str,
        kind: str = None,
    ):
        thumb = self.names.thumbnail_name(self._work_path, kind=kind)
        measurements = dict(self._measurements.iter_paths())

        missing_measurements = {
            red_measurement_name,
            green_measurement_name,
            blue_measurement_name,
        } - set(measurements)
        if missing_measurements:
            raise IncompleteDatasetError(
                ValidationMessage(
                    Level.error,
                    "missing_thumb_measurements",
                    f"Thumbnail measurements are missing: no measurements called {missing_measurements!r}. ",
                    hint=f"Available measurements: {', '.join(measurements)}",
                )
            )

        FileWrite().create_thumbnail(
            (
                measurements[red_measurement_name].absolute(),
                measurements[green_measurement_name].absolute(),
                measurements[blue_measurement_name].absolute(),
            ),
            thumb,
        )
        self._checksum.add_file(thumb)

    def _write_yaml(self, doc, path, allow_external_paths=False):
        make_paths_relative(
            doc, self._work_path, allow_paths_outside_base=allow_external_paths
        )
        serialise.dump_yaml(path, doc)
        self._checksum.add_file(path)

    def iter_measurement_paths(self) -> Generator[Tuple[str, Path], None, None]:
        return self._measurements.iter_paths()
