"""
API for easily writing an ODC Dataset
"""
import os
import shutil
import tempfile
import uuid
import warnings
from collections import defaultdict
from copy import deepcopy
from enum import Enum
from pathlib import Path, PurePath
from typing import Dict, List, Optional, Tuple, Generator, Any, Union

import numpy
import rasterio
from boltons import iterutils
from rasterio import DatasetReader
from rasterio.crs import CRS
from rasterio.enums import Resampling
from xarray import Dataset

import eodatasets3
from eodatasets3 import serialise, validate, images
from eodatasets3.documents import find_and_read_documents
from eodatasets3.images import FileWrite, GridSpec, MeasurementRecord
from eodatasets3.model import (
    DatasetDoc,
    ProductDoc,
    StacPropertyView,
    ComplicatedNamingConventions,
    AccessoryDoc,
    resolve_absolute_offset,
)
from eodatasets3.properties import EoFields
from eodatasets3.validate import Level, ValidationMessage
from eodatasets3.verify import PackageChecksum


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
    >>> # Relative pathlibs also become relative strings for consistency.
    >>> doc = {'villains': PurePath('the-baron.txt')}
    >>> make_paths_relative(doc, base)
    >>> doc
    {'villains': 'the-baron.txt'}
    """
    for doc_path, value in iterutils.research(
        doc, lambda p, k, v: isinstance(v, PurePath)
    ):
        value: Path

        if value.is_absolute():
            if base_directory not in value.parents:
                if not allow_paths_outside_base:
                    raise ValueError(
                        f"Path {value.as_posix()!r} is outside path {base_directory.as_posix()!r} "
                        f"(allow_paths_outside_base={allow_paths_outside_base})"
                    )
                continue
            value = value.relative_to(base_directory)

        docpath_set(doc, doc_path, str(value))


class IncompleteDatasetError(Exception):
    def __init__(self, validation: ValidationMessage) -> None:
        self.validation = validation


class DatasetCompletenessWarning(UserWarning):
    """A non-critical warning for invalid or incomplete metadata"""

    def __init__(self, validation: ValidationMessage) -> None:
        self.validation = validation

    def __str__(self) -> str:
        return str(self.validation)


class DatasetAssembler(EoFields):
    """
    Assemble an ODC dataset, writing metadata and (optionally) its imagery as COGs.
    """

    # Properties that can be inherited from a source dataset. (when auto_inherit_properties=True)
    INHERITABLE_PROPERTIES = {
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
        "odc:region_code",
    }

    def __init__(
        self,
        output_folder: Optional[Path] = None,
        metadata_file: Optional[Path] = None,
        paths_relative_to: Optional[Path] = None,
        # Optionally give a dataset id.
        dataset_id: Optional[uuid.UUID] = None,
        # By default, we complain if the output already exists.
        if_exists=IfExists.ThrowError,
        allow_absolute_paths=False,
        naming_conventions="default",
    ) -> None:
        self.dataset_id = dataset_id or uuid.uuid4()
        self._exists_behaviour = if_exists

        if not output_folder and not metadata_file:
            raise ValueError(
                "Must specify either an output folder or a single metadata file"
            )

        if output_folder and not output_folder.exists():
            raise ValueError(
                f"Provided base output folder doesn't exist: {output_folder}"
            )

        self._base_output_folder = output_folder

        # If not specified, it will be auto-generated inside the output folder.
        self._specified_metadata_path = metadata_file
        # Given relative paths are relative to this.
        self._base_location = (
            paths_relative_to or metadata_file or Path(os.getcwd()).absolute()
        )

        self._checksum = PackageChecksum()
        self._initialised_work_path: Optional[Path] = None
        self._measurements = MeasurementRecord()

        self._allow_absolute_paths = allow_absolute_paths

        self._user_metadata = dict()
        self._software_versions: List[Dict] = []

        self._lineage: Dict[str, List[uuid.UUID]] = defaultdict(list)
        self._accessories: Dict[str, Path] = {}

        self._props = StacPropertyView()
        self._label = None

        if naming_conventions == "default":
            self.names = ComplicatedNamingConventions(self)
        elif naming_conventions == "dea":
            self.names = ComplicatedNamingConventions.for_standard_dea(self)
        else:
            raise NotImplementedError("configurable naming conventions")

        self._is_finished = False
        self._finished_init_ = True

    @property
    def _work_path(self):
        if not self._initialised_work_path:
            if not self._base_output_folder:
                raise ValueError(
                    "Dataset assembler was given no base path on construction: cannot write new files."
                )

            self._initialised_work_path = Path(
                tempfile.mkdtemp(
                    prefix=".odcdataset-", dir=str(self._base_output_folder)
                )
            )

        return self._initialised_work_path

    @property
    def properties(self) -> StacPropertyView:
        return self._props

    @property
    def label(self) -> Optional[str]:
        """
        An optional displayable string to identify this dataset.

        These are often used when when presenting a list of datasets, such as in search results or a filesystem folder.
        They are unstructured, but should be more humane than showing a list of UUIDs.

        By convention they have no spaces, due to their usage in filenames.

        Eg. 'ga_ls5t_ard_3-0-0_092084_2009-12-17_final' or USGS's 'LT05_L1TP_092084_20091217_20161017_01_T1'

        A label will be auto-generated using the naming-conventions, but you can manually override it by
        setting this property.
        """
        return self._label or self.names.dataset_label

    @label.setter
    def label(self, val: str):
        self._label = val

    @property
    def destination_folder(self) -> Path:
        """
        The folder where the finished package will reside.

        This may not be accessible until enough metadata has been set.
        """
        return self.names.destination_folder(self._base_output_folder)

    def __enter__(self):
        return self

    def __setattr__(self, name: str, value: Any) -> None:
        """
        Prevent against users accidentally setting new properties on the assembler (it has happened multiple times).
        """
        if (
            name != "label"
            and hasattr(self, "_finished_init_")
            and not hasattr(self, name)
        ):
            raise TypeError(
                f"Cannot set new field '{name}' on an assembler. "
                f"(Perhaps you meant to set it on the .properties?)"
            )
        super().__setattr__(name, value)

    def __exit__(self, exc_type, exc_val, exc_tb):
        # The user has already called finish() if everything went right.
        # Clean up.
        self.close()

    def cancel(self):
        """Cancel the package, cleaning up temporary files.

        This works like `close()`, but is intentional, so no warning will
        be raised for forgetting to complete the package first.
        """
        self._is_finished = True
        self.close()

    def close(self):
        """Cleanup any temporary files, even if dataset has not been written"""
        if not self._is_finished:
            warnings.warn(
                "Closing assembler without finishing. "
                "Either call `done()` or `cancel() before closing`"
            )

        if self._initialised_work_path:
            # TODO: add implicit cleanup like tempfile.TemporaryDirectory?
            shutil.rmtree(self._work_path, ignore_errors=True)

    def add_source_path(
        self,
        *paths: Path,
        classifier: str = None,
        auto_inherit_properties: bool = False,
    ):
        """
        Record a source dataset using the path to its metadata document.

        Parameters are the same as self.add_source_dataset()

        """
        for _, doc in find_and_read_documents(*paths):
            # Newer documents declare a schema.
            if "$schema" in doc:
                self.add_source_dataset(
                    serialise.from_doc(doc),
                    classifier=classifier,
                    auto_inherit_properties=auto_inherit_properties,
                )
            else:
                if auto_inherit_properties:
                    raise NotImplementedError(
                        "Can't (yet) inherit properties from old-style metadata"
                    )
                classifier = classifier or doc.get("product_type")
                if not classifier:
                    # TODO: This rule is a little obscure to force people to know.
                    #       We could somehow figure out from the product?
                    raise ValueError(
                        "Source dataset (of old-style eo) doesn't have a 'product_type' property (eg. 'level1', 'fc'), "
                        "you must specify a classifier for the kind of source dataset."
                    )
                self._lineage[classifier].append(doc["id"])

    def add_source_dataset(
        self,
        dataset: DatasetDoc,
        classifier: Optional[str] = None,
        auto_inherit_properties: bool = False,
    ):
        """
        Record a source dataset using its metadata document.

        It can optionally copy common properties from the source dataset (platform, instrument etc)/

        (see self.INHERITABLE_PROPERTIES for the list of fields that are inheritable)

        :param auto_inherit_properties: Whether to copy any common properties from the dataset

        :param classifier: How to classify the kind of source dataset. This is will automatically
                           be filled with the family of dataset if available (eg. "level1").

                           You want to set this if you have two datasets of the same type that
                           are used for different purposes. Such as having a second level1 dataset
                           that was used for QA (but is not this same scene).


        See add_source_path() if you have a filepath reference instead of a document.

        """

        if not classifier:
            classifier = dataset.properties.get("odc:product_family")
            if not classifier:
                # TODO: This rule is a little obscure to force people to know.
                #       We could somehow figure out the product family from the product?
                raise ValueError(
                    "Source dataset doesn't have a 'odc:product_family' property (eg. 'level1', 'fc'), "
                    "you must specify a classifier for the kind of source dataset."
                )

        self._lineage[classifier].append(dataset.id)
        if auto_inherit_properties:
            self._inherit_properties_from(dataset)

    def _inherit_properties_from(self, source_dataset: DatasetDoc):
        for name in self.INHERITABLE_PROPERTIES:
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

    def write_measurement(
        self,
        name: str,
        path: Path,
        overviews=images.DEFAULT_OVERVIEWS,
        overview_resampling=Resampling.average,
        expand_valid_data=True,
        file_id: str = None,
    ):
        """
        Write a measurement by copying it from a file path.

        Assumes the file is gdal-readable.
        """
        with rasterio.open(self._path(path)) as ds:
            self.write_measurement_rio(
                name,
                ds,
                overviews=overviews,
                expand_valid_data=expand_valid_data,
                overview_resampling=overview_resampling,
                file_id=file_id,
            )

    def _path(self, path):
        return resolve_absolute_offset(self._base_location, path)

    def write_measurement_rio(
        self,
        name: str,
        ds: DatasetReader,
        overviews=images.DEFAULT_OVERVIEWS,
        overview_resampling=Resampling.average,
        expand_valid_data=True,
        file_id=None,
    ):
        """
        Write a measurement by reading it an open rasterio dataset
        """
        if len(ds.indexes) != 1:
            raise NotImplementedError(
                f"TODO: Multi-band images not currently implemented (have {len(ds.indexes)})"
            )

        self._write_measurement(
            name,
            ds.read(1),
            images.GridSpec.from_rio(ds),
            self.names.measurement_file_path(
                self._work_path, name, "tif", file_id=file_id
            ),
            expand_valid_data=expand_valid_data,
            nodata=ds.nodata,
            overview_resampling=overview_resampling,
            overviews=overviews,
        )

    def write_measurement_numpy(
        self,
        name: str,
        array: numpy.ndarray,
        grid_spec: GridSpec,
        nodata=None,
        overviews=images.DEFAULT_OVERVIEWS,
        overview_resampling=Resampling.average,
        expand_valid_data=True,
        file_id: str = None,
    ):
        """
        Write a measurement from a numpy array and grid spec.

        The most common case is to copy the grid spec from your input dataset,
        assuming you haven't reprojected.

        eg.
            p.write_measurement_numpy(
                "blue",
                new_array,
                GridSpec.from_dataset_doc(source_dataset),
                nodata=-999,
            )

        """
        self._write_measurement(
            name,
            array,
            grid_spec,
            self.names.measurement_file_path(
                self._work_path, name, "tif", file_id=file_id
            ),
            expand_valid_data=expand_valid_data,
            nodata=nodata,
            overview_resampling=overview_resampling,
            overviews=overviews,
        )

    def write_measurements_odc_xarray(
        self,
        dataset: Dataset,
        nodata: int,
        overviews=images.DEFAULT_OVERVIEWS,
        overview_resampling=Resampling.average,
        expand_valid_data=True,
        file_id=None,
    ):
        """
        Write measurements from an ODC xarray.Dataset

        The main requirement is that the Dataset contains a CRS attribute
        and X/Y or lat/long dimensions and coordinates. These are used to
        create an ODC GeoBox.
        """
        grid_spec = images.GridSpec.from_odc_xarray(dataset)
        for name, dataarray in dataset.data_vars.items():
            self._write_measurement(
                name,
                dataarray.data,
                grid_spec,
                self.names.measurement_file_path(
                    self._work_path, name, "tif", file_id=file_id
                ),
                expand_valid_data=expand_valid_data,
                overview_resampling=overview_resampling,
                overviews=overviews,
                nodata=nodata,
            )

    def _write_measurement(
        self,
        name: str,
        data: numpy.ndarray,
        grid: GridSpec,
        out_path: Path,
        expand_valid_data: bool,
        nodata: int,
        overview_resampling: Resampling,
        overviews: Tuple[int, ...],
    ):
        res = FileWrite.from_existing(grid.shape).write_from_ndarray(
            data,
            out_path,
            geobox=grid,
            nodata=nodata,
            overview_resampling=overview_resampling,
            overviews=overviews,
        )

        # Ensure the file_format field is set to what we're writing.
        file_format = res.file_format.name
        if "odc:file_format" not in self.properties:
            self.properties["odc:file_format"] = file_format

        if file_format != self.properties["odc:file_format"]:
            raise RuntimeError(
                f"Inconsistent file formats between bands. "
                f"Was {self.properties['odc:file_format']!r}, now {file_format !r}"
            )

        self._measurements.record_image(
            name,
            grid,
            out_path,
            data,
            nodata=nodata,
            expand_valid_data=expand_valid_data,
        )
        # We checksum immediately as the file has *just* been written so it may still
        # be in os/filesystem cache.
        self._checksum.add_file(out_path)

    def note_measurement(self, name, path: Union[str, Path], expand_valid_data=True):
        """
        Reference a measurement from its existing file path.

        (no data is copied, but Geo information is read from it.)
        """
        with rasterio.open(self._path(path)) as ds:
            ds: DatasetReader
            if ds.count != 1:
                raise NotImplementedError(
                    "TODO: Only single-band files currently supported"
                )

            self._measurements.record_image(
                name,
                images.GridSpec.from_rio(ds),
                path,
                ds.read(1),
                nodata=ds.nodata,
                expand_valid_data=expand_valid_data,
            )

    def extend_user_metadata(self, section: str, d: Dict):
        """
        Record extra metadata from the processing of the dataset.

        It can be any document structure suitable for yaml/json serialisation that you want,
        and will be written into the sidecar "proc-info" metadata.

        The section name should be unique, and identify the kind of document, eg 'brdf_ancillary'.
        """
        if section in self._user_metadata:
            raise ValueError(f"metadata section {section} already exists")

        self._user_metadata[section] = deepcopy(d)

    def note_software_version(self, name: str, url: str, version: str):
        """
        Record the version of some software used to produce the dataset.

        :param name: a short human-readable name for the software. eg "datacube-core"
        :param url: A URL where the software is found, such as the git repository.
        :param version: the version string, eg. "1.0.0b1"
        """
        for v in self._software_versions:
            # Uniquely identify software by the tuple (name, url)
            if v["name"] == name and v["url"] == url:
                existing_version = v["version"]
                if existing_version != version:
                    raise ValueError(
                        f"duplicate setting of software {url!r} with different value "
                        f"({existing_version!r} != {version!r})"
                    )
                return

        self._software_versions.append(dict(name=name, url=url, version=version))

    def done(
        self, validate_correctness=True, sort_bands=True
    ) -> Tuple[uuid.UUID, Path]:
        """
        Write the dataset and move it into place.

        It will be validated, metadata will be written, and if all is correct, it will be
        moved to the output location.

        The final move is done atomically, so the dataset will only exist in the output
        location if it is complete.

        IncompleteDatasetError is raised if any critical metadata is incomplete.

        Returns the final path to the dataset metadata file.
        """
        self.note_software_version(
            "eodatasets3",
            "https://github.com/GeoscienceAustralia/eo-datasets",
            eodatasets3.__version__,
        )

        crs, grid_docs, measurement_docs = self._measurements.as_geo_docs()

        if measurement_docs and sort_bands:
            measurement_docs = dict(sorted(measurement_docs.items()))

        valid_data = self._measurements.consume_and_get_valid_data()
        # Avoid the messiness of different empty collection types.
        # (to have a non-null geometry we'd also need non-null grids and crses)
        if valid_data.is_empty:
            valid_data = None

        # If we wrote any data, a temporary work directory will have been initialised.
        if self._base_output_folder:
            checksum_path = self.names.checksum_path(self._work_path)
            processing_metadata = self.names.metadata_path(
                self._work_path, suffix="proc-info.yaml"
            )
            self.add_accessory_file("checksum:sha1", checksum_path)
            self.add_accessory_file("metadata:processor", processing_metadata)

        dataset = DatasetDoc(
            id=self.dataset_id,
            label=self.label,
            product=ProductDoc(
                name=self.names.product_name, href=self.names.product_uri
            ),
            crs=self._crs_str(crs) if crs is not None else None,
            geometry=valid_data,
            grids=grid_docs,
            properties=self.properties,
            accessories={
                name: AccessoryDoc(path, name=name)
                for name, path in self._accessories.items()
            },
            measurements=measurement_docs,
            lineage=self._lineage,
        )

        doc = serialise.to_formatted_doc(dataset)
        self._write_yaml(
            doc,
            self._specified_metadata_path
            or self.names.metadata_path(self._work_path, suffix="odc-metadata.yaml"),
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

        # If we're using a tmp path, finish the package and move it into place.
        if self._base_output_folder:
            self._write_yaml(
                {**self._user_metadata, "software_versions": self._software_versions},
                processing_metadata,
                allow_external_paths=True,
            )
            self._checksum.write(checksum_path)

            # Match the lower r/w permission bits to the output folder.
            # (Temp directories default to 700 otherwise.)
            self._work_path.chmod(self._base_output_folder.stat().st_mode & 0o777)

            # GDAL writes extra metadata in aux files,
            # but we consider it a mistake if you're using those extensions.
            for aux_file in self._work_path.rglob("*.aux.xml"):
                warnings.warn(
                    f"Cleaning unexpected gdal aux file {aux_file.as_posix()!r}"
                )
                aux_file.unlink()

            # Now atomically move to final location.
            # Someone else may have created the output while we were working.
            # Try, and then decide how to handle it if so.
            try:
                self.destination_folder.parent.mkdir(parents=True, exist_ok=True)
                self._work_path.rename(self.destination_folder)
            except OSError:
                if not self.destination_folder.exists():
                    # Some other error?
                    raise

                if self._exists_behaviour == IfExists.Skip:
                    print(f"Skipping -- exists: {self.destination_folder}")
                elif self._exists_behaviour == IfExists.ThrowError:
                    raise
                elif self._exists_behaviour == IfExists.Overwrite:
                    raise NotImplementedError("overwriting outputs not yet implemented")
                else:
                    raise RuntimeError(
                        f"Unexpected exists behaviour: {self._exists_behaviour}"
                    )

        target_metadata_path = (
            self._specified_metadata_path
            or self.names.metadata_path(
                self.destination_folder, suffix="odc-metadata.yaml"
            )
        )
        assert target_metadata_path.exists()
        self._is_finished = True
        return dataset.id, target_metadata_path

    def _crs_str(self, crs: CRS) -> str:
        return f"epsg:{crs.to_epsg()}" if crs.is_epsg_code else crs.to_wkt()

    def write_thumbnail(
        self,
        red: str,
        green: str,
        blue: str,
        resampling: Resampling = Resampling.average,
        static_stretch: Tuple[int, int] = None,
        percentile_stretch: Tuple[int, int] = (2, 98),
        scale_factor=10,
        kind: str = None,
    ):
        """
        Write a thumbnail for the dataset using the given measurements (specified by name) as r/g/b.

        (the measurements must already have been written.)

        If you have multiple thumbnails, you can specify the 'kind' to distinguish
        them (it will be put in the filename).

        Eg. GA's ARD has thumbnails of kind 'nbar' and 'nbart'.

        A linear stretch is performed on the colour. By default this is a dynamic 2% stretch
        (the 2% and 98% percentile values of the input). The static_stretch parameter will
        override this with a static range of values.
        """
        thumb = self.names.thumbnail_name(self._work_path, kind=kind)
        measurements = dict(
            (name, (grid, path)) for grid, name, path in self._measurements.iter_paths()
        )

        missing_measurements = {red, green, blue} - set(measurements)
        if missing_measurements:
            raise IncompleteDatasetError(
                ValidationMessage(
                    Level.error,
                    "missing_thumb_measurements",
                    f"Thumbnail measurements are missing: no measurements called {missing_measurements!r}. ",
                    hint=f"Available measurements: {', '.join(measurements)}",
                )
            )
        rgbs = [measurements[b] for b in (red, green, blue)]
        unique_grids: List[GridSpec] = list(set(grid for grid, path in rgbs))
        if len(unique_grids) != 1:
            raise NotImplementedError(
                "Thumbnails can only currently be created from bands of the same grid spec."
            )
        grid = unique_grids[0]

        FileWrite().create_thumbnail(
            tuple(path for grid, path in rgbs),
            thumb,
            out_scale=scale_factor,
            resampling=resampling,
            static_stretch=static_stretch,
            percentile_stretch=percentile_stretch,
            input_geobox=grid,
        )
        self._checksum.add_file(thumb)

        accessory_name = "thumbnail"
        if kind:
            accessory_name += f":{kind}"
        self.add_accessory_file(accessory_name, thumb)

    def add_accessory_file(self, name: str, path: Path):
        """
        Add a reference to a file that is not an ODC measurement.

        Such as native metadata, thumbanils, checksums, etc.

        By convention, the name should have prefixes with their category, such as
        'metadata:' or 'thumbnail:'
        """
        existing_path = self._accessories.get(name)
        if existing_path is not None and existing_path != path:
            raise ValueError(
                f"Duplicate accessory name {name!r}. "
                f"New: {path.as_posix()!r}, previous: {existing_path.as_posix()!r}"
            )
        self._accessories[name] = path

    def _write_yaml(self, doc, path, allow_external_paths=False):
        make_paths_relative(
            doc, path.parent, allow_paths_outside_base=allow_external_paths
        )
        serialise.dump_yaml(path, doc)
        self._checksum.add_file(path)

    def iter_measurement_paths(
        self
    ) -> Generator[Tuple[GridSpec, str, Path], None, None]:
        """
        Iterate through the list of measurement names that have been written, and their current (temporary) paths.

        TODO: Perhaps we want to return a real measurement structure here as it's not very extensible.
        """
        return self._measurements.iter_paths()
