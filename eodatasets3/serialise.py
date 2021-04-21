import uuid
from datetime import datetime
from pathlib import Path, PurePath
from typing import Dict, Tuple, Text, IO, Union, Iterable, Mapping
from uuid import UUID

import attr
import cattr
import ciso8601
import click
import jsonschema
import numpy
import shapely
import shapely.affinity
import shapely.ops
from affine import Affine
from ruamel.yaml import YAML, ruamel, Representer
from ruamel.yaml.comments import CommentedMap, CommentedSeq
from shapely.geometry import shape
from shapely.geometry.base import BaseGeometry

from eodatasets3.model import (
    DatasetDoc,
    ODC_DATASET_SCHEMA_URL,
    StacPropertyView,
)
from eodatasets3.properties import FileFormat


def _format_representer(dumper, data: FileFormat):
    return dumper.represent_scalar("tag:yaml.org,2002:str", f"{data.name}")


def _uuid_representer(dumper, data):
    """
    :type dumper: yaml.representer.BaseRepresenter
    :type data: uuid.UUID
    :rtype: yaml.nodes.Node
    """
    return dumper.represent_scalar("tag:yaml.org,2002:str", f"{data}")


def represent_datetime(self, data: datetime):
    """
    The default Ruamel representer strips 'Z' suffixes for UTC.

    But we like to be explicit.
    """
    # If there's a non-utc timezone, use it.
    if data.tzinfo is not None and (data.utcoffset().total_seconds() > 0):
        value = data.isoformat(" ")
    else:
        # Otherwise it's UTC (including when tz==null).
        value = data.replace(tzinfo=None).isoformat(" ") + "Z"
    return self.represent_scalar("tag:yaml.org,2002:timestamp", value)


def represent_numpy_datetime(self, data: numpy.datetime64):
    return represent_datetime(self, data.astype("M8[ms]").tolist())


def represent_paths(self, data: PurePath):
    return Representer.represent_str(self, data.as_posix())


def _init_yaml() -> YAML:
    yaml = YAML()

    yaml.representer.add_representer(FileFormat, _format_representer)
    yaml.representer.add_multi_representer(UUID, _uuid_representer)
    yaml.representer.add_representer(datetime, represent_datetime)
    yaml.representer.add_multi_representer(PurePath, represent_paths)

    # WAGL spits out many numpy primitives in docs.
    yaml.representer.add_representer(numpy.int8, Representer.represent_int)
    yaml.representer.add_representer(numpy.uint8, Representer.represent_int)
    yaml.representer.add_representer(numpy.int16, Representer.represent_int)
    yaml.representer.add_representer(numpy.uint16, Representer.represent_int)
    yaml.representer.add_representer(numpy.int32, Representer.represent_int)
    yaml.representer.add_representer(numpy.uint32, Representer.represent_int)
    yaml.representer.add_representer(numpy.int, Representer.represent_int)
    yaml.representer.add_representer(numpy.int64, Representer.represent_int)
    yaml.representer.add_representer(numpy.uint64, Representer.represent_int)
    yaml.representer.add_representer(numpy.float, Representer.represent_float)
    yaml.representer.add_representer(numpy.float32, Representer.represent_float)
    yaml.representer.add_representer(numpy.float64, Representer.represent_float)
    yaml.representer.add_representer(numpy.ndarray, Representer.represent_list)
    yaml.representer.add_representer(numpy.datetime64, represent_numpy_datetime)

    # Match yamllint default expectations. (Explicit start/end are recommended to tell if a file is cut off)
    yaml.width = 80
    yaml.explicit_start = True
    yaml.explicit_end = True

    return yaml


def dump_yaml(output_yaml: Path, *docs: Mapping) -> None:
    if not output_yaml.name.lower().endswith(".yaml"):
        raise ValueError(
            "YAML filename doesn't end in *.yaml (?). Received {!r}".format(output_yaml)
        )

    yaml = _init_yaml()
    with output_yaml.open("w") as stream:
        yaml.dump_all(docs, stream)


def dumps_yaml(stream, *docs: Mapping) -> None:
    """Dump yaml through a stream, using the default serialisation settings."""
    return _init_yaml().dump_all(docs, stream=stream)


def load_yaml(p: Path) -> Dict:
    with p.open() as f:
        return YAML(typ="safe").load(f)


def loads_yaml(stream: Union[Text, IO]) -> Iterable[Dict]:
    """Dump yaml through a stream, using the default deserialisation settings."""
    return YAML(typ="safe").load_all(stream)


def from_path(path: Path, skip_validation=False) -> DatasetDoc:
    if path.suffix.lower() not in (".yaml", ".yml"):
        raise ValueError(f"Unexpected file type {path.suffix}. Expected yaml")

    return from_doc(load_yaml(path), skip_validation=skip_validation)


class InvalidDataset(Exception):
    def __init__(self, path: Path, error_code: str, reason: str) -> None:
        self.path = path
        self.error_code = error_code
        self.reason = reason


def _get_schema_validator(p: Path) -> jsonschema.Draft6Validator:
    with p.open() as f:
        schema = ruamel.yaml.safe_load(f)
    klass = jsonschema.validators.validator_for(schema)
    klass.check_schema(schema)
    return klass(schema, types=dict(array=(list, tuple)))


DATASET_SCHEMA = _get_schema_validator(Path(__file__).parent / "dataset.schema.yaml")


def from_doc(doc: Dict, skip_validation=False) -> DatasetDoc:
    """
    Convert a document to a dataset.

    By default it will validate it against the schema, which will result in far more
    useful error messages if fields are missing.
    """

    if not skip_validation:
        DATASET_SCHEMA.validate(doc)

    # TODO: stable cattrs (<1.0) balks at the $schema variable.
    doc = doc.copy()
    del doc["$schema"]

    c = cattr.Converter()
    c.register_structure_hook(uuid.UUID, _structure_as_uuid)
    c.register_structure_hook(BaseGeometry, _structure_as_shape)
    c.register_structure_hook(StacPropertyView, _structure_as_stac_props)

    c.register_structure_hook(Affine, _structure_as_affine)

    c.register_unstructure_hook(StacPropertyView, _unstructure_as_stac_props)
    return c.structure(doc, DatasetDoc)


def _structure_as_uuid(d, t):
    return uuid.UUID(str(d))


def _structure_as_stac_props(d, t):
    return StacPropertyView(d)


def _structure_as_affine(d: Tuple, t):
    if len(d) != 9:
        raise ValueError(f"Expected 9 coefficients in transform. Got {d!r}")

    if tuple(d[-3:]) != (0.0, 0.0, 1.0):
        raise ValueError(
            f"Nine-element affine should always end in [0, 0, 1]. Got {d!r}"
        )

    return Affine(*d[:-3])


def _unstructure_as_stac_props(v: StacPropertyView):
    return v._props


def _structure_as_shape(d, t):
    return shape(d)


def to_doc(d: DatasetDoc) -> Dict:
    """
    Serialise a DatasetDoc to a dict

    If you plan to write this out as a yaml file on disk, you're
    better off with `to_formatted_doc()`.
    """
    doc = attr.asdict(
        d,
        recurse=True,
        dict_factory=dict,
        # Exclude fields that are the default.
        filter=lambda attr, value: "doc_exclude" not in attr.metadata
        and value != attr.default
        # Exclude any fields set to None. The distinction should never matter in our docs.
        and value is not None,
        retain_collection_types=False,
    )
    doc["$schema"] = ODC_DATASET_SCHEMA_URL
    if d.geometry is not None:
        doc["geometry"] = shapely.geometry.mapping(d.geometry)
    doc["id"] = str(d.id)
    doc["properties"] = dict(d.properties)
    return doc


def to_formatted_doc(d: DatasetDoc) -> CommentedMap:
    """Serialise a DatasetDoc to a yaml-serialisation-ready dict"""
    doc = prepare_formatting(to_doc(d))
    # Add user-readable names for measurements as a comment if present.
    if d.measurements:
        for band_name, band_doc in d.measurements.items():
            if band_doc.alias and band_name.lower() != band_doc.alias.lower():
                doc["measurements"].yaml_add_eol_comment(band_doc.alias, band_name)

    return doc


def _stac_key_order(key: str):
    """All keys in alphabetical order, but unprefixed keys first."""
    if ":" in key:
        # Tilde comes after all alphanumerics.
        return f"~{key}"
    else:
        return key


def _eo3_key_order(keyval: str):
    """
    Order keys in an an EO3 document.

    Suitable for sorted() func usage.
    """
    key, val = keyval
    try:
        i = _EO3_PROPERTY_ORDER.index(key)
        if i == -1:
            return 999
        return i
    except ValueError:
        return 999


# A logical, readable order for properties to be in a dataset document.
_EO3_PROPERTY_ORDER = [
    "$schema",
    # Products / Types
    "name",
    "license",
    "metadata_type",
    "description",
    "metadata",
    # EO3
    "id",
    "label",
    "product",
    "crs",
    "geometry",
    "grids",
    "properties",
    "measurements",
    "accessories",
    "lineage",
    "location",
    "locations",
]


def prepare_formatting(d: Mapping) -> CommentedMap:
    """
    Format an eo3 dataset dict for human-readable yaml serialisation.

    This will order fields, add whitespace, comments, etc.

    Output is intended for ruamel.yaml.
    """
    # Sort properties for readability.
    doc = CommentedMap(sorted(d.items(), key=_eo3_key_order))
    doc["properties"] = CommentedMap(
        sorted(doc["properties"].items(), key=_stac_key_order)
    )

    # Whitespace
    doc.yaml_set_comment_before_after_key("$schema", before="Dataset")
    if "geometry" in doc:
        # Set some numeric fields to be compact yaml format.
        _use_compact_format(doc["geometry"], "coordinates")
    if "grids" in doc:
        for grid in doc["grids"].values():
            _use_compact_format(grid, "shape", "transform")

    _add_space_before(
        doc,
        "label" if "label" in doc else "id",
        "crs",
        "properties",
        "measurements",
        "accessories",
        "lineage",
        "location",
        "locations",
    )

    p: CommentedMap = doc["properties"]
    p.yaml_add_eol_comment("# Ground sample distance (m)", "eo:gsd")

    return doc


def _use_compact_format(d: dict, *keys):
    """Change the given sequence to compact YAML form"""
    for key in keys:
        if key in d:
            d[key] = CommentedSeq(d[key])
            d[key].fa.set_flow_style()


def _add_space_before(d: CommentedMap, *keys):
    """Add an empty line to the document before a section (key)"""
    for key in keys:
        d.yaml_set_comment_before_after_key(key, before="\n")


class ClickDatetime(click.ParamType):
    """
    Take a datetime parameter, supporting any ISO8601 date/time/timezone combination.
    """

    name = "date"

    def convert(self, value, param, ctx):
        if value is None:
            return value

        if isinstance(value, datetime):
            return value

        try:
            return ciso8601.parse_datetime(value)
        except ValueError:
            self.fail(
                (
                    "Invalid date string {!r}. Expected any ISO date/time format "
                    '(eg. "2017-04-03" or "2014-05-14 12:34")'.format(value)
                ),
                param,
                ctx,
            )
