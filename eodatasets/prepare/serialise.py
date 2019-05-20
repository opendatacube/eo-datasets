import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict
from uuid import UUID

import attr
import cattr
import ciso8601
import click
import jsonschema
import shapely
import shapely.affinity
import shapely.ops
from ruamel.yaml import YAML, ruamel
from ruamel.yaml.comments import CommentedMap, CommentedSeq
from shapely.geometry import shape
from shapely.geometry.base import BaseGeometry

from eodatasets.prepare.model import FileFormat, Dataset


with (Path(__file__).parent / 'dataset.schema.yaml').open() as f:
    _DATASET_SCHEMA = ruamel.yaml.safe_load(f)


def _format_representer(dumper, data: FileFormat):
    return dumper.represent_scalar(u"tag:yaml.org,2002:str", "%s" % data.name)


def _uuid_representer(dumper, data):
    """
    :type dumper: yaml.representer.BaseRepresenter
    :type data: uuid.UUID
    :rtype: yaml.nodes.Node
    """
    return dumper.represent_scalar(u"tag:yaml.org,2002:str", "%s" % data)


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


def _init_yaml(yaml: YAML):
    yaml.representer.add_representer(FileFormat, _format_representer)
    yaml.representer.add_multi_representer(UUID, _uuid_representer)
    yaml.representer.add_representer(datetime, represent_datetime)
    yaml.explicit_start = True


def dump_yaml(output_yaml, doc):
    # type: (Path, Dict) -> None
    if not output_yaml.name.lower().endswith(".yaml"):
        raise ValueError(
            "YAML filename doesn't end in *.yaml (?). Received {!r}".format(output_yaml)
        )

    with output_yaml.open("w") as stream:
        dump_yaml_to_stream(stream, doc)


def dump_yaml_to_stream(stream, doc):
    yaml = YAML()
    _init_yaml(yaml)
    yaml.dump(doc, stream)


def from_doc(doc: Dict):
    jsonschema.validate(doc, _DATASET_SCHEMA, types=dict(array=(list, tuple)))

    c = cattr.Converter()
    c.register_structure_hook(uuid.UUID, lambda d, t: uuid.UUID(d))
    c.register_structure_hook(BaseGeometry, lambda d, t: shape(d))
    return c.structure(doc, Dataset)


def to_doc(d: Dataset) -> Dict:
    return _to_doc(d, with_formatting=False)


def to_formatted_doc(d: Dataset) -> CommentedMap:
    return _to_doc(d, with_formatting=True)


def _to_doc(d: Dataset, with_formatting: bool):
    if with_formatting:
        doc = CommentedMap()
        doc.yaml_set_comment_before_after_key("$schema", before="Dataset")
    else:
        doc = {}

    doc["$schema"] = f"https://schemas.opendatacube.org/dataset"
    doc.update(
        attr.asdict(
            d,
            recurse=True,
            dict_factory=CommentedMap if with_formatting else dict,
            # Exclude fields that are the default.
            filter=lambda attr, value: "doc_exclude" not in attr.metadata
                                       and value != attr.default,
            retain_collection_types=False,
        )
    )
    doc["geometry"] = shapely.geometry.mapping(d.geometry)
    doc["id"] = str(d.id)

    if with_formatting:
        # Set some numeric fields to be compact yaml format.
        _use_compact_format(doc["geometry"], "coordinates")
        # _use_compact_format(d, "bbox")
        for grid in doc["grids"].values():
            _use_compact_format(grid, "shape", "transform")

        _add_space_before(doc, "id", "crs", "measurements", "properties", "lineage")

        p: CommentedMap = doc["properties"]
        p.yaml_add_eol_comment(
            "# When the dataset was processed/created", "odc:processing_datetime"
        )

    return doc


def _use_compact_format(d: dict, *keys):
    """Change the given sequence to compact YAML form"""
    for key in keys:
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
