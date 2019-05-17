from datetime import datetime

# flake8 doesn't recognise type hints as usage
from pathlib import Path  # noqa: F401
from typing import Dict  # noqa: F401
from uuid import UUID

import ciso8601
import click
from ruamel.yaml import YAML

from eodatasets.prepare.model import FileFormat


# pylint: disable=too-many-ancestors


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


def init_yaml(yaml: YAML):
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
    init_yaml(yaml)
    yaml.dump(doc, stream)


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
