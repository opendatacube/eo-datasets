import collections

from datetime import datetime

# flake8 doesn't recognise type hints as usage
from pathlib import Path  # noqa: F401
from typing import Dict  # noqa: F401

import ciso8601
import click
import yaml

from eodatasets import serialise as eodserial
from eodatasets.prepare.model import FileFormat


# pylint: disable=too-many-ancestors
class OrderPreservingDumper(yaml.SafeDumper):
    pass


def _dict_representer(dumper, data):
    return dumper.represent_mapping(
        yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, data.items()
    )


def _format_representer(dumper, data: FileFormat):
    return dumper.represent_scalar(u"tag:yaml.org,2002:str", "%s" % data.name)


OrderPreservingDumper.add_representer(collections.OrderedDict, _dict_representer)
eodserial.init_yaml_handling(OrderPreservingDumper)

OrderPreservingDumper.add_representer(FileFormat, _format_representer)


def dump_yaml(output_yaml, doc):
    # type: (Path, Dict) -> None
    if not output_yaml.name.lower().endswith(".yaml"):
        raise ValueError(
            "YAML filename doesn't end in *.yaml (?). Received {!r}".format(output_yaml)
        )

    with output_yaml.open("w") as stream:
        dump_yaml_to_stream(stream, doc)


def dump_yaml_to_stream(stream, doc):
    yaml.dump(
        # In CPython 3.5+ dicts are already ordered, but yaml.dump() does not maintain that order unless we do this.
        collections.OrderedDict(doc),
        stream,
        Dumper=OrderPreservingDumper,
        default_flow_style=False,
        indent=4,
    )


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
