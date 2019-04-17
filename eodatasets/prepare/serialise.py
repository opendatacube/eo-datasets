import collections

# flake8 doesn't recognise type hints as usage
from pathlib import Path  # noqa: F401
from typing import Dict  # noqa: F401

import yaml


# pylint: disable=too-many-ancestors
class OrderPreservingDumper(yaml.SafeDumper):
    pass


def _dict_representer(dumper, data):
    return dumper.represent_mapping(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, data.items())


OrderPreservingDumper.add_representer(collections.OrderedDict, _dict_representer)


def dump_yaml(output_yaml, doc):
    # type: (Path, Dict) -> None
    if not output_yaml.name.lower().endswith('.yaml'):
        raise ValueError("YAML filename doesn't end in *.yaml (?). Received {!r}".format(output_yaml))

    with output_yaml.open('w') as stream:
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
