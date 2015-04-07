

from yaml.representer import BaseRepresenter
import datetime
import os
import collections
import uuid
import time
import logging

from pathlib import Path
import pathlib
import yaml

import eodatasets.type as ptype


_LOG = logging.getLogger(__name__)


def simpleobject_representer(dumper, data):
    """

    :type dumper: BaseRepresenter
    :type data: SimpleObject
    :rtype: yaml.nodes.Node
    """

    # Loop through properties in order
    # Represent them as needed.
    # Return outer

    clean_arg = lambda arg: arg[:-1] if arg.endswith('_') else arg

    k_v = [(clean_arg(k), v) for k, v in data.items_ordered()]

    return dumper.represent_mapping(u'tag:yaml.org,2002:map', k_v)


def ordereddict_representer(dumper, data):
    """
    Output an OrderedDict as a dict. The order is purely for readability of the document.

    :type dumper: BaseRepresenter
    :type data: collections.OrderedDict
    :rtype: yaml.nodes.Node
    """
    return dumper.represent_mapping(u'tag:yaml.org,2002:map', data.items())


def uuid_representer(dumper, data):
    """
    :type dumper: BaseRepresenter
    :type data: uuid.UUID
    :rtype: yaml.nodes.Node
    """
    return dumper.represent_scalar(u'tag:yaml.org,2002:str', '%s' % data)


def unicode_representer(dumper, data):
    """
    It's strange that PyYaml doesn't use unicode internally. We're doing everything in UTF-8 so we translate.
    :type dumper: BaseRepresenter
    :type data: unicode
    :rtype: yaml.nodes.Node
    """
    return dumper.represent_scalar(u'tag:yaml.org,2002:str', data.encode('utf-8'))


def create_relative_dumper(folder):
    """
    Create a Dump implementation that can dump pathlib.Path() objects as relative paths.

    Paths will be output relative to the given folder param.

    :type folder: str
    :rtype: yaml.Dumper
    """
    class RelativeDumper(yaml.Dumper):
        pass

    def path_representer(dumper, data):
        """
        :type dumper: BaseRepresenter
        :type data: pathlib.Path
        :rtype: yaml.nodes.Node
        """
        if not data.is_absolute():
            data = Path(folder).joinpath(data)
        return dumper.represent_scalar(u'tag:yaml.org,2002:str', str(data.relative_to(folder)))

    RelativeDumper.add_multi_representer(pathlib.Path, path_representer)

    return RelativeDumper


def write_yaml_metadata(d, metadata_file, target_directory=None):
    """
    Write the given dataset to yaml.

    All 'Path' values are converted to relative paths: relative to the given
    target directory.

    :type d: DatasetMetadata
    :type target_directory: str
    :type metadata_file: str
    """
    if not target_directory:
        target_directory = os.path.dirname(os.path.abspath(metadata_file))

    _LOG.info('Writing metadata file %r', metadata_file)
    with open(str(metadata_file), 'w') as f:
        yaml.dump(
            d,
            f,
            default_flow_style=False,
            indent=4,
            Dumper=create_relative_dumper(target_directory),
            allow_unicode=True
        )


def read_yaml_metadata(metadata_file):
    """

    :type metadata: str
    :rtype: DatasetMetadata
    """
    with open(str(metadata_file), 'r') as f:
        dict_ = yaml.load(f)
    return read_dict_metadata(dict_)


def read_dict_metadata(dict_):
    return ptype.DatasetMetadata.from_dict(dict_)


def write_property_metadata(d, metadata_file, target_directory):
    def _clean_val(v):
        if isinstance(v, pathlib.Path):
            return str(v.relative_to(target_directory))

        return v
    with open(str(metadata_file), 'w') as f:
        f.writelines(['%s=%r\n' % (k, _clean_val(v)) for k, v in as_flat_key_value(d)])


def as_flat_key_value(o, relative_to=None, key_separator='.', key_prefix=''):
    """
    Output as a flat stream of keys and values. No nesting.

    Nested keys are joined by the given separator arugment (default '.').

    This is suitable for storage in simple key-value stores, such
    as the metadata in a gdal image.

    >>> list(as_flat_key_value({'a': {'b': 1}, 'c': 2}))
    [('a.b', 1), ('c', 2)]
    """
    if relative_to is None:
        relative_to = os.getcwd()

    def namespace(k, key_prefix):
        clean_arg = lambda arg: arg[:-1] if arg.endswith('_') else arg
        k = clean_arg(k)

        if not key_prefix:
            return k

        key = key_separator.join([key_prefix, k])
        return key

    if type(o) in (unicode, str, int, long, float):
        yield key_prefix, o
    elif isinstance(o, dict):
        for k, v in o.iteritems():
            for nested_k, nested_v in as_flat_key_value(v, key_prefix=namespace(k, key_prefix)):
                yield nested_k, nested_v
    elif isinstance(o, (list, set)):
        for index, v in enumerate(o):
            for nested_k, nested_v in as_flat_key_value(v, key_prefix=namespace(str(index), key_prefix)):
                yield nested_k, nested_v
    elif isinstance(o, (datetime.datetime, datetime.date)):
        yield key_prefix, o.isoformat()
    elif isinstance(o, time.struct_time):
        yield key_prefix, time.strftime('%Y-%m-%dT%H:%M:%S', o)
    elif isinstance(o, uuid.UUID):
        yield key_prefix, str(o)
    elif isinstance(o, Path):
        if not o.is_absolute():
            _LOG.warn('Non-absolute path: %r', o)
        val = o.relative_to(relative_to) if o.is_absolute() else o
        yield key_prefix, str(val)
    elif o is None:
        yield key_prefix, None
    elif isinstance(o, ptype.SimpleObject):
        for k, v in o.items_ordered():
            for nested_k, nested_v in as_flat_key_value(v, key_prefix=namespace(k, key_prefix)):
                yield nested_k, nested_v
    else:
        _LOG.debug('Unhandled type: %s (%s.%s). Value: %s', type(o), type(o).__module__, type(o).__name__, repr(o))
        for nested_k, nested_v in as_flat_key_value(o.__dict__, key_prefix=key_prefix):
            yield nested_k, nested_v


yaml.add_multi_representer(ptype.SimpleObject, simpleobject_representer)
yaml.add_multi_representer(uuid.UUID, uuid_representer)
# TODO: This proabbly shouldn't be performed globally as it changes the output behaviour for a built-in type.
# (although the default behaviour doesn't seem very widely useful: it outputs as a list.)
yaml.add_multi_representer(collections.OrderedDict, ordereddict_representer)
yaml.add_representer(unicode, unicode_representer)
