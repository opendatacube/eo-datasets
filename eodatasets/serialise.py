# coding=utf-8
from __future__ import absolute_import

import collections
import datetime
import logging
import os
import time
import uuid

import yaml
from pathlib import Path

from eodatasets import compat, documents
import eodatasets.type as ptype

_LOG = logging.getLogger(__name__)


def read_dataset_metadata(dataset_path):
    """
    Read the metadata for a dataset

    :type dataset_path: Path
    :rtype: ptype.DatasetMetadata
    """
    metadata_path = documents.find_metadata_path(dataset_path)
    if metadata_path is None or not metadata_path.exists():
        return None

    return read_yaml_metadata(metadata_path)


def write_dataset_metadata(dataset_path, dataset_metadata):
    """
    Write the given metadata for the given dataset path.

    :type dataset_path: Path
    :type dataset_metadata: ptype.DatasetMetadata
    :rtype: Path
    :return Path to the metadata file.
    """
    _LOG.debug('Generating YAML for dataset: %r', dataset_metadata)
    metadata_path = documents.new_metadata_path(dataset_path)
    write_yaml_metadata(dataset_metadata, metadata_path)
    return metadata_path


def init_yaml_handling():
    """
    Allow load/dump of our custom classes in YAML.
    """

    def simpleobject_representer(dumper, data):
        """

        Output the properties of a SimpleObject implementation as a map.

        We deliberately output in constructor-arg order for human readability.

        eg. the document id should be at the top of the document.

        :type dumper: yaml.representer.BaseRepresenter
        :type data: ptype.SimpleObject
        :rtype: yaml.nodes.Node
        """
        k_v = [(_clean_identifier(k), v) for k, v in data.items_ordered()]

        return dumper.represent_mapping(u'tag:yaml.org,2002:map', k_v)

    def ordereddict_representer(dumper, data):
        """
        Output an OrderedDict as a dict. The order is purely for readability of the document.

        :type dumper: yaml.representer.BaseRepresenter
        :type data: collections.OrderedDict
        :rtype: yaml.nodes.Node
        """
        return dumper.represent_mapping(u'tag:yaml.org,2002:map', data.items())

    def uuid_representer(dumper, data):
        """
        :type dumper: yaml.representer.BaseRepresenter
        :type data: uuid.UUID
        :rtype: yaml.nodes.Node
        """
        return dumper.represent_scalar(u'tag:yaml.org,2002:str', '%s' % data)

    def unicode_representer(dumper, data):
        """
        It's strange that PyYaml doesn't use unicode internally. We're doing everything in UTF-8 so we translate.
        :type dumper: yaml.representer.BaseRepresenter
        :type data: unicode
        :rtype: yaml.nodes.Node
        """
        return dumper.represent_scalar(u'tag:yaml.org,2002:str', data.encode('utf-8'))

    yaml.add_multi_representer(ptype.SimpleObject, simpleobject_representer)
    yaml.add_multi_representer(uuid.UUID, uuid_representer)
    # TODO: This proabbly shouldn't be performed globally as it changes the output behaviour for a built-in type.
    # (although the default behaviour doesn't seem very widely useful: it outputs as a list.)
    yaml.add_multi_representer(collections.OrderedDict, ordereddict_representer)
    if compat.PY2:
        # 'unicode' is undefined in python 3
        # pylint: disable=undefined-variable
        yaml.add_representer(unicode, unicode_representer)


def _create_relative_dumper(folder):
    """
    Create a Dump implementation that can dump pathlib.Path() objects as relative paths.

    Paths will be output relative to the given folder param.

    :type folder: str
    :rtype: yaml.Dumper
    """

    # We can't control how many ancestors this dumper API uses, Pylint.
    # pylint: disable=too-many-ancestors
    class RelativeDumper(yaml.Dumper):
        pass

    def path_representer(dumper, data):
        """
        :type dumper: yaml.representer.BaseRepresenter
        :type data: pathlib.Path
        :rtype: yaml.nodes.Node
        """
        if not data.is_absolute():
            data = Path(folder).joinpath(data)
        return dumper.represent_scalar(u'tag:yaml.org,2002:str', str(data.relative_to(folder)))

    RelativeDumper.add_multi_representer(Path, path_representer)
    RelativeDumper.ignore_aliases = lambda self, data: True

    return RelativeDumper


def write_yaml_metadata(d, metadata_path, target_directory=None):
    """
    Write the given dataset to yaml.

    All 'Path' values are converted to relative paths: relative to the given
    target directory.

    :type d: DatasetMetadata
    :type target_directory: str or Path
    :type metadata_path: str or Path
    """
    metadata_file = str(metadata_path)
    if not target_directory:
        target_directory = os.path.dirname(os.path.abspath(metadata_file))

    _LOG.info('Writing metadata file %r', metadata_file)
    with open(str(metadata_file), 'w') as f:
        yaml.dump(
            d,
            f,
            default_flow_style=False,
            indent=4,
            Dumper=_create_relative_dumper(str(target_directory)),
            allow_unicode=True
        )


def read_yaml_metadata(metadata_file):
    """
    :type metadata_file: Path
    :rtype: DatasetMetadata
    """
    doc = list(documents.read_documents(Path(metadata_file)))

    if len(doc) > 1:
        raise NotImplementedError(
            'Multiple datasets in one metadata file is not yet supported: {}'.format(
                metadata_file
            )
        )

    _, doc = doc[0]
    return read_dict_metadata(doc)


def read_dict_metadata(dict_):
    return ptype.DatasetMetadata.from_dict(dict_)


def write_property_metadata(d, metadata_file, target_directory):
    def _clean_val(v):
        if isinstance(v, Path):
            return str(v.relative_to(target_directory))

        return v

    with open(str(metadata_file), 'w') as f:
        f.writelines(['%s=%r\n' % (k, _clean_val(v)) for k, v in as_flat_key_value(d)])


def _clean_identifier(arg):
    """
    Our class property names have an appended underscore when they clash with Python names.

    This will strip off any trailing underscores, ready for serialisation.

    :type arg: str
    :rtype: str

    >>> _clean_identifier('id_')
    'id'
    >>> _clean_identifier('id')
    'id'
    """
    return arg[:-1] if arg.endswith('_') else arg


# pylint: disable=too-many-branches
def as_flat_key_value(o, relative_to=None, key_separator='.', key_prefix=''):
    """
    Output as a flat stream of keys and values. No nesting.

    Nested keys are joined by the given separator arugment (default '.').

    This is suitable for storage in simple key-value stores, such
    as the metadata in a gdal image.

    >>> sorted(list(as_flat_key_value({'a': {'b': 1}, 'c': 2})))
    [('a.b', 1), ('c', 2)]
    """
    if relative_to is None:
        relative_to = os.getcwd()

    def recur(key, value):
        return as_flat_key_value(
            value,
            relative_to=relative_to,
            key_separator=key_separator,
            key_prefix=namespace(key, key_prefix)
        )

    def namespace(k, key_prefix):
        k = _clean_identifier(k)

        if not key_prefix:
            return k

        return key_separator.join([key_prefix, k])

    if isinstance(o, compat.string_types) or \
        isinstance(o, compat.integer_types) or \
        isinstance(o, float):
        yield key_prefix, o
    elif isinstance(o, dict):
        for k in sorted(o):
            v = o[k]
            for nested_k, nested_v in recur(k, v):
                yield nested_k, nested_v
    elif isinstance(o, (list, set)):
        for index, v in enumerate(o):
            for nested_k, nested_v in recur(str(index), v):
                yield nested_k, nested_v
    elif isinstance(o, (datetime.datetime, datetime.date)):
        yield key_prefix, o.isoformat()
    elif isinstance(o, time.struct_time):
        yield key_prefix, time.strftime('%Y-%m-%dT%H:%M:%S', o)
    elif isinstance(o, uuid.UUID):
        yield key_prefix, str(o)
    elif isinstance(o, Path):
        if not o.is_absolute():
            _LOG.warning('Non-absolute path: %r', o)
        val = o.relative_to(relative_to) if o.is_absolute() else o
        yield key_prefix, str(val)
    elif o is None:
        yield key_prefix, None
    elif isinstance(o, ptype.SimpleObject):
        for k, v in o.items_ordered():
            for nested_k, nested_v in recur(k, v):
                yield nested_k, nested_v
    else:
        raise ValueError('Unhandled type: %s (%s.%s). Value: %s' %
                         (type(o), type(o).__module__, type(o).__name__, repr(o)))


init_yaml_handling()
