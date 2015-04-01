import datetime
import inspect
import os
import socket
import collections
import uuid
import time
import logging

from pathlib import Path
import yaml


_LOG = logging.getLogger()


class SimpleObject(object):
    """
    An object with identical constructor arguments and properties.

    Implements repr and eq methods that print/compare all properties.

    Beware of cyclic dependencies in properties
    """

    def __repr__(self):
        """
        >>> class TestObj(SimpleObject):
        ...     def __init__(self, a, b=None, c=None):
        ...         self.a = a
        ...         self.b = b
        ...         self.c = c
        >>> t = TestObj(a=1)
        >>> repr(t)
        'TestObj(a=1)'
        >>> eval(repr(t)) == t
        True
        >>> t = TestObj(a=1, b='b', c=3)
        >>> repr(t)
        "TestObj(a=1, b='b', c=3)"
        >>> eval(repr(t)) == t
        True
        """
        # Print constructor with properties in order.
        return '%s(%s)' % (
            self.__class__.__name__,
            ", ".join(map("{0[0]}={0[1]!r}".format, self.items_ordered()))
        )

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.__dict__ == other.__dict__
        else:
            return False

    def items_ordered(self):
        """
        Generator of all properties as (k, v) tuples. Properties are output in the same order
        as the constructor.

        (ordered output is primarily useful for readability: such as log output.)
        :return:
        """
        constructor_spec = inspect.getargspec(self.__class__.__init__)
        constructor_args = constructor_spec.args[1:]

        # Record the default value for each property (from constructor)
        defaultless_count = len(constructor_args) - len(constructor_spec.defaults or [])
        value_defaults = ([None] * defaultless_count) + list(constructor_spec.defaults or [])
        value_defaults.reverse()

        for k in constructor_args:
            v = getattr(self, k)
            default_value = value_defaults.pop()

            # Skip None properties that default to None
            if v is None and default_value is None:
                continue

            yield k, v

    @classmethod
    def from_dict(cls, dict):
        pass


class PlatformMetadata(SimpleObject):
    def __init__(self, code=None):
        self.code = code


class InstrumentMetadata(SimpleObject):
    def __init__(self, name=None, type_=None, operation_mode=None):
        self.name = name
        self.type_ = type_
        self.operation_mode = operation_mode


class FormatMetadata(SimpleObject):
    def __init__(self, name=None, version=None):
        self.name = name
        self.version = version


class Coord(SimpleObject):
    def __init__(self, lat, lon, height=None):
        self.lat = lat
        self.lon = lon
        self.height = height


class ExtentMetadata(SimpleObject):
    def __init__(self,
                 reference_system=None,
                 coord=None,
                 from_dt=None,
                 center_dt=None,
                 to_dt=None):
        """

        :type reference_system: str
        :type from_dt:
        :type to_dt:
        """
        # Example: 'WGS84'
        self.reference_system = reference_system

        self.coord = coord

        self.from_dt = from_dt
        self.center_dt = center_dt
        self.to_dt = to_dt


class GridSpatialMetadata(SimpleObject):
    def __init__(self, dimensions=None, projection=None):
        """
        :type dimensions: list of DimensionMetadata
        :type projection: ProjectionMetadata
        """
        # TODO: We don't have a single set of dimensions? Per band?
        self.dimensions = dimensions

        self.projection = projection


class ProjectionMetadata(SimpleObject):
    def __init__(self, centre_point=None,
                 geo_ref_points=None,
                 datum=None, ellipsoid=None, point_in_pixel=None,
                 map_projection=None,
                 orientation=None,
                 resampling_option=None,
                 zone=None):
        # The units of these points are dependent on the reference system.
        # Eg. 'GDA94' points are a distance in meters.

        #: :type: Point
        self.centre_point = centre_point

        #: :type: Polygon
        self.geo_ref_points = geo_ref_points

        # Eg. 'GDA94'
        #: :type: str
        self.datum = datum

        # Eg. 'GRS80'
        #: :type: str
        self.ellipsoid = ellipsoid

        self.point_in_pixel = point_in_pixel

        self.orientation = orientation

        self.map_projection = map_projection

        self.resampling_option = resampling_option
        self.zone = zone


class BrowseMetadata(SimpleObject):
    def __init__(self, path=None, file_type=None, checksum_md5=None,
                 cell_size=None, red_band=None, green_band=None, blue_band=None):

        #: :type: pathlib.Path
        self.path = path
        self.file_type = file_type
        self.checksum_md5 = checksum_md5

        self.cell_size = cell_size
        self.red_band = red_band
        self.green_band = green_band
        self.blue_band = blue_band


class BandMetadata(SimpleObject):
    def __init__(self, path=None, type=None, label=None, number=None, shape=None, cell_size=None, checksum_md5=None):

        # Prefer absolute paths. Path objects can be converted to relative
        # during serialisation (relative to whatever we want).
        #: :type: pathlib.Path
        self.path = path

        self.type = type

        # Eg. 'visible_red'
        self.label = label

        # Band number. Not always a number (eg. 'QA')
        self.number = number

        self.shape = shape
        self.cell_size = cell_size
        self.checksum_md5 = checksum_md5


class ImageMetadata(SimpleObject):
    def __init__(self,
                 satellite_ref_point_start=None,
                 satellite_ref_point_end=None,
                 cloud_cover_percentage=None,
                 cloud_cover_details=None,
                 sun_azimuth=None,
                 sun_elevation=None,
                 sun_earth_distance=None,
                 ground_control_points_model=None,
                 geometric_rmse_model=None,
                 geometric_rmse_model_x=None,
                 geometric_rmse_model_y=None,
                 viewing_incidence_angle_long_track=None,
                 viewing_incidence_angle_x_track=None,
                 bands=None):
        self.satellite_ref_point_start = satellite_ref_point_start
        self.satellite_ref_point_end = satellite_ref_point_end

        self.cloud_cover_percentage = cloud_cover_percentage
        self.cloud_cover_details = cloud_cover_details
        self.sun_azimuth = sun_azimuth
        self.sun_elevation = sun_elevation
        self.sun_earth_distance = sun_earth_distance

        self.ground_control_points_model = ground_control_points_model
        self.geometric_rmse_model = geometric_rmse_model
        self.geometric_rmse_model_x = geometric_rmse_model_x
        self.geometric_rmse_model_y = geometric_rmse_model_y

        self.viewing_incidence_angle_long_track = viewing_incidence_angle_long_track
        self.viewing_incidence_angle_x_track = viewing_incidence_angle_x_track

        #: :type: dict of (str, BandMetadata)
        self.bands = bands


class AlgorithmMetadata(SimpleObject):
    def __init__(self, name=None, version=None, parameters=None):
        self.name = name
        self.version = version
        self.parameters = parameters


_RUNTIME_ID = uuid.uuid1()


class MachineMetadata(SimpleObject):
    def __init__(self, hostname=None, runtime_id=None, type_id=None, version=None, uname=None):
        self.hostname = hostname or socket.getfqdn()
        self.runtime_id = runtime_id or _RUNTIME_ID
        self.type_id = type_id or 'jobmanager'
        self.version = version or '2.4.0'
        self.uname = uname or ' '.join(os.uname())


class AncillaryMetadata(SimpleObject):
    def __init__(self, type_=None, name=None, uri=None):
        self.type_ = type_
        self.name = name
        self.uri = uri


class LineageMetadata(SimpleObject):
    def __init__(self, algorithm=None, machine=None, ancillary=None, source_datasets=None):
        #: :type: AlgorithmMetadata
        self.algorithm = algorithm

        #: :type: MachineMetadata
        self.machine = machine

        #: :type: dict of (str, AncillaryMetadata)
        self.ancillary = ancillary

        #: :type: dict of (str, DatasetMetadata)
        self.source_datasets = source_datasets


class DatasetMetadata(SimpleObject):
    def __init__(self, id_=None,
                 ga_label=None,
                 ga_level=None,
                 usgs_dataset_id=None,
                 product_type=None,
                 creation_dt=None,
                 size_bytes=None,
                 platform=None,
                 instrument=None,
                 format_=None,
                 acquisition=None,
                 extent=None,
                 grid_spatial=None,
                 browse=None,
                 image=None,
                 lineage=None):
        super(DatasetMetadata, self).__init__()

        self.id_ = id_ or uuid.uuid1()
        self.creation_dt = creation_dt or datetime.datetime.utcnow()

        #: :type: int
        self.size_bytes = size_bytes
        self.ga_label = ga_label
        self.ga_level = ga_level
        self.usgs_dataset_id = usgs_dataset_id

        self.product_type = product_type

        #: :type: PlatformMetadata
        self.platform = platform
        #: :type: InstrumentMetadata
        self.instrument = instrument
        #: :type: FormatMetadata
        self.format_ = format_

        #: :type: AcquisitionMetadata
        self.acquisition = acquisition

        #: :type: ExtentMetadata
        self.extent = extent

        #: :type: GridSpatialMetadata
        self.grid_spatial = grid_spatial
        #: :type: dict of (str, BrowseMetadata)
        self.browse = browse
        #: :type: ImageMetadata
        self.image = image
        #: :type: LineageMetadata
        self.lineage = lineage


class IdentificationMd(object):
    def __init__(self):
        super(IdentificationMd, self).__init__()

        self.dataset_id = None
        self.citation = None
        self.description = None


from yaml.representer import BaseRepresenter


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
    It's strange that PyYaml doesn't use unicode internally... but we're doing everything in UTF-8 so we'll translate.
    :type dumper: BaseRepresenter
    :type data: unicode
    :rtype: yaml.nodes.Node
    """
    return dumper.represent_scalar(u'tag:yaml.org,2002:str', data.encode('utf-8'))


yaml.add_multi_representer(SimpleObject, simpleobject_representer)
yaml.add_multi_representer(uuid.UUID, uuid_representer)
# TODO: This shouldn't be performed globally as it changes the output behaviour for a built-in type.
yaml.add_multi_representer(collections.OrderedDict, ordereddict_representer)
yaml.add_representer(unicode, unicode_representer)

class Point(SimpleObject):
    def __init__(self, x, y, z=None):
        self.x = x
        self.y = y
        self.z = z


class Range(SimpleObject):
    def __init__(self, from_, to):
        self.from_ = from_
        self.to = to


class Polygon(SimpleObject):
    def __init__(self, ul, ur, ll, lr):
        self.ul = ul
        self.ur = ur
        self.ll = ll
        self.lr = lr


class DimensionMetadata(SimpleObject):
    def __init__(self, name, resolution, size):
        self.name = name
        #: :type: float
        self.resolution = resolution
        #: :type: int
        self.size = size


class AcquisitionMetadata(SimpleObject):
    def __init__(self, aos=None, los=None, groundstation=None, heading=None, platform_orbit=None):
        """
        :type groundstation: GroundstationMetadata
        :type platform_orbit: int
        """
        self.aos = aos
        self.los = los
        self.groundstation = groundstation
        self.heading = heading
        self.platform_orbit = platform_orbit


class GroundstationMetadata(SimpleObject):
    def __init__(self, code, antenna_coord=None):
        """

        :param code: The GSI of the groundstation ("ASA" etc)
        :type antenna_coord: Coord
        :return:
        """

        self.code = code
        self.antenna_coord = antenna_coord


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
    elif isinstance(o, SimpleObject):
        for k, v in o.items_ordered():
            for nested_k, nested_v in as_flat_key_value(v, key_prefix=namespace(k, key_prefix)):
                yield nested_k, nested_v
    else:
        _LOG.debug('Unhandled type: %s (%s.%s). Value: %s', type(o), type(o).__module__, type(o).__name__, repr(o))
        for nested_k, nested_v in as_flat_key_value(o.__dict__, key_prefix=key_prefix):
            yield nested_k, nested_v
