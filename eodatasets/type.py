# coding=utf-8
# Our metadata 'classes' validly have many arguments, to match the metadata format.
# pylint: disable=too-many-arguments,too-many-instance-attributes,too-many-locals
from __future__ import absolute_import
import datetime
import inspect
import uuid
import logging

from pathlib import Path


_LOG = logging.getLogger()


class SimpleObject(object):
    """
    An object with identical constructor arguments and properties.

    Implements repr and eq methods, and allows easy (de)serialisation between
    json/yaml/dicts.

    Beware of cyclic object dependencies in properties
    """

    # Constructors for specific properties.
    # Used for deserialisation from nested dicts (typically from parsed json/yaml)
    # (property name, parse function)
    PROPERTY_PARSERS = {}

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

    @classmethod
    def item_defaults(cls):
        """
        Return property names and their defaults in constructor order.

        (ordered output is primarily useful for readability: such as repr() or log output.)
        :rtype: [(str, obj)]
        """
        constructor_spec = inspect.getargspec(cls.__init__)
        constructor_args = constructor_spec.args[1:]

        defaults = constructor_spec.defaults
        # Record the default value for each property (from constructor)
        defaultless_count = len(constructor_args) - len(defaults or [])
        value_defaults = ([None] * defaultless_count) + list(defaults or [])

        return zip(constructor_args, value_defaults)

    def steal_fields_from(self, other):
        """
        Override any unset (default value) fields of mine with fields of the given object.
        :type other:
        :return:
        """
        for prop_name, default_value in self.__class__.item_defaults():
            if getattr(self, prop_name) == default_value:
                setattr(self, prop_name, getattr(other, prop_name))

    def items_ordered(self, skip_nones=True):
        """
        Generator of all property names and current values as (k, v) tuples.

        Properties are output in the same order as the constructor.

        (ordered output is primarily useful for readability: such as log output.)
        :rtype: [(str, obj)]
        """
        for prop, default_value in self.item_defaults():
            value = getattr(self, prop)

            # Skip None properties that default to None
            if skip_nones and (value is None and default_value is None):
                continue

            yield prop, value

    @classmethod
    def from_dict(cls, dict_):
        """
        Create an instance of this class from a given dictionary.

        (intended for use with the nested dictionaries from parsed json/yaml files)

        Subclasses can add to cls.PROPERTY_PARSERS to customise how some properties
        are parsed.

        :type dict_: dict[str, obj]
        """
        possible_properties = dict(cls.item_defaults())
        props = {}

        for key, value in dict_.items():
            if key not in possible_properties:
                # Reserved python words may have an underscore appended
                if key + '_' not in possible_properties:
                    _LOG.warn('Unknown property %r in %r', key, cls.__name__)
                    continue

                key += '_'

            if key in cls.PROPERTY_PARSERS:
                parser = cls.PROPERTY_PARSERS[key]
                try:
                    value = parser(value)
                except Exception:
                    _LOG.error('Error in %r: %r', key, value)
                    raise

            props[key] = value

        try:
            o = cls(**props)
        except TypeError:
            _LOG.error('Incorrect props for %s: %r', cls.__name__, props)
            raise

        return o

    @classmethod
    def from_dicts(cls, list_):
        """
        Create a list of these objects.

        Similar to from_dict, but will create a list of them.

        :type list_: Iterable[dict]
        :return: list of objects of this class
        """
        return [cls.from_dict(d) for d in list_]

    @classmethod
    def from_named_dicts(cls, dict_):
        """
        Create a dict of objects (maintaining the key name).

        Similar to :py:func:`from_dicts`, but operates on a dict instead of a list.

        :type dict_: dict[str, dict[str, ob]
        :return: dict of objects of this class
        """
        return dict([(k, cls.from_dict(v)) for (k, v) in dict_.items()])


class Point(SimpleObject):
    def __init__(self, x, y, z=None):
        self.x = x
        self.y = y
        self.z = z


class Range(SimpleObject):
    def __init__(self, from_, to):
        self.from_ = from_
        self.to = to


class PlatformMetadata(SimpleObject):
    def __init__(self, code=None):
        self.code = code


class InstrumentMetadata(SimpleObject):
    def __init__(self, name=None, type_=None, operation_mode=None):
        """
        :type name: str
        :type type_: str
        :type operation_mode: str
        """
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


class Polygon(SimpleObject):
    def __init__(self, ul, ur, ll, lr):
        self.ul = ul
        self.ur = ur
        self.ll = ll
        self.lr = lr


class CoordPolygon(Polygon):
    PROPERTY_PARSERS = {
        'ul': Coord.from_dict,
        'ur': Coord.from_dict,
        'll': Coord.from_dict,
        'lr': Coord.from_dict
    }


class PointPolygon(Polygon):
    PROPERTY_PARSERS = {
        'ul': Point.from_dict,
        'ur': Point.from_dict,
        'll': Point.from_dict,
        'lr': Point.from_dict
    }


class ExtentMetadata(SimpleObject):
    """
    Standardised spatial and temporal information.

    This will use the same projection & datum across all datasets,
    so the coordinates given here can be indexed for easy comparison.

    (such as via a web service)

    Everything is WGS84 & GDA94
    """
    PROPERTY_PARSERS = {
        'coord': CoordPolygon.from_dict
    }

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

        # Eg. 'WGS84'
        self.reference_system = reference_system

        #: :type: CoordPolygon
        self.coord = coord

        #: :type: datetime.datetime
        self.from_dt = from_dt
        #: :type: datetime.datetime
        self.center_dt = center_dt
        #: :type: datetime.datetime
        self.to_dt = to_dt


class DimensionMetadata(SimpleObject):
    def __init__(self, name, resolution, size):
        self.name = name
        #: :type: float
        self.resolution = resolution
        #: :type: int
        self.size = size


def _lookup_alias(aliases, value):
    """
    Translate to a common name if our value is an alias.

    :type aliases: dict of (str, [str])
    :type value: str
    :rtype: str

    >>> _lookup_alias({'name1': ['alias1']}, 'name1')
    'name1'
    >>> _lookup_alias({'name1': ['alias1', 'alias2']}, 'alias1')
    'name1'
    >>> _lookup_alias({'name1': ['alias1', 'alias2']}, 'alias2')
    'name1'
    >>> _lookup_alias({'name1': ['alias1']}, 'name2')
    'name2'
    """
    better_name = [name for (name, aliases) in aliases.items()
                   if value in aliases]
    return better_name[0] if better_name else value


class ProjectionMetadata(SimpleObject):
    """
    The projection and datum information of the current image.
    """
    PROPERTY_PARSERS = {
        'centre_point': Point.from_dict,
        'geo_ref_points': PointPolygon.from_dict
    }

    _ELLIPSOID_ALIASES = {
        'WGS84': ['WGS 1984', 'EPSG:4326', '4326'],
        'GRS80': ['GRS 1980']
    }

    _POINT_IN_PIXEL_ALIASES = {
        'UL': ['Upper Left'],
        'UR': ['Upper Right'],
        'LL': ['Lower Left'],
        'LR': ['Lower Right']
    }

    def __init__(self,
                 centre_point=None,
                 geo_ref_points=None,
                 datum=None,
                 ellipsoid=None,
                 point_in_pixel=None,
                 map_projection=None,
                 orientation=None,
                 resampling_option=None,
                 zone=None,
                 unit=None):
        """

        :param centre_point:
        :param geo_ref_points:
        :param datum:
        :param ellipsoid:
        :param point_in_pixel:
        :param map_projection:
        :param orientation:
        :param resampling_option:
        :param zone:
        :param unit:
        :return:


        >>> ProjectionMetadata(ellipsoid='4326')
        ProjectionMetadata(ellipsoid='WGS84')
        >>> ProjectionMetadata(ellipsoid='WGS84')
        ProjectionMetadata(ellipsoid='WGS84')
        >>> ProjectionMetadata(ellipsoid='NAD83')
        ProjectionMetadata(ellipsoid='NAD83')
        """
        # The units of these points are dependent on the reference system.
        # Eg. 'GDA94' points are a distance in meters.

        #: :type: Point
        self.centre_point = centre_point

        #: :type: PointPolygon
        self.geo_ref_points = geo_ref_points

        # Eg. 'GDA94'
        #: :type: str
        self.datum = datum

        # Eg. 'GRS80'
        #: :type: str
        self.ellipsoid = _lookup_alias(self._ELLIPSOID_ALIASES, ellipsoid)

        # Eg. 'UL'
        #: :type: str
        self.point_in_pixel = _lookup_alias(self._POINT_IN_PIXEL_ALIASES, point_in_pixel)

        #: Eg. 'NUP' (North up)
        #: :type: str
        self.orientation = orientation

        # Eg. 'UTM'
        #: :type: :str
        self.map_projection = map_projection

        # Eg. 'CUBIC_CONVOLUTION'
        #: :type: str
        self.resampling_option = resampling_option

        # Eg. -53
        #: :type: int
        self.zone = zone

        # Eg. 'metre'
        #: :type: str
        self.unit = unit


class GridSpatialMetadata(SimpleObject):
    PROPERTY_PARSERS = {
        'dimensions': DimensionMetadata.from_dicts,
        'projection': ProjectionMetadata.from_dict
    }

    def __init__(self, dimensions=None, projection=None):
        """
        :type dimensions: list of DimensionMetadata
        :type projection: ProjectionMetadata
        """
        # TODO: We don't have a single set of dimensions? Per band?
        self.dimensions = dimensions

        self.projection = projection


class BrowseMetadata(SimpleObject):
    PROPERTY_PARSERS = {
        'path': Path,
        'shape': Point.from_dict,
    }

    def __init__(self, path=None, file_type=None, cell_size=None,
                 shape=None,
                 red_band=None, green_band=None, blue_band=None):
        #: :type: pathlib.Path
        self.path = path

        # eg 'image/jpeg'
        self.file_type = file_type

        # Size in metres
        self.cell_size = cell_size

        # x/y size (or constraints). Either x or y may be None (if unconstrained/unknown).
        #: :type: Point
        self.shape = shape

        # There are band numbers: They should correspond to a band key in the band list. (eg. '7' or '6_vcid_1')
        self.red_band = red_band
        self.green_band = green_band
        self.blue_band = blue_band


class BandMetadata(SimpleObject):
    PROPERTY_PARSERS = {
        'path': Path,
        'shape': Point.from_dict
    }

    def __init__(self, path=None, file_offset=None, type_=None, label=None, number=None, shape=None, cell_size=None):
        # The file path of the band.
        #
        # Try to use absolute paths. We translate all 'Path' objects to relative paths
        # during serialisation (relative to whatever we want).
        #: :type: pathlib.Path
        self.path = path

        # The offset of this band within the file.
        #
        # This is useful when multiple bands are stored in one file.
        #
        # This is dependent on file type.
        #   - GeoTIFF: A number (starting from 1, as in gdal)
        #   - HDF5: The item key (eg. 'subgroup/datasetname')
        self.file_offset = file_offset

        # Eg. 'thermal'
        self.type_ = type_

        # Eg. 'visible_red'
        self.label = label

        # Satellite band number. Not always a number (eg. 'qa' in landsat 7, or '6_vcid_1' in landsat 7)
        self.number = number

        # Width/height
        #: :type: Point
        self.shape = shape

        # Size in metres
        self.cell_size = cell_size


class ImageMetadata(SimpleObject):
    PROPERTY_PARSERS = {
        'satellite_ref_point_start': Point.from_dict,
        'satellite_ref_point_end': Point.from_dict,
        'bands': BandMetadata.from_named_dicts,
    }

    def __init__(self,
                 satellite_ref_point_start=None,
                 satellite_ref_point_end=None,
                 cloud_cover_percentage=None,
                 cloud_cover_details=None,
                 sun_azimuth=None,
                 sun_elevation=None,
                 sun_earth_distance=None,
                 ground_control_points_version=None,
                 ground_control_points_model=None,
                 geometric_rmse_model=None,
                 geometric_rmse_model_x=None,
                 geometric_rmse_model_y=None,
                 viewing_incidence_angle_long_track=None,
                 viewing_incidence_angle_x_track=None,
                 bands=None):
        # Typically path/row for Landsat:
        #: :type: Point
        self.satellite_ref_point_start = satellite_ref_point_start
        #: :type: Point
        self.satellite_ref_point_end = satellite_ref_point_end

        self.cloud_cover_percentage = cloud_cover_percentage
        self.cloud_cover_details = cloud_cover_details
        self.sun_azimuth = sun_azimuth
        self.sun_elevation = sun_elevation
        self.sun_earth_distance = sun_earth_distance

        self.ground_control_points_version = ground_control_points_version
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


class MachineMetadata(SimpleObject):
    PROPERTY_PARSERS = {
        'runtime_id': uuid.UUID
    }

    def __init__(self, hostname=None, runtime_id=None, type_id=None, version=None, software=None, uname=None):
        # Hostname the dataset was processed on.
        self.hostname = hostname
        # A uuid generated each time a program runs.
        # Allows for distinguishing between processss on the same machine.
        #: :type: uuid.UUID
        self.runtime_id = runtime_id
        # Informal machine type/class. Often will correspond to the Puppet script used to create the machine.
        #: :type: str
        self.type_id = type_id
        # Version of machine type/class (eg. puppet file VCS version)
        self.version = version

        # Software provenance on the machine at time of processing.
        #: :type: eodatasets.provenance.SoftwareProvenance
        self.software = software

        # Uname information of machine.
        #    - from python: ' '.join(os.uname()))
        #    - from shell:  uname -a
        #: :type: str
        self.uname = uname


class AncillaryMetadata(SimpleObject):
    def __init__(self, type_=None, name=None, uri=None):
        self.type_ = type_
        self.name = name
        self.uri = uri


class LineageMetadata(SimpleObject):
    PROPERTY_PARSERS = {
        'algorithm': AlgorithmMetadata.from_dict,
        'machine': MachineMetadata.from_dict,
        'ancillary': AncillaryMetadata.from_named_dicts
    }

    def __init__(self, algorithm=None, machine=None, ancillary_quality=None, ancillary=None, source_datasets=None):
        #: :type: AlgorithmMetadata
        self.algorithm = algorithm

        #: :type: MachineMetadata
        self.machine = machine

        # 'PREDICTIVE' or 'DEFINITIVE'
        # :type: str
        self.ancillary_quality = ancillary_quality

        #: :type: dict of (str, AncillaryMetadata)
        self.ancillary = ancillary

        #: :type: dict of (str, DatasetMetadata)
        self.source_datasets = source_datasets


class GroundstationMetadata(SimpleObject):
    PROPERTY_PARSERS = {
        'antenna_coord': Coord.from_dict
    }

    def __init__(self, code, label=None, eods_domain_code=None, antenna_coord=None):
        """

        :param code: The GSI of the groundstation ("ASA" etc)
        :type code: str
        :type antenna_coord: Coord
        :type label: str
        :return:
        """
        self.code = code
        # A common name. Eg. 'Alice Springs'
        self.label = label
        # Coordinates of receiving antenna (at time of acquisition).
        self.antenna_coord = antenna_coord

        # Domain code for this groundstation. As seen in EODS and GA dataset_id.
        # Eg. '002' for Alice.
        #: :type: str
        self.eods_domain_code = eods_domain_code


class AcquisitionMetadata(SimpleObject):
    PROPERTY_PARSERS = {
        'groundstation': GroundstationMetadata.from_dict
    }

    def __init__(self, aos=None, los=None, groundstation=None, heading=None, platform_orbit=None):
        """
        :type groundstation: GroundstationMetadata
        :type platform_orbit: int
        :type aos: datetime.datetime
        :type los: datetime.datetime
        """

        # Acquisition/Loss Of signal
        self.aos = aos
        self.los = los

        self.groundstation = groundstation

        self.heading = heading
        self.platform_orbit = platform_orbit


class DatasetMetadata(SimpleObject):
    PROPERTY_PARSERS = {
        'id_': uuid.UUID,
        'platform': PlatformMetadata.from_dict,
        'instrument': InstrumentMetadata.from_dict,
        'format_': FormatMetadata.from_dict,
        'acquisition': AcquisitionMetadata.from_dict,
        'extent': ExtentMetadata.from_dict,
        'grid_spatial': GridSpatialMetadata.from_dict,
        'browse': BrowseMetadata.from_named_dicts,
        'image': ImageMetadata.from_dict,
        'lineage': LineageMetadata.from_dict
    }

    def __init__(self, id_=None,
                 ga_label=None,
                 ga_level=None,
                 usgs_dataset_id=None,
                 product_type=None,
                 product_level=None,
                 creation_dt=None,
                 size_bytes=None,
                 checksum_path=None,
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

        # When the dataset was created.
        #: :type: datetime.datetime
        self.creation_dt = creation_dt

        #: :type: int
        self.size_bytes = size_bytes

        # Formally known as 'dataset_id' within GA.
        # Eg. 'LS7_ETM_SYS_P31_GALPGS01-002_114_73_20050107'
        #: :type: str
        self.ga_label = ga_label

        # Eg. 'P00', 'P51' etc.
        #: :type: str
        self.ga_level = ga_level

        # 'L1T', 'L1G', 'L1GT' etc if applicable.
        # :type: str
        self.product_level = product_level

        # Currently correspond to driver ids:
        # 'raw', 'ortho', 'nbar_terrain' etc.
        #: :type: str
        self.product_type = product_type

        # Dataset id used by USGS software.
        # Eg. 'LC81160740742015089ASA00'
        #: :type: str
        self.usgs_dataset_id = usgs_dataset_id

        #: :type: pathlib.Path
        self.checksum_path = checksum_path

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


def rebase_path(from_root_path, to_root_path, path):
    """
    Rebase the path to a new root path.
    :type path: pathlib.Path
    :type from_root_path: pathlib.Path
    :type to_root_path: pathlib.Path
    :rtype: pathlib.Path
    >>> source = Path('/tmp/from/something')
    >>> dest = Path('/tmp/to')
    >>> rebase_path(source, dest, Path('/tmp/from/something/test.txt'))
    PosixPath('/tmp/to/test.txt')
    >>> rebase_path(source, dest, Path('/tmp/other.txt'))
    PosixPath('/tmp/other.txt')
    >>> rebase_path(source, dest, Path('other.txt'))
    PosixPath('other.txt')
    >>> dest = Path('/tmp/to/something/else')
    >>> rebase_path(source, dest, Path('/tmp/from/something/test.txt'))
    PosixPath('/tmp/to/something/else/test.txt')
    """
    if from_root_path not in path.absolute().parents:
        return path

    relative_path = path.absolute().relative_to(from_root_path)
    return to_root_path.joinpath(relative_path)


def map_values(f, o, skip_nones=False):
    """
    Apply a function to all values recursively.

    Recurses through container types (dicts, lists, tuples and and SimpleObjects).

    Returns a new instance (deep copy) without modifying the original.

    >>> # More complex tests (for objects etc) are in test_type.py
    >>> map_values(lambda a: a+1, [1, 2, 3])
    [2, 3, 4]
    >>> map_values(lambda a: a+1, {'a': 1, 'b': 2, 'c': 3}) == {'a': 2, 'b': 3, 'c': 4}
    True
    >>> map_values(lambda a: a+1, {'a': 1, 'b': 2, 'c': 3}) == {'a': 2, 'b': 3, 'c': 4}
    True
    """

    def recur(v):
        return map_values(f, v, skip_nones=skip_nones)

    # If is a container type, run recursively on values.
    if isinstance(o, dict):
        return {k: recur(v) for k, v in o.items()}
    if isinstance(o, list):
        return [recur(v) for v in o]
    if isinstance(o, SimpleObject):
        out = {}
        for name, value in o.items_ordered(skip_nones=False):
            out[name] = recur(value)

        return o.__class__(**out)
    if isinstance(o, tuple):
        return tuple(recur(v) for v in o)

    if o is None and skip_nones:
        return None

    # Otherwise map the value.
    return f(o)


def rebase_paths(source_path, destination_path, object_):
    """
    Rebase all paths in a given object structure (list, dict, SimpleObject) from
    one root path to another.

    :type source_path: Path
    :type destination_path: Path
    """
    def rebase_if_path(o):
        if isinstance(o, Path):
            return rebase_path(source_path, destination_path, o)
        return o

    return map_values(rebase_if_path, object_)

# Circular reference.
LineageMetadata.PROPERTY_PARSERS['source_datasets'] = DatasetMetadata.from_named_dicts
