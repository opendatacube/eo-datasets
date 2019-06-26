import collections.abc
import warnings
from collections import defaultdict
from datetime import datetime
from typing import Tuple, Dict, Optional, Any, Mapping, Callable, Union

import ciso8601

from eodatasets2.utils import default_utc


def nest_properties(d: Mapping[str, Any], separator=":") -> Dict[str, Any]:
    """
    Split keys with embedded colons into sub dictionaries.

    Intended for stac-like properties

    >>> nest_properties({'landsat:path':1, 'landsat:row':2, 'clouds':3})
    {'landsat': {'path': 1, 'row': 2}, 'clouds': 3}
    """
    out = defaultdict(dict)
    for key, val in d.items():
        section, *remainder = key.split(separator, 1)
        if remainder:
            [sub_key] = remainder
            out[section][sub_key] = val
        else:
            out[section] = val

    for key, val in out.items():
        if isinstance(val, dict):
            out[key] = nest_properties(val, separator=separator)

    return dict(out)


def datetime_type(value):
    if isinstance(value, str):
        value = ciso8601.parse_datetime(value)

    # Store all dates with a timezone.
    # yaml standard says all dates default to UTC.
    # (and ruamel normalises timezones to UTC itself)
    if isinstance(value, datetime):
        value = default_utc(value)

    return value


def of_enum_type(vals: Tuple[str, ...], lower=False, upper=False, strict=True):
    def normalise(v: str):
        if upper:
            v = v.upper()
        if lower:
            v = v.lower()

        if v not in vals:
            msg = f"Unexpected value {v!r}. Expected one of: {', '.join(vals)},"
            if strict:
                raise ValueError(msg)
            else:
                warnings.warn(msg)
        return v

    return normalise


def percent_type(value):
    value = float(value)

    if not (0.0 <= value <= 100.0):
        raise ValueError("Expected percent between 0,100")
    return value


def normalise_platform(s: str):
    """
    >>> normalise_platform('LANDSAT_8')
    'landsat-8'
    """
    return s.lower().replace("_", "-")


def degrees_type(value):
    value = float(value)

    if not (-360.0 <= value <= 360.0):
        raise ValueError("Expected percent between 0,100")

    return value


def producer_check(value):
    if "." not in value:
        warnings.warn(
            "Property 'odc:producer' is expected to be a domain name, "
            "eg 'usgs.gov' or 'ga.gov.au'"
        )
    return value


# The primitive types allowed as stac values.
PrimitiveType = Union[str, int, float, datetime]
# A function to normalise a value.
# (eg. convert to int, or make string lowercase).
# They throw a ValueError if not valid.
NormaliseValueFn = Callable[[Any], PrimitiveType]


class StacPropertyView(collections.abc.Mapping):
    # Every property we've seen or dealt with so far. Feel free to expand with abandon...
    # This is to minimise minor typos, case differences, etc, which plagued previous systems.
    # Keep sorted.
    KNOWN_STAC_PROPERTIES: Mapping[str, Optional[NormaliseValueFn]] = {
        "datetime": datetime_type,
        "dea:dataset_maturity": of_enum_type(("final", "interim", "nrt"), lower=True),
        "dea:processing_level": None,
        "dtr:end_datetime": datetime_type,
        "dtr:start_datetime": datetime_type,
        "eo:azimuth": float,
        "eo:cloud_cover": percent_type,
        "eo:epsg": None,
        "eo:gsd": None,
        "eo:instrument": None,
        "eo:off_nadir": None,
        "eo:platform": normalise_platform,
        "eo:sun_azimuth": degrees_type,
        "eo:sun_elevation": degrees_type,
        "landsat:collection_category": None,
        "landsat:collection_number": int,
        "landsat:data_type": None,
        "landsat:earth_sun_distance": None,
        "landsat:ephemeris_type": None,
        "landsat:geometric_rmse_model": None,
        "landsat:geometric_rmse_model_x": None,
        "landsat:geometric_rmse_model_y": None,
        "landsat:geometric_rmse_verify": None,
        "landsat:ground_control_points_model": None,
        "landsat:ground_control_points_verify": None,
        "landsat:ground_control_points_version": None,
        "landsat:image_quality_oli": None,
        "landsat:image_quality_tirs": None,
        "landsat:landsat_product_id": None,
        "landsat:landsat_scene_id": None,
        "landsat:processing_software_version": None,
        "landsat:station_id": None,
        "landsat:wrs_path": int,
        "landsat:wrs_row": int,
        "odc:dataset_version": None,
        "odc:file_format": of_enum_type(("GeoTIFF", "NetCDF"), strict=False),
        "odc:processing_datetime": datetime_type,
        "odc:producer": producer_check,
        "odc:product_family": None,
        "odc:reference_code": None,
    }

    def __init__(self, properties=None) -> None:
        self._props = properties or {}

    def __getitem__(self, item):
        return self._props[item]

    def __iter__(self):
        return iter(self._props)

    def __len__(self):
        return len(self._props)

    def __setitem__(self, key, value):
        if key in self._props:
            warnings.warn(f"Overriding property {key!r}")

        if key not in self.KNOWN_STAC_PROPERTIES:
            warnings.warn(f"Unknown stac property {key!r}")

        if value is not None:
            normalise = self.KNOWN_STAC_PROPERTIES.get(key)
            if normalise:
                value = normalise(value)

        self._props[key] = value

    def nested(self):
        return nest_properties(self._props)

    # Convenient access fields for the most common/essential properties in datasets"""

    @property
    def properties(self):
        return self

    @property
    def platform(self) -> str:
        return self.properties["eo:platform"]

    @property
    def instrument(self) -> str:
        return self.properties["eo:instrument"]

    @property
    def producer(self) -> str:
        """
        Organisation that produced the data.

        eg. usgs.gov or ga.gov.au
        """
        return self.properties.get("odc:producer")

    @producer.setter
    def producer(self, domain: str):
        self.properties["odc:producer"] = domain

    @property
    def datetime_range(self):
        return (
            self.properties.get("dtr:start_datetime"),
            self.properties.get("dtr:end_datetime"),
        )

    @datetime_range.setter
    def datetime_range(self, val: Tuple[datetime, datetime]):
        # TODO: string type conversion, better validation/errors
        start, end = val
        self.properties["dtr:start_datetime"] = start
        self.properties["dtr:end_datetime"] = end

    @property
    def datetime(self) -> datetime:
        return self.properties.get("datetime")

    @datetime.setter
    def datetime(self, val: datetime) -> datetime:
        self.properties["datetime"] = val

    @property
    def processed(self) -> datetime:
        """
        When the dataset was processed (Default to UTC if not specified)
        """
        return self.properties.get("odc:processing_datetime")

    @processed.setter
    def processed(self, value):
        self.properties["odc:processing_datetime"] = value
