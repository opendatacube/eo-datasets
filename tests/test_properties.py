import datetime
import warnings
from contextlib import contextmanager

import pytest

from eodatasets3 import Eo3Dict, namer
from eodatasets3.model import DatasetDoc
from eodatasets3.names import NamingConventions
from eodatasets3.properties import PropertyOverrideWarning


def test_multi_platform_fields():
    """
    Multiple platforms can be specified.

    (they are normalised in eo3 as a sorted, comma-separated list)
    """
    d = DatasetDoc()
    assert d.platform is None
    assert d.platforms == set()

    d.platforms = {"LANDSAT_5", "LANDSAT_4"}
    assert d.platform == "landsat-4,landsat-5"
    assert d.platforms == {"landsat-4", "landsat-5"}

    d = DatasetDoc()
    d.platform = "sentinel-2a, landsat_5, LANDSAT_5"
    assert d.platform == "landsat-5,sentinel-2a"
    assert d.platforms == {"landsat-5", "sentinel-2a"}

    d = DatasetDoc()
    d.platform = ""
    assert d.platform is None


@contextmanager
def ignore_property_overrides():
    """Don't warn about setting a property twice."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", PropertyOverrideWarning)
        yield


def test_naming_abbreviations():
    d = DatasetDoc()
    names = NamingConventions(d.properties)

    with ignore_property_overrides():
        assert names.platform_abbreviated is None

        # A single platform uses its known abbreviation.
        d.platforms = ["landsat-5"]
        assert names.platform_abbreviated == "ls5"

        # Multiple platforms from a known group use the group name.
        d.platforms = ["landsat-5", "landsat_7"]
        assert names.platform_abbreviated == "ls"
        d.platforms = ["sentinel-2a", "sentinel-2b"]
        assert names.platform_abbreviated == "s2"

        # Non-groupable platforms are dash-separated.
        d.platforms = ["landsat-5", "sentinel-2a"]
        assert names.platform_abbreviated is None


def test_unknown_abbreviations():
    d = DatasetDoc()
    names = NamingConventions(d.properties)

    with ignore_property_overrides():
        # Unknown platforms are abbreviated by just removing dashes.
        d.platform = "grover-1"
        assert names.platform_abbreviated == "grover1"

        # Constellation can be used as a fallback grouping.
        d.platforms = ["clippings-1a", "clippings-2b"]
        d.properties["constellation"] = "clippings"
        assert names.platform_abbreviated == "clippings"

        # Unless unknown platforms aren't allowed
        # (DEA wants to be stricter and add real abbreviations for everything.)
        names = namer(d.properties, conventions="dea")
        with pytest.raises(
            ValueError, match="don't know the DEA abbreviation for platform"
        ):
            print(names.platform_abbreviated)


def test_normalise_input_fields():
    """
    When properties are constructed manually, they should be normalised, including
    sometimes the generation of new, parsed properties, without failing.

    (this matches usage by Alchemist)
    """
    # Manually constructed properties that will need normalisation.
    # (this is what Alechmist does)
    properties = Eo3Dict(
        {
            "odc:region_code": "234343",
            "datetime": datetime.datetime(2014, 3, 4),
            "eo:instrument": "MSI",
            # Needs normalisation
            "eo:platform": "SENTINEL-2",
            "sentinel:sentinel_tile_id": "S2A_OPER_MSI_L1C_TL_EPAE_20201031T022859_A027984_T53JQJ_N02.09",
            # Alchemist includes this field with invalid value "??" for Sentinel.
            "landsat:landsat_scene_id": "??",
        }
    )

    assert dict(properties) == {
        "datetime": datetime.datetime(2014, 3, 4, 0, 0, tzinfo=datetime.timezone.utc),
        "eo:instrument": "MSI",
        # Was normalised.
        "eo:platform": "sentinel-2",
        "landsat:landsat_scene_id": "??",
        "odc:region_code": "234343",
        # Was read from the tile id.
        "sentinel:datatake_start_datetime": datetime.datetime(
            2020, 10, 31, 2, 28, 59, tzinfo=datetime.timezone.utc
        ),
        "sentinel:sentinel_tile_id": "S2A_OPER_MSI_L1C_TL_EPAE_20201031T022859_A027984_T53JQJ_N02.09",
    }
