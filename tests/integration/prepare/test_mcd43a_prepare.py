import datetime
from pathlib import Path
from pprint import pprint

from eodatasets3.prepare.nasa_c_m_mcd43a1_6_prepare import parse_xml

PRE_24_XML = Path(__file__).parent / "MCD43A1.A2024071.h16v14.061.2024085194442.hdf.xml"
POST_24_XML = (
    Path(__file__).parent / "MCD43A1.A2024070.h19v16.061.2024079033215.hdf.xml"
)


def test_parse_pre24_xml():
    result = parse_xml(POST_24_XML)

    pprint(result)
    assert result == {
        "collection_version": "61",
        "granule_id": "MCD43A1.A2024070.h19v16.061.2024079033215.hdf",
        "instrument": "MODIS",
        "platform": "Terra+Aqua",
        "horizontal_tile": 19,
        "vertical_tile": 16,
        "from_dt": datetime.datetime(2024, 3, 2, 0, 0, tzinfo=datetime.timezone.utc),
        "to_dt": datetime.datetime(
            2024, 3, 17, 23, 59, 59, 999999, tzinfo=datetime.timezone.utc
        ),
        "creation_dt": datetime.datetime(
            2024, 3, 18, 22, 52, 25, 228000, tzinfo=datetime.timezone.utc
        ),
    }


def test_parse_post24_xml():
    result = parse_xml(PRE_24_XML)
    pprint(result)
    assert result == {
        "collection_version": "61",
        "granule_id": "MCD43A1.A2024071.h16v14.061.2024085194442.hdf",
        "instrument": "MODIS",
        "platform": "Terra+Aqua",
        "horizontal_tile": 16,
        "vertical_tile": 14,
        "from_dt": datetime.datetime(2024, 3, 11, 0, 0, tzinfo=datetime.timezone.utc),
        "to_dt": datetime.datetime(
            2024, 3, 18, 23, 59, 59, 999000, tzinfo=datetime.timezone.utc
        ),
        "creation_dt": datetime.datetime(
            2024, 3, 25, 14, 55, 23, 768000, tzinfo=datetime.timezone.utc
        ),
    }
