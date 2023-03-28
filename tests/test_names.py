import pytest

from eodatasets3.names import LazyPlatformAbbreviation

def test_LazyPlatformAbbreviation():
    p = DatasetAssembler(tmp_path, naming_conventions="dea_c3")
    p.platform = "landsat-7"
    p.datetime = datetime(1998, 7, 30)
    p.product_family = "wo"
    p.processed = "1998-07-30T12:23:23"
    p.maturity = "interim"
    p.producer = "ga.gov.au"
    p.region_code = "090081"

    assert LazyPlatformAbbreviation() ==
