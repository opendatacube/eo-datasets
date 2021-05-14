from eodatasets3.model import DatasetDoc


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
