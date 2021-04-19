from pathlib import Path
from eodatasets3.wagl import _get_level1_metadata_path

# data/LC08_L1TP_090084_20160121_20200907_02_T1/LC08_L1TP_090084_20160121_20200907_02_T1.odc-metadata.yaml

# The matching Level1 metadata (produced by landsat_l1_prepare.py)
L1_C2_METADATA_DIR: Path = (
    Path(__file__).parent / "data/LC08_L1TP_090084_20160121_20200907_02_T1"
)
L1_C2_METADATA_DIR_YAML: Path = (
    Path(__file__).parent
    / "data/LC08_L1TP_090084_20160121_20200907_02_T1/"
    / "LC08_L1TP_090084_20160121_20200907_02_T1.odc-metadata.yaml"
)

L1_C2_METADATA_TAR: Path = (
    Path(__file__).parent / "data/LE07_L1TP_104078_20130429_20161124_01_T1.tar"
)

L1_C2_METADATA_TAR_YAML: Path = (
    Path(__file__).parent
    / "data/LE07_L1TP_104078_20130429_20161124_01_T1.odc-metadata.yaml"
)


def test_get_level1_metadata_path_yaml_in_dir():
    wagl_doc = {"source_datasets": {"source_level1": L1_C2_METADATA_DIR}}
    result = Path(_get_level1_metadata_path(wagl_doc))
    assert result.name == "LC08_L1TP_090084_20160121_20200907_02_T1.odc-metadata.yaml"
    assert result == L1_C2_METADATA_DIR_YAML


def test_get_level1_metadata_path_yaml_alongside_tar():
    wagl_doc = {"source_datasets": {"source_level1": L1_C2_METADATA_TAR}}
    result = Path(_get_level1_metadata_path(wagl_doc))
    assert result == L1_C2_METADATA_TAR_YAML


def test_get_level1_metadata_no_source():
    wagl_doc = {"source_datasets": {"source_level1": "/no/where/good"}}
    assert _get_level1_metadata_path(wagl_doc) is None
