from pathlib import Path
from uuid import UUID

import pytest

from eodatasets3.wagl import _load_level1_doc

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
    doc = _load_level1_doc(wagl_doc)
    assert doc.id == UUID("d9221c40-24c3-5356-ab22-4dcac2bf2d70")


def test_get_level1_metadata_path_yaml_alongside_tar():
    wagl_doc = {"source_datasets": {"source_level1": L1_C2_METADATA_TAR}}
    doc = _load_level1_doc(wagl_doc)
    assert doc.id == UUID("f23c5fa2-3321-5be9-9872-2be73fee12a6")


def test_get_level1_metadata_no_source():
    # Complain when the embedded level1 reference doesn't exist.
    wagl_doc = {"source_datasets": {"source_level1": "/no/where/good"}}
    with pytest.raises(
        ValueError,
        match="No level1 found or provided. WAGL said it was at path '/no/where/good'*",
    ):
        _load_level1_doc(wagl_doc)

    # .... unless we allow missing provenance.
    doc = _load_level1_doc(wagl_doc, allow_missing_provenance=True)
    assert doc is None
