# coding=utf-8
"""
Module
"""
from __future__ import absolute_import
import datetime

import mock

from pathlib import Path

from tests import write_files, assert_same
import eodatasets.metadata.pds as pds
import eodatasets.type as ptype


def test_find_aqua_pds_file():
    d = write_files({
        'P1540064AAAAAAAAAAAAAA14202165651000.PDS': '',
        'P1540064AAAAAAAAAAAAAA14202165651001.PDS': '',
        'P1540141AAAAAAAAAAAAAA14202165651000.PDS': '',
        'P1540141AAAAAAAAAAAAAA14202165651001.PDS': '',
        'P1540157AAAAAAAAAAAAAA14202165651000.PDS': '',
        'P1540157AAAAAAAAAAAAAA14202165651001.PDS': '',
        'P1540261AAAAAAAAAAAAAA14202165651000.PDS': '',
        'P1540261AAAAAAAAAAAAAA14202165651001.PDS': '',
        'P1540262AAAAAAAAAAAAAA14202165651000.PDS': '',
        'P1540262AAAAAAAAAAAAAA14202165651001.PDS': '',
        'P1540290AAAAAAAAAAAAAA14202165651000.PDS': '',
        'P1540290AAAAAAAAAAAAAA14202165651001.PDS': '',
        'P1540342AAAAAAAAAAAAAA14202165651000.PDS': '',
        'P1540342AAAAAAAAAAAAAA14202165651001.PDS': '',
        'P1540402AAAAAAAAAAAAAA14202165651000.PDS': '',
        'P1540402AAAAAAAAAAAAAA14202165651001.PDS': '',
        'P1540404AAAAAAAAAAAAAA14202165651000.PDS': '',
        'P1540404AAAAAAAAAAAAAA14202165651001.PDS': '',
        'P1540405AAAAAAAAAAAAAA14202165651000.PDS': '',
        'P1540405AAAAAAAAAAAAAA14202165651001.PDS': '',
        'P1540406AAAAAAAAAAAAAA14202165651000.PDS': '',
        'P1540406AAAAAAAAAAAAAA14202165651001.PDS': '',
        'P1540407AAAAAAAAAAAAAA14202165651000.PDS': '',
        'P1540407AAAAAAAAAAAAAA14202165651001.PDS': '',
        'P1540414AAAAAAAAAAAAAA14202165651000.PDS': '',
        'P1540414AAAAAAAAAAAAAA14202165651001.PDS': '',
        'P1540415AAAAAAAAAAAAAA14202165651000.PDS': '',
        'P1540957AAAAAAAAAAAAAA14202165651000.PDS': '',
        'P1540957AAAAAAAAAAAAAA14202165651001.PDS': '',
        'P1540415AAAAAAAAAAAAAA14202165651001.PDS': ''
    })

    found = pds.find_pds_file(d)
    # It should find the '0064' APID with '01' suffix.
    expected = d.joinpath('P1540064AAAAAAAAAAAAAA14202165651001.PDS')
    assert expected == found


def test_find_terra_pds_file():
    d = write_files({
        'P0420064AAAAAAAAAAAAAA14202013839000.PDS': '',
        'P0420064AAAAAAAAAAAAAA14202013839001.PDS': '',
    })

    found = pds.find_pds_file(d)
    # It should find the '0064' APID with '01' suffix.
    expected = d.joinpath('P0420064AAAAAAAAAAAAAA14202013839001.PDS')
    assert expected == found


@mock.patch('eodatasets.metadata.pds._run_pdsinfo_exe')
def test_get_pdsinfo(_run_pdsinfo_exe):
    pdsfile = Path('/tmp/pds')

    _run_pdsinfo_exe.return_value = """APID 64: count 610338 invalid 0 missing 6255
first packet: 2014/08/07 03:16:28.750910
last packet: 2014/08/07 03:21:28.604695
missing seconds: 2
day packets: 545223/64311
night packets: 0/0
engineering packets: 804/0
"""

    start, end, day, night = pds.get_pdsinfo(pdsfile)

    assert start == datetime.datetime(2014, 8, 7, 3, 16, 28, 750910)
    assert end == datetime.datetime(2014, 8, 7, 3, 21, 28, 604695)
    assert day == 545223
    assert night == 0


@mock.patch('eodatasets.metadata.pds._run_pdsinfo_exe')
def test_extract_md(_run_pdsinfo_exe):
    input_dir = write_files({'P1540064AAAAAAAAAAAAAA14219032341001.PDS': ''})

    # def run_pdsinfo(file_):
    #     assert file_ == input_dir
    #
    #     return

    _run_pdsinfo_exe.return_value = """APID 64: count 610338 invalid 0 missing 6255
    first packet: 2014/08/07 03:16:28.750910
    last packet: 2014/08/07 03:21:28.604695
    missing seconds: 2
    day packets: 545223/64311
    night packets: 0/0
    engineering packets: 804/0
    """

    md = pds.extract_md(ptype.DatasetMetadata(), input_dir)

    expected = ptype.DatasetMetadata(
        platform=ptype.PlatformMetadata(code='AQUA'),
        instrument=ptype.InstrumentMetadata(name='MODIS'),
        acquisition=ptype.AcquisitionMetadata(
            aos=datetime.datetime(2014, 8, 7, 3, 16, 28, 750910),
            los=datetime.datetime(2014, 8, 7, 3, 21, 28, 604695)
        ),
        image=ptype.ImageMetadata(
            day_percentage_estimate=100.0
        )
    )

    md.id_, expected.id_ = None, None

    assert_same(expected, md)
