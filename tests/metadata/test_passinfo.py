# coding=utf-8
from __future__ import absolute_import

import datetime
import unittest

from eodatasets import type as ptype
from eodatasets.metadata import passinfo as extraction
from tests import write_files


class TestPassinfo(unittest.TestCase):
    def test_parse_passinfo(self):
        pm = ptype.DatasetMetadata()

        lines = [
            "STATION ALICE\n",
            "STRING  S1A1C1D1R1\n",
            "PASSID  LANDSAT-5.110912\n",
            "SATELLITE   LANDSAT-5\n",
            "ORBIT   110912\n",
            "LINKID  X\n",
            "BITRATE 84900000\n",
            "SENSOR  TM\n",
            "START   2005-01-06T23:32:14\n",
            "STOP    2005-01-06T23:39:12\n",
            "DURATION    423\n",
            "COMMENTS    Bit\n",
            "logfile acs acs.log\n",
            "logfile ref ref.log\n",
            "logfile demod   demod.log\n",
            "logfile 1050110912.eph  1050110912.eph.log\n",
            "telemetry   telemetry.data\n"
        ]
        md = extraction._parse_passinfo_md(pm, lines)

        self.assertEqual(md.acquisition.groundstation.code, "ASA")
        self.assertEqual(md.acquisition.platform_orbit, 110912)
        self.assertEqual(md.platform.code, "LANDSAT_5")
        self.assertEqual(md.instrument.name, "TM")
        self.assertEqual(md.acquisition.aos, datetime.datetime(2005, 1, 6, 23, 32, 14))
        self.assertEqual(md.acquisition.los, datetime.datetime(2005, 1, 6, 23, 39, 12))

    def test_parse_passinfo_file(self):
        d = write_files({
            'subdirectory': {

            },
            'passinfo': [
                "STATION ALICE\n",
                "STRING  S1A1C1D1R1\n",
                "PASSID  LANDSAT-5.110912\n",
                "SATELLITE   LANDSAT-5\n",
                "ORBIT   110912\n",
                "LINKID  X\n",
                "BITRATE 84900000\n",
                "SENSOR  TM\n",
                "START   2005-01-06T23:32:14\n",
                "STOP    2005-01-06T23:39:12\n",
                "DURATION    423\n",
                "COMMENTS    Bit\n",
                "logfile acs acs.log\n",
                "logfile ref ref.log\n",
                "logfile demod   demod.log\n",
                "logfile 1050110912.eph  1050110912.eph.log\n",
                "telemetry   telemetry.data\n"
            ]})
        # It should find a passinfo file one directory up.
        md = extraction.extract_md(ptype.DatasetMetadata(), d.joinpath('subdirectory'))

        self.assertEqual(md.acquisition.groundstation.code, "ASA")
        self.assertEqual(md.acquisition.platform_orbit, 110912)
        self.assertEqual(md.platform.code, "LANDSAT_5")
        self.assertEqual(md.instrument.name, "TM")
        self.assertEqual(md.acquisition.aos, datetime.datetime(2005, 1, 6, 23, 32, 14))
        self.assertEqual(md.acquisition.los, datetime.datetime(2005, 1, 6, 23, 39, 12))

    def test_parse_passinfo_ls7_file(self):
        d = write_files({'passinfo': [
            "STATION	ALICE\n",
            "STRING	S1A1C2D3R3\n",
            "PASSID	LANDSAT-7.30486\n",
            "SATELLITE	LANDSAT-7\n",
            "ORBIT	30486\n",
            "LINKID	L\n",
            "BITRATE	150000000\n",
            "SENSOR	ETM\n",
            "START	2005-01-07T02:00:28.000\n",
            "STOP	2005-01-07T02:07:19.000\n",
            "DURATION	528\n",
            "COMMENTS	Bit\n",
            "logfile	acs	acs.log\n",
            "logfile	ref	ref.log\n",
            "logfile	1070030486.eph	1070030486.eph.log\n",
            "logfile	demod	demod.log\n",
            "telemetry	telemetry.data\n",
        ]})
        md = extraction.extract_md(ptype.DatasetMetadata(), d)

        self.assertEqual(md.acquisition.groundstation.code, "ASA")
        self.assertEqual(md.acquisition.platform_orbit, 30486)
        self.assertEqual(md.platform.code, "LANDSAT_7")
        self.assertEqual(md.instrument.name, "ETM")
        self.assertEqual(md.acquisition.aos, datetime.datetime(2005, 1, 7, 2, 0, 28))
        self.assertEqual(md.acquisition.los, datetime.datetime(2005, 1, 7, 2, 7, 19))

    def test_unusual_filenames(self):
        # Some passinfo filenames have orbit numbers appended

        d = write_files({'passinfo.24775': [
            "STATION TERSS\n",
            "STRING  S1A1C1D1R1\n",
            "PASSID  LANDSAT-5.110912\n",
            "SATELLITE   LANDSAT-5\n",
            "ORBIT   110912\n",
            "LINKID  X\n",
            "BITRATE 84900000\n",
            "SENSOR  TM\n",
            "START   2005-01-06T23:32:14\n",
            "STOP    2005-01-06T23:39:12\n",
            "DURATION    423\n",
            "COMMENTS    Bit\n",
            "logfile acs acs.log\n",
            "logfile ref ref.log\n",
            "logfile demod   demod.log\n",
            "logfile 1050110912.eph  1050110912.eph.log\n",
            "telemetry   telemetry.data\n"
        ]})
        md = extraction.extract_md(ptype.DatasetMetadata(), d)

        # Station "TERSS" is hobart.
        self.assertEqual(md.acquisition.groundstation.code, "HOA")
        self.assertEqual(md.acquisition.platform_orbit, 110912)
        self.assertEqual(md.platform.code, "LANDSAT_5")
        self.assertEqual(md.instrument.name, "TM")
        self.assertEqual(md.acquisition.aos, datetime.datetime(2005, 1, 6, 23, 32, 14))
        self.assertEqual(md.acquisition.los, datetime.datetime(2005, 1, 6, 23, 39, 12))


if __name__ == '__main__':
    unittest.main()
