import datetime
import unittest

from eodatasets.drivers import RawDriver
from eodatasets.tests import write_files
import eodatasets.type as ptype


class TestDrivers(unittest.TestCase):
    def test_ls8_time_calc(self):
        d = write_files({
            'LANDSAT-8.11308': {
                'LC81160740742015089ASA00': {
                    '480.000.2015089022657325.ASA': '',
                    '481.000.2015089022653346.ASA': '',
                    'LC81160740742015089ASA00_IDF.xml': '',
                    'LC81160740742015089ASA00_MD5.txt': '',
                    'file.list': '',
                }
            }
        })

        metadata = RawDriver().fill_metadata(
            ptype.DatasetMetadata(),
            d.joinpath('LANDSAT-8.11308', 'LC81160740742015089ASA00')
        )

        self.assertEqual(metadata.platform.code, 'LANDSAT_8')
        self.assertEqual(metadata.instrument.name, 'OLI_TIRS')

        # TODO: Can we extract the operation mode?
        self.assertEqual(metadata.instrument.operation_mode, None)

        # Note that the files are not in expected order: when ordered by their first number (storage location), the
        # higher number is actually an earlier date.
        self.assertEqual(metadata.acquisition.aos, datetime.datetime(2015, 3, 30, 2, 25, 53, 346000))
        self.assertEqual(metadata.acquisition.los, datetime.datetime(2015, 3, 30, 2, 26, 57, 325000))