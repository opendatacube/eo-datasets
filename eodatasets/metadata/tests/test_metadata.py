# coding=utf-8
from __future__ import absolute_import
import unittest
import uuid
import datetime

from pathlib import PosixPath, Path
from eodatasets.tests import write_files
from eodatasets.tests import assert_same
from eodatasets.type import BandMetadata
import eodatasets.type as ptype
from eodatasets import metadata


BASIC_LS8_BANDS = {
    '11': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_B11.TIF'),
        number='11',
    ),
    '10': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_B10.TIF'),
        number='10',
    ),
    '1': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_B1.TIF'),
        number='1',
    ),
    '3': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_B3.TIF'),
        number='3',
    ),
    '2': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_B2.TIF'),
        number='2',
    ),
    '5': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_B5.TIF'),
        number='5',
    ),
    '4': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_B4.TIF'),
        number='4',
    ),
    '7': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_B7.TIF'),
        number='7',
    ),
    '6': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_B6.TIF'),
        number='6',
    ),
    '9': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_B9.TIF'),
        number='9',
    ),
    '8': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_B8.TIF'),
        number='8',
    ),
    'quality': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_BQA.TIF'),
        number='quality',
    )
}

EXPANDED_LS8_BANDS = {
    '11': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_B11.TIF'),
        type_=u'thermal',
        label=u'Thermal Infrared 2',
        number='11',
        cell_size=25.0
    ),
    '10': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_B10.TIF'),
        type_=u'thermal',
        label=u'Thermal Infrared 1',
        number='10',
        cell_size=25.0
    ),
    '1': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_B1.TIF'),
        type_=u'reflective',
        label=u'Coastal Aerosol',
        number='1',
        cell_size=25.0
    ),
    '3': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_B3.TIF'),
        type_=u'reflective',
        label=u'Visible Green',
        number='3',
        cell_size=25.0
    ),
    '2': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_B2.TIF'),
        type_=u'reflective',
        label=u'Visible Blue',
        number='2',
        cell_size=25.0
    ),
    '5': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_B5.TIF'),
        type_=u'reflective',
        label=u'Near Infrared',
        number='5',
        cell_size=25.0
    ),
    '4': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_B4.TIF'),
        type_=u'reflective',
        label=u'Visible Red',
        number='4',
        cell_size=25.0
    ),
    '7': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_B7.TIF'),
        type_=u'reflective',
        label=u'Short-wave Infrared 2',
        number='7',
        cell_size=25.0
    ),
    '6': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_B6.TIF'),
        type_=u'reflective',
        label=u'Short-wave Infrared 1',
        number='6',
        cell_size=25.0
    ),
    '9': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_B9.TIF'),
        type_=u'atmosphere',
        label=u'Cirrus',
        number='9',
        cell_size=25.0
    ),
    '8': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_B8.TIF'),
        type_=u'panchromatic',
        label=u'Panchromatic',
        number='8',
        cell_size=12.5
    ),
    'quality': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_BQA.TIF'),
        type_=u'quality',
        label=u'Quality',
        number='quality',
        cell_size=25.0
    )
}

BASIC_LS7_BANDS = {
    '1': BandMetadata(path=PosixPath('/tmp/fake-folder/LE71140732005007ASA00_B1.TIF'), number='1'),
    '3': BandMetadata(path=PosixPath('/tmp/fake-folder/LE71140732005007ASA00_B3.TIF'), number='3'),
    '2': BandMetadata(path=PosixPath('/tmp/fake-folder/LE71140732005007ASA00_B2.TIF'), number='2'),
    '5': BandMetadata(path=PosixPath('/tmp/fake-folder/LE71140732005007ASA00_B5.TIF'), number='5'),
    '4': BandMetadata(path=PosixPath('/tmp/fake-folder/LE71140732005007ASA00_B4.TIF'), number='4'),
    '7': BandMetadata(path=PosixPath('/tmp/fake-folder/LE71140732005007ASA00_B7.TIF'), number='7'),
    '8': BandMetadata(path=PosixPath('/tmp/fake-folder/LE71140732005007ASA00_B8.TIF'), number='8'),
    '6_vcid_2': BandMetadata(path=PosixPath('/tmp/fake-folder/LE71140732005007ASA00_B6_VCID_2.TIF'), number='6_vcid_2'),
    '6_vcid_1': BandMetadata(path=PosixPath('/tmp/fake-folder/LE71140732005007ASA00_B6_VCID_1.TIF'), number='6_vcid_1')
}

EXPANDED_LS7_BANDS = {
    '1': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LE71140732005007ASA00_B1.TIF'),
        type_=u'reflective',
        label=u'Visible Blue',
        number='1',
        cell_size=25.0,
    ),
    '3': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LE71140732005007ASA00_B3.TIF'),
        type_=u'reflective',
        label=u'Visible Red',
        number='3',
        cell_size=25.0,
    ),
    '2': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LE71140732005007ASA00_B2.TIF'),
        type_=u'reflective',
        label=u'Visible Green',
        number='2',
        cell_size=25.0,
    ),
    '5': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LE71140732005007ASA00_B5.TIF'),
        type_=u'reflective',
        label=u'Middle Infrared 1',
        number='5',
        cell_size=25.0,
    ),
    '4': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LE71140732005007ASA00_B4.TIF'),
        type_=u'reflective',
        label=u'Near Infrared',
        number='4',
        cell_size=25.0,
    ),
    '7': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LE71140732005007ASA00_B7.TIF'),
        type_=u'reflective',
        label=u'Middle Infrared 2',
        number='7',
        cell_size=25.0,
    ),
    '8': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LE71140732005007ASA00_B8.TIF'),
        type_=u'panchromatic',
        label=u'Panchromatic',
        number='8',
        cell_size=12.5,
    ),
    '6_vcid_2': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LE71140732005007ASA00_B6_VCID_2.TIF'),
        type_=u'thermal',
        label=u'Thermal Infrared [High Gain]',
        number='6_vcid_2',
        cell_size=50.0,
    ),
    '6_vcid_1': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LE71140732005007ASA00_B6_VCID_1.TIF'),
        type_=u'thermal',
        label=u'Thermal Infrared [Low Gain]',
        number='6_vcid_1',
        cell_size=50.0,
    )
}

BASIC_LS5_BANDS = {
    '1': BandMetadata(path=PosixPath('/tmp/fake-folder/LT51130632005152ASA00_B1.TIF'), number='1'),
    '3': BandMetadata(path=PosixPath('/tmp/fake-folder/LT51130632005152ASA00_B3.TIF'), number='3'),
    '2': BandMetadata(path=PosixPath('/tmp/fake-folder/LT51130632005152ASA00_B2.TIF'), number='2'),
    '5': BandMetadata(path=PosixPath('/tmp/fake-folder/LT51130632005152ASA00_B5.TIF'), number='5'),
    '4': BandMetadata(path=PosixPath('/tmp/fake-folder/LT51130632005152ASA00_B4.TIF'), number='4'),
    '7': BandMetadata(path=PosixPath('/tmp/fake-folder/LT51130632005152ASA00_B7.TIF'), number='7'),
    '6': BandMetadata(path=PosixPath('/tmp/fake-folder/LT51130632005152ASA00_B6.TIF'), number='6')
}

EXPANDED_LS5_BANDS = {
    '1': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LT51130632005152ASA00_B1.TIF'),
        type_=u'reflective',
        label=u'Visible Blue',
        number='1',
        cell_size=25.0,
    ),
    '3': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LT51130632005152ASA00_B3.TIF'),
        type_=u'reflective',
        label=u'Visible Red',
        number='3',
        cell_size=25.0,
    ),
    '2': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LT51130632005152ASA00_B2.TIF'),
        type_=u'reflective',
        label=u'Visible Green',
        number='2',
        cell_size=25.0,
    ),
    '5': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LT51130632005152ASA00_B5.TIF'),
        type_=u'reflective',
        label=u'Middle Infrared 1',
        number='5',
        cell_size=25.0,
    ),
    '4': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LT51130632005152ASA00_B4.TIF'),
        type_=u'reflective',
        label=u'Near Infrared',
        number='4',
        cell_size=25.0,
    ),
    '7': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LT51130632005152ASA00_B7.TIF'),
        type_=u'reflective',
        label=u'Middle Infrared 2',
        number='7',
        cell_size=25.0,
    ),
    '6': BandMetadata(
        path=PosixPath('/tmp/fake-folder/LT51130632005152ASA00_B6.TIF'),
        type_=u'thermal',
        label=u'Thermal Infrared',
        number='6',
        cell_size=100.0,
    )
}


class TestBandExpansion(unittest.TestCase):
    def test_expand_metadata(self):
        id_ = uuid.uuid1()
        create_dt = datetime.datetime.utcnow()
        d = ptype.DatasetMetadata(
            id_=id_,
            creation_dt=create_dt,
            platform=ptype.PlatformMetadata(code='LANDSAT_8'),
            instrument=ptype.InstrumentMetadata(name='OLI_TIRS'),
            image=ptype.ImageMetadata(bands={
                '4': BandMetadata(
                    path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_B4.TIF'),
                    number='4',
                )
            }),
            acquisition=ptype.AcquisitionMetadata(
                groundstation=ptype.GroundstationMetadata('ALSP')
            )
        )

        assert_same(
            metadata.expand_common_metadata(d),
            ptype.DatasetMetadata(
                id_=id_,
                creation_dt=create_dt,
                platform=ptype.PlatformMetadata(code='LANDSAT_8'),
                instrument=ptype.InstrumentMetadata(name='OLI_TIRS'),
                image=ptype.ImageMetadata(bands={
                    '4': BandMetadata(
                        path=PosixPath('/tmp/fake-folder/LC81010782014285LGN00_B4.TIF'),
                        type_='reflective',
                        label='Visible Red',
                        number='4',
                        cell_size=25.0
                    ),
                }),
                acquisition=ptype.AcquisitionMetadata(
                    groundstation=ptype.GroundstationMetadata(
                        # Should have translated groundstation 'ALSP' to standard GSI form 'ASA'
                        code='ASA',
                        label='Alice Springs',
                        eods_domain_code='002'
                    )
                )
            )
        )

    def test_expand_band(self):
        # Create fake image file.
        image_file = write_files({'LC81010782014285LGN00_B6.TIF': 'test'})
        image_file = image_file.joinpath('LC81010782014285LGN00_B6.TIF')

        md = metadata._expand_band_information(
            'LANDSAT_8', 'OLI_TIRS',
            BandMetadata(path=image_file, number='6')
        )

        expected = BandMetadata(
            path=Path(image_file),
            type_=u'reflective',
            label=u'Short-wave Infrared 1',
            number='6',
            # MD5 of image contents ('test')
            cell_size=25.0
        )
        assert_same(md, expected)

    def test_expand_all_ls8_bands(self):
        for number, band_metadata in BASIC_LS8_BANDS.items():
            metadata._expand_band_information('LANDSAT_8', 'OLI_TIRS', band_metadata)

        assert_same(BASIC_LS8_BANDS, EXPANDED_LS8_BANDS)

    def test_expand_all_ls7_bands(self):
        for number, band_metadata in BASIC_LS7_BANDS.items():
            metadata._expand_band_information('LANDSAT_7', 'ETM', band_metadata)

        assert_same(BASIC_LS7_BANDS, EXPANDED_LS7_BANDS)

    def test_expand_all_ls5_bands(self):
        for number, band_metadata in BASIC_LS5_BANDS.items():
            metadata._expand_band_information('LANDSAT_5', 'TM', band_metadata)

        assert_same(BASIC_LS5_BANDS, EXPANDED_LS5_BANDS)
