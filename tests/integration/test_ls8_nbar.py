# coding=utf-8
"""
Package an LS8 NBAR dataset.
"""
from __future__ import absolute_import

import datetime

import yaml
from pathlib import Path

from tests import temp_dir, assert_file_structure, assert_same, integration_test, run_packaging_cli
from tests.integration import load_checksum_filenames, hardlink_arg, directory_size, add_default_software_versions

#: :type: Path
source_folder = Path(__file__).parent.joinpath('input', 'ls8-nbar')
assert source_folder.exists()

source_dataset = source_folder.joinpath('data')
assert source_dataset.exists()

parent_dataset = source_folder.joinpath('parent')
assert parent_dataset.exists()


@integration_test
def test_package():
    output_path = temp_dir()

    run_packaging_cli([
        hardlink_arg(output_path, source_dataset),
        'nbar_brdf',
        '--parent', str(parent_dataset),
        str(source_dataset), str(output_path)
    ])

    output_dataset = output_path.joinpath('LS8_OLITIRS_NBAR_P54_GANBAR01-002_112_079_20140126')

    assert_file_structure(output_path, {
        'LS8_OLITIRS_NBAR_P54_GANBAR01-002_112_079_20140126': {
            'browse.jpg': '',
            'browse.fr.jpg': '',
            'product': {
                'LS8_OLITIRS_NBAR_P54_GANBAR01-002_112_079_20140126_B1.tif': '',
                'LS8_OLITIRS_NBAR_P54_GANBAR01-002_112_079_20140126_B2.tif': '',
                'LS8_OLITIRS_NBAR_P54_GANBAR01-002_112_079_20140126_B3.tif': '',
                'LS8_OLITIRS_NBAR_P54_GANBAR01-002_112_079_20140126_B4.tif': '',
                'LS8_OLITIRS_NBAR_P54_GANBAR01-002_112_079_20140126_B5.tif': '',
                'LS8_OLITIRS_NBAR_P54_GANBAR01-002_112_079_20140126_B6.tif': '',
                'LS8_OLITIRS_NBAR_P54_GANBAR01-002_112_079_20140126_B7.tif': '',
            },
            'ga-metadata.yaml': '',
            'package.sha1': ''
        }
    })

    # Load metadata file and compare it to expected.
    output_checksum_path = output_dataset.joinpath('ga-metadata.yaml')
    assert output_checksum_path.exists()
    md = yaml.load(output_checksum_path.open('r'))

    # ID is different every time: check not none, and clear it.
    assert md['id'] is not None
    md['id'] = None

    # Check metadata is as expected.
    EXPECTED_METADATA['size_bytes'] = directory_size(output_dataset / 'product')
    add_default_software_versions(EXPECTED_METADATA)
    assert_same(
        md,
        EXPECTED_METADATA
    )

    # TODO: Assert correct checksums? They shouldn't change in theory. But they may with gdal versions etc.
    # Check all files are listed in checksum file.
    output_checksum_path = output_dataset.joinpath('package.sha1')
    assert output_checksum_path.exists()
    checksummed_filenames = load_checksum_filenames(output_checksum_path)
    assert checksummed_filenames == [
        'browse.fr.jpg',
        'browse.jpg',
        'ga-metadata.yaml',
        'product/LS8_OLITIRS_NBAR_P54_GANBAR01-002_112_079_20140126_B1.tif',
        'product/LS8_OLITIRS_NBAR_P54_GANBAR01-002_112_079_20140126_B2.tif',
        'product/LS8_OLITIRS_NBAR_P54_GANBAR01-002_112_079_20140126_B3.tif',
        'product/LS8_OLITIRS_NBAR_P54_GANBAR01-002_112_079_20140126_B4.tif',
        'product/LS8_OLITIRS_NBAR_P54_GANBAR01-002_112_079_20140126_B5.tif',
        'product/LS8_OLITIRS_NBAR_P54_GANBAR01-002_112_079_20140126_B6.tif',
        'product/LS8_OLITIRS_NBAR_P54_GANBAR01-002_112_079_20140126_B7.tif'
    ]


EXPECTED_METADATA = {
    'id': None,
    'product_type': 'NBAR',
    'checksum_path': 'package.sha1',
    'ga_label': 'LS8_OLITIRS_NBAR_P54_GANBAR01-002_112_079_20140126',
    'ga_level': 'P54',
    'size_bytes': 4550,
    'platform': {'code': 'LANDSAT_8'},
    'product_doi': 'http://dx.doi.org/10.4225/25/5487CC0D4F40B',
    # Default creation date is the same as the input folder ctime.
    'creation_dt': datetime.datetime.utcfromtimestamp(source_dataset.stat().st_ctime),
    'instrument': {'name': 'OLI_TIRS'},
    'format': {'name': 'GeoTIFF'},
    'extent': {
        'center_dt': datetime.datetime(2014, 1, 26, 2, 5, 23, 126373),
        'coord': {
            'ul': {'lat': -26.37259, 'lon': 116.58914},
            'lr': {'lat': -28.48062, 'lon': 118.96145},
            'ur': {'lat': -26.36025, 'lon': 118.92432},
            'll': {'lat': -28.49412, 'lon': 116.58121}
        }
    },
    'acquisition': {
        'groundstation': {
            'code': 'ASA',
            'eods_domain_code': '002',
            'label': 'Alice Springs'
        }
    },
    'grid_spatial': {
        'projection': {
            'map_projection': 'UTM',
            'resampling_option': 'CUBIC_CONVOLUTION',
            'zone': -50,
            'geo_ref_points': {
                'ul': {'y': 7082987.5, 'x': 459012.5},
                'lr': {'y': 6847987.5, 'x': 692012.5},
                'ur': {'y': 7082987.5, 'x': 692012.5},
                'll': {'y': 6847987.5, 'x': 459012.5}
            },
            'orientation': 'NORTH_UP',
            'datum': 'GDA94',
            'ellipsoid': 'GRS80'
        }
    },
    'image': {
        'satellite_ref_point_start': {'y': 79, 'x': 112},
        'bands': {
            '4': {
                'type': 'reflective',
                'cell_size': 25.0,
                'path': 'product/LS8_OLITIRS_NBAR_P54_GANBAR01-002_112_079_20140126_B4.tif',
                'label': 'Visible Red',
                'number': '4'
            },
            '6': {
                'type': 'reflective',
                'cell_size': 25.0,
                'path': 'product/LS8_OLITIRS_NBAR_P54_GANBAR01-002_112_079_20140126_B6.tif',
                'label': 'Short-wave Infrared 1',
                'number': '6'
            },
            '1': {
                'type': 'reflective',
                'cell_size': 25.0,
                'path': 'product/LS8_OLITIRS_NBAR_P54_GANBAR01-002_112_079_20140126_B1.tif',
                'label': 'Coastal Aerosol',
                'number': '1'
            },
            '2': {
                'type': 'reflective',
                'cell_size': 25.0,
                'path': 'product/LS8_OLITIRS_NBAR_P54_GANBAR01-002_112_079_20140126_B2.tif',
                'label': 'Visible Blue',
                'number': '2'
            },
            '3': {
                'type': 'reflective',
                'cell_size': 25.0,
                'path': 'product/LS8_OLITIRS_NBAR_P54_GANBAR01-002_112_079_20140126_B3.tif',
                'label': 'Visible Green',
                'number': '3'
            },
            '5': {
                'type': 'reflective',
                'cell_size': 25.0,
                'path': 'product/LS8_OLITIRS_NBAR_P54_GANBAR01-002_112_079_20140126_B5.tif',
                'label': 'Near Infrared',
                'number': '5'
            },
            '7': {
                'type': 'reflective',
                'cell_size': 25.0,
                'path': 'product/LS8_OLITIRS_NBAR_P54_GANBAR01-002_112_079_20140126_B7.tif',
                'label': 'Short-wave Infrared 2',
                'number': '7'
            }
        }
    },
    'browse': {
        'full': {
            'path': 'browse.fr.jpg',
            'shape': {'y': 10, 'x': 10},
            'red_band': '7',
            'blue_band': '2',
            'file_type': 'image/jpg',
            'cell_size': 25.0,
            'green_band': '5'
        },
        'medium': {
            'path': 'browse.jpg',
            'shape': {'y': 1024, 'x': 1024},
            'red_band': '7',
            'blue_band': '2',
            'file_type': 'image/jpg',
            'cell_size': 0.244140625,
            'green_band': '5'
        }
    },
    'lineage': {
        'algorithm': {
            'name': 'brdf',
            'doi': 'http://dx.doi.org/10.1109/JSTARS.2010.2042281',
            'version': '2.0',
            'parameters': {
                'band_3_brdf_geo': 0.029762539794515593,
                'band_2_brdf_vol': 0.0641287443021489,
                'elevation': 0.168,
                'band_5_brdf_geo': 0.05726580475363577,
                'band_3_brdf_iso': 0.2494662967585558,
                'band_5_brdf_iso': 0.48908226159467477,
                'band_7_brdf_vol': 0.09830102426119791,
                'band_7_brdf_iso': 0.44022800551699187,
                'band_4_brdf_iso': 0.3316855374068447,
                'solar_distance': 0.9906,
                'band_5_brdf_vol': 0.15177135337529846,
                'band_4_brdf_geo': 0.03216758465378772,
                'band_4_brdf_vol': 0.14373983883221186,
                'band_3_brdf_vol': 0.09341573511323348,
                'band_1_brdf_iso': 0.06963196404022864,
                'band_7_brdf_geo': 0.058113631944287425,
                'ozone': 0.257,
                'band_1_brdf_vol': 0.03317581036104479,
                'aerosol': 0.066711,
                'water_vapour': 3.8899993896484375,
                'band_2_brdf_geo': 0.012102656283915781,
                'band_1_brdf_geo': 0.002104826893857174,
                'band_2_brdf_iso': 0.12737418602127198
            },
        },
        'machine': {
            'software_versions': {
                'nbar': '4.0'
            }
        },
        'ancillary': {
            'band_3_brdf_geo': {
                'file_owner': 'Matt Paget',
                'modification_dt': datetime.datetime(2010, 3, 10, 15, 24, 58),
                'name': 'MCD43A1.2010.049.aust.005.b03.500m_0620_0670nm_brdf_par_fgeo.hdf.gz',
                'access_dt': datetime.datetime(2016, 2, 8, 23, 1, 55),
                'type': 'band_3_brdf_geo',
                'uri': '/g/data/u39/public/data/modis/lpdaac-mosaics-cmar/v1-hdf4/aust/MCD43A1.005/2010.02.18/'
                       'MCD43A1.2010.049.aust.005.b03.500m_0620_0670nm_brdf_par_fgeo.hdf.gz'},
            'band_2_brdf_vol': {
                'file_owner': 'Matt Paget',
                'modification_dt': datetime.datetime(2010, 3, 10, 15, 25, 51),
                'name': 'MCD43A1.2010.049.aust.005.b11.500m_0545_0565nm_brdf_par_fvol.hdf.gz',
                'access_dt': datetime.datetime(2016, 2, 8, 23, 1, 53),
                'type': 'band_2_brdf_vol',
                'uri': '/g/data/u39/public/data/modis/lpdaac-mosaics-cmar/v1-hdf4/aust/MCD43A1.005/2010.02.18/'
                       'MCD43A1.2010.049.aust.005.b11.500m_0545_0565nm_brdf_par_fvol.hdf.gz'},
            'elevation': {
                'file_owner': 'Roger Edberg',
                'modification_dt': datetime.datetime(2010, 2, 23, 21, 59, 29),
                'name': 'DEM_one_deg.tif',
                'access_dt': datetime.datetime(2016, 2, 8, 23, 1, 39),
                'type': 'elevation',
                'uri': '/g/data/v10/eoancillarydata/elevation/world_1deg/DEM_one_deg.tif'},
            'band_5_brdf_geo': {
                'file_owner': 'Matt Paget',
                'modification_dt': datetime.datetime(2010, 3, 10, 15, 26, 39),
                'name': 'MCD43A1.2010.049.aust.005.b18.500m_1628_1652nm_brdf_par_fgeo.hdf.gz',
                'access_dt': datetime.datetime(2016, 2, 8, 23, 2, 9),
                'type': 'band_5_brdf_geo',
                'uri': '/g/data/u39/public/data/modis/lpdaac-mosaics-cmar/v1-hdf4/aust/MCD43A1.005/2010.02.18/'
                       'MCD43A1.2010.049.aust.005.b18.500m_1628_1652nm_brdf_par_fgeo.hdf.gz'},
            'band_3_brdf_iso': {
                'file_owner': 'Matt Paget',
                'modification_dt': datetime.datetime(2010, 3, 10, 15, 24, 48),
                'name': 'MCD43A1.2010.049.aust.005.b01.500m_0620_0670nm_brdf_par_fiso.hdf.gz',
                'access_dt': datetime.datetime(2016, 2, 8, 23, 1, 57),
                'type': 'band_3_brdf_iso',
                'uri': '/g/data/u39/public/data/modis/lpdaac-mosaics-cmar/v1-hdf4/aust/MCD43A1.005/2010.02.18/'
                       'MCD43A1.2010.049.aust.005.b01.500m_0620_0670nm_brdf_par_fiso.hdf.gz'},
            'band_5_brdf_iso': {
                'file_owner': 'Matt Paget',
                'modification_dt': datetime.datetime(2010, 3, 10, 15, 26, 27),
                'name': 'MCD43A1.2010.049.aust.005.b16.500m_1628_1652nm_brdf_par_fiso.hdf.gz',
                'access_dt': datetime.datetime(2016, 2, 8, 23, 2, 12),
                'type': 'band_5_brdf_iso',
                'uri': '/g/data/u39/public/data/modis/lpdaac-mosaics-cmar/v1-hdf4/aust/MCD43A1.005/2010.02.18/'
                       'MCD43A1.2010.049.aust.005.b16.500m_1628_1652nm_brdf_par_fiso.hdf.gz'},
            'band_7_brdf_vol': {
                'file_owner': 'Matt Paget',
                'modification_dt': datetime.datetime(2010, 3, 10, 15, 26, 53),
                'name': 'MCD43A1.2010.049.aust.005.b20.500m_2105_2155nm_brdf_par_fvol.hdf.gz',
                'access_dt': datetime.datetime(2016, 2, 8, 23, 2, 22),
                'type': 'band_7_brdf_vol',
                'uri': '/g/data/u39/public/data/modis/lpdaac-mosaics-cmar/v1-hdf4/aust/MCD43A1.005/2010.02.18/'
                       'MCD43A1.2010.049.aust.005.b20.500m_2105_2155nm_brdf_par_fvol.hdf.gz'},
            'band_7_brdf_iso': {
                'file_owner': 'Matt Paget',
                'modification_dt': datetime.datetime(2010, 3, 10, 15, 26, 46),
                'name': 'MCD43A1.2010.049.aust.005.b19.500m_2105_2155nm_brdf_par_fiso.hdf.gz',
                'access_dt': datetime.datetime(2016, 2, 8, 23, 2, 19),
                'type': 'band_7_brdf_iso',
                'uri': '/g/data/u39/public/data/modis/lpdaac-mosaics-cmar/v1-hdf4/aust/MCD43A1.005/2010.02.18/'
                       'MCD43A1.2010.049.aust.005.b19.500m_2105_2155nm_brdf_par_fiso.hdf.gz'},
            'band_4_brdf_iso': {
                'file_owner': 'Matt Paget',
                'modification_dt': datetime.datetime(2010, 3, 10, 15, 25, 4),
                'name': 'MCD43A1.2010.049.aust.005.b04.500m_0841_0876nm_brdf_par_fiso.hdf.gz',
                'access_dt': datetime.datetime(2016, 2, 8, 23, 2, 4),
                'type': 'band_4_brdf_iso',
                'uri': '/g/data/u39/public/data/modis/lpdaac-mosaics-cmar/v1-hdf4/aust/MCD43A1.005/2010.02.18/'
                       'MCD43A1.2010.049.aust.005.b04.500m_0841_0876nm_brdf_par_fiso.hdf.gz'},
            'solar_distance': {
                'file_owner': 'Roger Edberg',
                'modification_dt': datetime.datetime(2012, 5, 21, 2, 4, 1),
                'name': 'EarthSun_distanceLUT.txt',
                'access_dt': datetime.datetime(2016, 2, 8, 23, 1, 40),
                'type': 'solar_distance',
                'uri': '/g/data/v10/eoancillarydata/lookup_tables/earthsun_distance/EarthSun_distanceLUT.txt'},
            'band_5_brdf_vol': {
                'file_owner': 'Matt Paget',
                'modification_dt': datetime.datetime(2010, 3, 10, 15, 26, 34),
                'name': 'MCD43A1.2010.049.aust.005.b17.500m_1628_1652nm_brdf_par_fvol.hdf.gz',
                'access_dt': datetime.datetime(2016, 2, 8, 23, 2, 14),
                'type': 'band_5_brdf_vol',
                'uri': '/g/data/u39/public/data/modis/lpdaac-mosaics-cmar/v1-hdf4/aust/MCD43A1.005/2010.02.18/'
                       'MCD43A1.2010.049.aust.005.b17.500m_1628_1652nm_brdf_par_fvol.hdf.gz'},
            'band_4_brdf_geo': {
                'file_owner': 'Matt Paget',
                'modification_dt': datetime.datetime(2010, 3, 10, 15, 25, 15),
                'name': 'MCD43A1.2010.049.aust.005.b06.500m_0841_0876nm_brdf_par_fgeo.hdf.gz',
                'access_dt': datetime.datetime(2016, 2, 8, 23, 2, 2),
                'type': 'band_4_brdf_geo',
                'uri': '/g/data/u39/public/data/modis/lpdaac-mosaics-cmar/v1-hdf4/aust/MCD43A1.005/2010.02.18/'
                       'MCD43A1.2010.049.aust.005.b06.500m_0841_0876nm_brdf_par_fgeo.hdf.gz'},
            'band_4_brdf_vol': {
                'file_owner': 'Matt Paget',
                'modification_dt': datetime.datetime(2010, 3, 10, 15, 25, 9),
                'name': 'MCD43A1.2010.049.aust.005.b05.500m_0841_0876nm_brdf_par_fvol.hdf.gz',
                'access_dt': datetime.datetime(2016, 2, 8, 23, 2, 7),
                'type': 'band_4_brdf_vol',
                'uri': '/g/data/u39/public/data/modis/lpdaac-mosaics-cmar/v1-hdf4/aust/MCD43A1.005/2010.02.18/'
                       'MCD43A1.2010.049.aust.005.b05.500m_0841_0876nm_brdf_par_fvol.hdf.gz'},
            'band_3_brdf_vol': {
                'file_owner': 'Matt Paget',
                'modification_dt': datetime.datetime(2010, 3, 10, 15, 24, 53),
                'name': 'MCD43A1.2010.049.aust.005.b02.500m_0620_0670nm_brdf_par_fvol.hdf.gz',
                'access_dt': datetime.datetime(2016, 2, 8, 23, 2),
                'type': 'band_3_brdf_vol',
                'uri': '/g/data/u39/public/data/modis/lpdaac-mosaics-cmar/v1-hdf4/aust/MCD43A1.005/2010.02.18/'
                       'MCD43A1.2010.049.aust.005.b02.500m_0620_0670nm_brdf_par_fvol.hdf.gz'},
            'band_1_brdf_iso': {
                'file_owner': 'Matt Paget',
                'modification_dt': datetime.datetime(2010, 3, 10, 15, 25, 20),
                'name': 'MCD43A1.2010.049.aust.005.b07.500m_0459_0479nm_brdf_par_fiso.hdf.gz',
                'access_dt': datetime.datetime(2016, 2, 8, 23, 1, 44),
                'type': 'band_1_brdf_iso',
                'uri': '/g/data/u39/public/data/modis/lpdaac-mosaics-cmar/v1-hdf4/aust/MCD43A1.005/2010.02.18/'
                       'MCD43A1.2010.049.aust.005.b07.500m_0459_0479nm_brdf_par_fiso.hdf.gz'},
            'band_7_brdf_geo': {
                'file_owner': 'Matt Paget',
                'modification_dt': datetime.datetime(2010, 3, 10, 15, 26, 58),
                'name': 'MCD43A1.2010.049.aust.005.b21.500m_2105_2155nm_brdf_par_fgeo.hdf.gz',
                'access_dt': datetime.datetime(2016, 2, 8, 23, 2, 17),
                'type': 'band_7_brdf_geo',
                'uri': '/g/data/u39/public/data/modis/lpdaac-mosaics-cmar/v1-hdf4/aust/MCD43A1.005/2010.02.18/'
                       'MCD43A1.2010.049.aust.005.b21.500m_2105_2155nm_brdf_par_fgeo.hdf.gz'},
            'ozone': {
                'file_owner': 'Roger Edberg',
                'modification_dt': datetime.datetime(2010, 2, 23, 22, 0, 21),
                'name': 'feb.tif', 'access_dt': datetime.datetime(2016, 2, 8, 23, 1, 40),
                'type': 'ozone',
                'uri': '/g/data/v10/eoancillarydata/lookup_tables/ozone/feb.tif'},
            'band_1_brdf_vol': {
                'file_owner': 'Matt Paget',
                'modification_dt': datetime.datetime(2010, 3, 10, 15, 25, 24),
                'name': 'MCD43A1.2010.049.aust.005.b08.500m_0459_0479nm_brdf_par_fvol.hdf.gz',
                'access_dt': datetime.datetime(2016, 2, 8, 23, 1, 46),
                'type': 'band_1_brdf_vol',
                'uri': '/g/data/u39/public/data/modis/lpdaac-mosaics-cmar/v1-hdf4/aust/MCD43A1.005/2010.02.18/'
                       'MCD43A1.2010.049.aust.005.b08.500m_0459_0479nm_brdf_par_fvol.hdf.gz'},
            'aerosol': {
                'file_owner': 'Roger Edberg',
                'modification_dt': datetime.datetime(2012, 7, 12, 1, 19, 43),
                'name': 'aot_mean_Feb_All_Aerosols.cmp',
                'access_dt': datetime.datetime(2016, 2, 8, 23, 1, 40), 'type': 'aerosol',
                'uri': '/g/data/v10/eoancillarydata/aerosol/AATSR/2.0/aot_mean_Feb_All_Aerosols.cmp'},
            'water_vapour': {
                'file_owner': 'Landsat Processing',
                'modification_dt': datetime.datetime(2016, 2, 7, 14, 21, 37),
                'name': 'pr_wtr.eatm.2010.tif',
                'access_dt': datetime.datetime(2016, 2, 8, 23, 1, 41),
                'type': 'water_vapour',
                'uri': '/g/data/v10/eoancillarydata/water_vapour/pr_wtr.eatm.2010.tif'},
            'band_2_brdf_geo': {
                'file_owner': 'Matt Paget',
                'modification_dt': datetime.datetime(2010, 3, 10, 15, 25, 57),
                'name': 'MCD43A1.2010.049.aust.005.b12.500m_0545_0565nm_brdf_par_fgeo.hdf.gz',
                'access_dt': datetime.datetime(2016, 2, 8, 23, 1, 48),
                'type': 'band_2_brdf_geo',
                'uri': '/g/data/u39/public/data/modis/lpdaac-mosaics-cmar/v1-hdf4/aust/MCD43A1.005/2010.02.18/'
                       'MCD43A1.2010.049.aust.005.b12.500m_0545_0565nm_brdf_par_fgeo.hdf.gz'},
            'band_1_brdf_geo': {
                'file_owner': 'Matt Paget',
                'modification_dt': datetime.datetime(2010, 3, 10, 15, 25, 36),
                'name': 'MCD43A1.2010.049.aust.005.b09.500m_0459_0479nm_brdf_par_fgeo.hdf.gz',
                'access_dt': datetime.datetime(2016, 2, 8, 23, 1, 41),
                'type': 'band_1_brdf_geo',
                'uri': '/g/data/u39/public/data/modis/lpdaac-mosaics-cmar/v1-hdf4/aust/MCD43A1.005/2010.02.18/'
                       'MCD43A1.2010.049.aust.005.b09.500m_0459_0479nm_brdf_par_fgeo.hdf.gz'},
            'band_2_brdf_iso': {
                'file_owner': 'Matt Paget',
                'modification_dt': datetime.datetime(2010, 3, 10, 15, 25, 46),
                'name': 'MCD43A1.2010.049.aust.005.b10.500m_0545_0565nm_brdf_par_fiso.hdf.gz',
                'access_dt': datetime.datetime(2016, 2, 8, 23, 1, 50),
                'type': 'band_2_brdf_iso',
                'uri': '/g/data/u39/public/data/modis/lpdaac-mosaics-cmar/v1-hdf4/aust/MCD43A1.005/2010.02.18/'
                       'MCD43A1.2010.049.aust.005.b10.500m_0545_0565nm_brdf_par_fiso.hdf.gz'}},
        'source_datasets': {
            'ortho': {
                'product_level': 'L1T',
                'product_type': 'ortho',
                'id': '5cf41d98-eda9-11e4-8a8e-1040f381a756',
                'usgs': {
                    'scene_id': 'LC81120792014026ASA00'
                },
                'extent': {
                    'center_dt': datetime.datetime(2014, 1, 26, 2, 5, 23, 126373),
                    'coord': {
                        'ul': {'lat': -26.37259, 'lon': 116.58914},
                        'lr': {'lat': -28.48062, 'lon': 118.96145},
                        'ur': {'lat': -26.36025, 'lon': 118.92432},
                        'll': {'lat': -28.49412, 'lon': 116.58121}
                    }
                },
                'size_bytes': 1854924494,
                'platform': {
                    'code': 'LANDSAT_8'},
                'creation_dt': datetime.datetime(2015, 4, 7, 0, 58, 8),
                'instrument': {'name': 'OLI_TIRS'},
                'checksum_path': 'package.sha1',
                'ga_label': 'LS8_OLITIRS_OTH_P51_GALPGS01-002_112_079_20140126',
                'image': {
                    'ground_control_points_model': 380,
                    'geometric_rmse_model_x': 3.74,
                    'sun_elevation': 57.68594289,
                    'satellite_ref_point_start': {
                        'y': 79, 'x': 112},
                    'geometric_rmse_model': 5.458,
                    'bands': {
                        '6': {
                            'type': 'reflective',
                            'cell_size': 25.0,
                            'path': 'product/LC81120792014026ASA00_B6.TIF',
                            'label': 'Short-wave Infrared 1',
                            'number': '6'
                        },
                        '4': {
                            'type': 'reflective',
                            'cell_size': 25.0,
                            'path': 'product/LC81120792014026ASA00_B4.TIF',
                            'label': 'Visible Red',
                            'number': '4'
                        },
                        '9': {
                            'type': 'atmosphere',
                            'cell_size': 25.0,
                            'path': 'product/LC81120792014026ASA00_B9.TIF',
                            'label': 'Cirrus',
                            'number': '9'},
                        '2': {
                            'type': 'reflective',
                            'cell_size': 25.0,
                            'path': 'product/LC81120792014026ASA00_B2.TIF',
                            'label': 'Visible Blue',
                            'number': '2'},
                        '8': {
                            'type': 'panchromatic',
                            'cell_size': 12.5,
                            'path': 'product/LC81120792014026ASA00_B8.TIF',
                            'label': 'Panchromatic',
                            'number': '8'},
                        '11': {
                            'type': 'thermal',
                            'cell_size': 25.0,
                            'path': 'product/LC81120792014026ASA00_B11.TIF',
                            'label': 'Thermal Infrared 2',
                            'number': '11'},
                        '5': {
                            'type': 'reflective',
                            'cell_size': 25.0,
                            'path': 'product/LC81120792014026ASA00_B5.TIF',
                            'label': 'Near Infrared',
                            'number': '5'
                        },
                        'quality': {
                            'type': 'quality',
                            'cell_size': 25.0,
                            'path': 'product/LC81120792014026ASA00_BQA.TIF',
                            'label': 'Quality',
                            'number': 'quality'},
                        '10': {
                            'type': 'thermal',
                            'cell_size': 25.0,
                            'path': 'product/LC81120792014026ASA00_B10.TIF',
                            'label': 'Thermal Infrared 1',
                            'number': '10'},
                        '1': {
                            'type': 'reflective',
                            'cell_size': 25.0,
                            'path': 'product/LC81120792014026ASA00_B1.TIF',
                            'label': 'Coastal Aerosol',
                            'number': '1'},
                        '3': {
                            'type': 'reflective',
                            'cell_size': 25.0,
                            'path': 'product/LC81120792014026ASA00_B3.TIF',
                            'label': 'Visible Green',
                            'number': '3'},
                        '7': {
                            'type': 'reflective',
                            'cell_size': 25.0,
                            'path': 'product/LC81120792014026ASA00_B7.TIF',
                            'label': 'Short-wave Infrared 2',
                            'number': '7'}
                    },
                    'sun_earth_distance': 0.9845943,
                    'geometric_rmse_model_y': 3.975,
                    'sun_azimuth': 82.05926755,
                    'cloud_cover_percentage': 0.62
                },
                'browse': {
                    'full': {
                        'path': 'browse.fr.jpg',
                        'shape': {'y': 9401, 'x': 9321},
                        'red_band': '7',
                        'blue_band': '2',
                        'file_type': 'image/jpg',
                        'cell_size': 25.0,
                        'green_band': '5'
                    },
                    'medium': {
                        'path': 'browse.jpg',
                        'shape': {'y': 1033, 'x': 1024},
                        'red_band': '7',
                        'blue_band': '2',
                        'file_type': 'image/jpg',
                        'cell_size': 227.5634765625,
                        'green_band': '5'
                    }
                },
                'grid_spatial': {
                    'projection': {
                        'map_projection': 'UTM',
                        'resampling_option': 'CUBIC_CONVOLUTION',
                        'zone': -50,
                        'geo_ref_points': {
                            'ul': {'y': 7082987.5, 'x': 459012.5},
                            'lr': {'y': 6847987.5, 'x': 692012.5},
                            'ur': {'y': 7082987.5, 'x': 692012.5},
                            'll': {'y': 6847987.5, 'x': 459012.5}
                        },
                        'orientation': 'NORTH_UP',
                        'datum': 'GDA94',
                        'ellipsoid': 'GRS80'
                    }
                },
                'acquisition': {
                    'groundstation': {
                        'code': 'ASA',
                        'eods_domain_code': '002',
                        'label': 'Alice Springs'
                    }
                },
                'format': {'name': 'GEOTIFF'},
                'lineage': {
                    'algorithm': {
                        'name': 'LPGS',
                        'parameters': {},
                        'version': '2.4.0'
                    },
                    'machine': {},
                    'source_datasets': {
                        'satellite_telemetry_data': {
                            'product_type': 'satellite_telemetry_data',
                            'checksum_path': 'package.sha1',
                            'id': '4ec8fe97-e8b9-11e4-87ff-1040f381a756',
                            'ga_label': 'LS8_OLITIRS_STD-MD_P00_LC81160740742015089ASA00_'
                                        '116_074_20150330T022553Z20150330T022657',
                            'usgs': {
                                'interval_id': 'LC81160740742015089ASA00'
                            },
                            'ga_level': 'P00',
                            'image': {
                                'satellite_ref_point_end': {
                                    'y': 74, 'x': 116},
                                'satellite_ref_point_start': {
                                    'y': 74, 'x': 116}},
                            'size_bytes': 637660782,
                            'platform': {
                                'code': 'LANDSAT_8'},
                            'creation_dt': datetime.datetime(
                                2015, 4, 22, 6, 32,
                                4),
                            'acquisition': {
                                'aos': datetime.datetime(
                                    2015, 3, 30, 2, 25,
                                    53, 346000),
                                'los': datetime.datetime(
                                    2015, 3, 30, 2, 26,
                                    57, 325000),
                                'groundstation': {
                                    'code': 'ASA'},
                                'platform_orbit': 11308},
                            'instrument': {
                                'name': 'OLI_TIRS'},
                            'format': {
                                'name': 'MD'},
                            'lineage': {
                                'machine': {
                                    'hostname': 'niggle.local',
                                    'runtime_id': '4bc6225c-e8b9-11e4-8b66-1040f381a756',
                                    'version': '2.4.0',
                                    'type_id': 'jobmanager',
                                    'uname': 'Darwin niggle.local 14.3.0 Darwin Kernel Version 14.3.0: '
                                             'Mon Mar 23 11:59:05 PDT 2015; '
                                             'root:xnu-2782.20.48~5/RELEASE_X86_64 x86_64'},
                                'source_datasets': {}
                            }
                        }
                    },
                    'ancillary': {
                        'bpf_oli': {
                            'name': 'LO8BPF20140127130115_20140127144056.01'},
                        'cpf': {
                            'name': 'L8CPF20140101_20140331.05'},
                        'rlut': {
                            'name': 'L8RLUT20130211_20431231v09.h5'},
                        'bpf_tirs': {
                            'name': 'LT8BPF20140116023714_20140116032836.02'}
                    }
                }
            }
        }
    }
}
