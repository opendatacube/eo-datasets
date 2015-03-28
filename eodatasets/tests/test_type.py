import logging
import unittest
import uuid
import collections

import dateutil.parser
from pathlib import Path
import yaml

from eodatasets import type


def _serialise_to_file(file_name, dataset):
    """

    :type file_name: str
    :type dataset: type.DatasetMetadata
    :return:
    """
    with open('%s.yaml' % file_name, 'w') as f:
        f.write(yaml.dump(dataset, default_flow_style=False, indent=4))

    with open('%s.properties' % file_name, 'w') as f:
        f.writelines(['%s=%r\n' % (k, v) for k, v in type.as_flat_key_value(dataset)])


def _build_ls8_raw():
    _reset_runtime_id()
    raw = type.DatasetMetadata(
        id_=uuid.UUID('1c76a8ca-51ae-11e4-8644-0050568d59ac'),
        usgs_dataset_id='LC81010782014285LGN00',
        creation_dt=dateutil.parser.parse("2014-10-12 04:18:01"),
        size_bytes=5680940 * 1024,
        ga_label='MDF_P00_LC81010700832014285LGN00_101_070-083_20141012T032336Z20141012T032910_1',
        product_type='RAW',
        platform=type.PlatformMetadata(code='LANDSAT-8'),
        instrument=type.InstrumentMetadata(name='OLI_TIRS'),
        format_=type.FormatMetadata(name='MD'),
        acquisition=type.AcquisitionMetadata(
            aos=dateutil.parser.parse('2014-10-12T00:52:52'),
            los=dateutil.parser.parse('2014-10-12T00:58:37'),
            groundstation=type.GroundstationMetadata(
                code='ASA',
                antenna_coord=type.Coord(
                    lat=-23.759,
                    lon=133.8824,
                    height=579.312
                )
            ),
            heading='D',
            platform_orbit=8846
        ),
        extent=None,
        grid_spatial=None,
        browse=None,
        image=type.ImageMetadata(
            satellite_ref_point_start=type.Point(101, 70),
            satellite_ref_point_end=type.Point(101, 83)
        ),
        lineage=type.LineageMetadata(
            machine=type.MachineMetadata()
        )
    )
    return raw


def _build_ls8_ortho():

    _reset_runtime_id()
    return type.DatasetMetadata(
        id_=uuid.UUID('17b92c16-51d3-11e4-909d-005056bb6972'),
        ga_label='LS8_OLITIRS_OTH_P51_GALPGS01-002_101_078_20141012',
        product_type='GAORTHO01',
        creation_dt=dateutil.parser.parse('2014-10-12 05:46:20'),
        size_bytes=2386550 * 1024,
        platform=type.PlatformMetadata(code='LANDSAT-8'),
        instrument=type.InstrumentMetadata(
            name='OLI_TIRS',
            type_="Multi-Spectral",
            operation_mode='PUSH-BROOM'
        ),
        format_=type.FormatMetadata(name='GeoTiff', version=1),
        extent=type.ExtentMetadata(
            reference_system='WGS84',
            coord=type.Polygon(
                ul=type.Coord(lat=-24.97,
                              lon=133.97969),
                ur=type.Coord(lat=-24.96826,
                              lon=136.24838),
                lr=type.Coord(lat=-26.96338,
                              lon=136.26962),
                ll=type.Coord(lat=-26.96528,
                              lon=133.96233)
            ),
            from_dt=dateutil.parser.parse("2014-10-12T00:55:54"),
            center_dt=dateutil.parser.parse("2014-10-12T00:56:06"),
            to_dt=dateutil.parser.parse("2014-10-12T00:56:18"),

        ),
        grid_spatial=type.GridSpatialMetadata(
            dimensions=[
                type.DimensionMetadata(name='sample', resolution=25.0, size=9161),
                type.DimensionMetadata(name='line', resolution=25.0, size=9161)
            ],
            projection=type.ProjectionMetadata(
                centre_point=type.Point(511512.500000, 7127487.500000),
                geo_ref_points=type.Polygon(
                    ul=type.Point(397012.5, 7237987.5),
                    ur=type.Point(626012.5, 7237987.5),
                    ll=type.Point(397012.5, 7016987.5),
                    lr=type.Point(626012.5, 7016987.5)
                ),
                datum='GDA94',
                ellipsoid='GRS80',
                point_in_pixel='UL',
                map_projection='UTM',
                resampling_option='CUBIC_CONVOLUTION',
                zone=-53
            )
        ),
        browse=collections.OrderedDict([
            ('medium',
             type.BrowseMetadata(
                 path=Path('product/LS8_OLITIRS_OTH_P51_GALPGS01-032_101_078_20141012.jpg'),
                 file_type='image/jpg',
                 checksum_md5='6dd96d4e93e48eb8b15c842cfb40f466',
                 sample_pixel_resolution=219.75,
                 red_band=7,
                 green_band=5,
                 blue_band=1
             )),
            ('full',
             type.BrowseMetadata(
                 path=Path('LS8_OLITIRS_OTH_P51_GALPGS01-032_101_078_20141012_FR.jpg'),
                 file_type='image/jpg',
                 checksum_md5='232606ffabd1596431acb6ad9f488cf4',
                 sample_pixel_resolution=25.0,
                 red_band=7,
                 green_band=5,
                 blue_band=1
             ))
        ]),
        image=type.ImageMetadata(
            satellite_ref_point_start=type.Point(101, 78),
            cloud_cover_percentage=0,
            cloud_cover_details=None,

            sun_elevation=58.00268508,
            sun_azimuth=59.41814014,

            ground_control_points_model=420,
            geometric_rmse_model=4.610,
            geometric_rmse_model_x=3.527,
            geometric_rmse_model_y=2.968,

            # TODO: What are these two?
            viewing_incidence_angle_long_track=0,
            viewing_incidence_angle_x_track=0,

            bands=collections.OrderedDict([
                ('coastal_aerosol', type.BandMetadata(
                    path=Path('product/LC81010782014285LGN00_B1.TIF'),
                    number=1,
                    type='reflective',
                    cell_size=25.0,
                    checksum_md5='db31e11abe485fa3e78acd6f25b15d24'
                )),
                ('visible_blue', type.BandMetadata(
                    path=Path('product/LC81010782014285LGN00_B2.TIF'),
                    number=2,
                    type='reflective',
                    cell_size=25.0,
                    checksum_md5='c29c4edf7befa459b547bf7a9585e38a'
                )),
                ('visible_green', type.BandMetadata(
                    path=Path('product/LC81010782014285LGN00_B3.TIF'),
                    number=3,
                    type='reflective',
                    cell_size=25.0,
                    checksum_md5='d02f11a48ad72133332e94e0442fee15'
                )),
                ('visible_red', type.BandMetadata(
                    path=Path('product/LC81010782014285LGN00_B4.TIF'),
                    number=4,
                    type='reflective',
                    cell_size=25.0,
                    checksum_md5='dfb10aa259f44e30eb6b022d9b34394d'
                )),
                ('near_infrared', type.BandMetadata(
                    path=Path('product/LC81010782014285LGN00_B5.TIF'),
                    number=5,
                    type='reflective',
                    cell_size=25.0,
                    checksum_md5='371c364cea0068cdd706da24b771ce61'
                )),
                ('short_wave_infrared1', type.BandMetadata(
                    path=Path('product/LC81010782014285LGN00_B6.TIF'),
                    number=6,
                    type='reflective',
                    cell_size=25.0,
                    checksum_md5='bab291bf301289bd125de213889c5cae'
                )),
                ('short_wave_infrared2', type.BandMetadata(
                    path=Path('product/LC81010782014285LGN00_B7.TIF'),
                    number=7,
                    type='reflective',
                    cell_size=25.0,
                    checksum_md5='351329b7a6e2d45a0c43dfc4759e5b7e'
                )),
                ('panchromatic', type.BandMetadata(
                    path=Path('product/LC81010782014285LGN00_B8.TIF'),
                    number=8,
                    type='panchromatic',
                    cell_size=12.50,
                    shape=type.Point(17761, 18241),
                    checksum_md5='baddd6402559d773f36858931512a333'
                )),
                ('cirrus', type.BandMetadata(
                    path=Path('product/LC81010782014285LGN00_B9.TIF'),
                    number=9,
                    type='atmosphere',
                    checksum_md5='661ce050355f0fc1efc625857c9a9d97'
                )),
                ('thermal_infrared1', type.BandMetadata(
                    path=Path('product/LC81010782014285LGN00_B10.TIF'),
                    number=10,
                    type='thermal',
                    cell_size=25.0,
                    shape=type.Point(8881, 9121),
                    checksum_md5='4f2f5de0403e575f2712778de3877ddc'
                )),
                ('thermal_infrared2', type.BandMetadata(
                    path=Path('product/LC81010782014285LGN00_B11.TIF'),
                    number=11,
                    type='thermal',
                    cell_size=25.0,
                    shape=type.Point(8881, 9121),
                    checksum_md5='328dbb7324bc92c080d2acc9b62d1d9c'
                )),
                ('quality', type.BandMetadata(
                    path=Path('product/LC81010782014285LGN00_BQA.TIF'),
                    number='QA',
                    type='quality',
                    checksum_md5='469bf4767d3b9e7dd4e8093a80455fca'
                ))
            ])
        ),
        lineage=type.LineageMetadata(
            algorithm=type.AlgorithmMetadata(
                name='Pinkmatter Landsat Processor',
                version='3.3.3104',
                parameters={
                    'resampling': 'CC',
                    'radiometric_correction': 'CPF',
                    'orientation': 'NUP',
                    'hemisphere': 'S',
                }
            ),
            machine=type.MachineMetadata(hostname='rhe-jm-prod08.prod.lan', type_id='jobmanager',
                                         uname='Linux rhe-jm-dev08.dev.lan 2.6.32-279.22.1.el6.x86_64 #1 SMP Sun Oct '
                                               '12 '
                                               '09:21:40 EST 2014 x86_64 x86_64 x86_64 GNU/Linux'),
            ancillary={
                'cpf':
                    type.AncillaryMetadata(name='L8CPF20141001_20141231.01',
                                           uri='/eoancillarydata/sensor-specific/LANDSAT8/CalibrationParameterFile'
                                               '/L8CPF20141001_20141231.01'),
                'bpf_tirs':
                    type.AncillaryMetadata(name='LT8BPF20141012002432_20141012020301.01',
                                           uri='/eoancillarydata/sensor-specific/LANDSAT8/BiasParameterFile/2014/10'
                                               '/LT8BPF20141012002432_20141012020301.01'),
                'bpf_oli':
                    type.AncillaryMetadata(name='LO8BPF20141012002825_20141012011100.01',
                                           uri='/eoancillarydata/sensor-specific/LANDSAT8/BiasParameterFile/2014/10'
                                               '/LT8BPF20141012002432_20141012020301.01'),
                'rlut':
                    type.AncillaryMetadata(name='L8RLUT20130211_20431231v09.h5')
            },
            source_datasets={'raw': _build_ls8_raw()}
        )
    )


def _reset_runtime_id():
    """
    Regenerate the runtime id to simulate creation on different hosts/days.
    :return:
    """
    type._RUNTIME_ID = uuid.uuid1()


def _build_ls7_wofs():
    return type.DatasetMetadata(
        ga_label='LS7_ETM_WATER_140_-027_2013-07-24T00-32-27.952897',
        product_type='GAWATER',
        size_bytes=616 * 1024,
        platform=type.PlatformMetadata(code='LS7'),
        instrument=type.InstrumentMetadata(
            name='ETM',
            type_='Multi-Spectral'
        ),
        format_=type.FormatMetadata('GeoTIFF', version=1),
        extent=type.ExtentMetadata(
            reference_system='WGS84',
            coord=type.Polygon(
                ul=type.Coord(140.0000000, -26.0000000),
                ll=type.Coord(140.0000000, -27.0000000),
                ur=type.Coord(141.0000000, -26.0000000),
                lr=type.Coord(141.0000000, -27.0000000)
            ),

            # TODO: Should we store the center coordinate?

            from_dt=dateutil.parser.parse('2013-07-24 00:32:27.952897'),
            to_dt=dateutil.parser.parse('2013-07-24 00:33:15.899670')
        ),
        grid_spatial=type.GridSpatialMetadata(
            dimensions=[
                type.DimensionMetadata(name='x', resolution=27.1030749476, size=4000),
                type.DimensionMetadata(name='y', resolution=27.1030749476, size=4000)
            ],
            # TODO: Should WOfS have tile coordinates here?
            # georectified=type.GeoRectifiedSpacialMetadata(
            # geo_ref_points=Polygon(
            # ul=type.Point(255012.500, 7229987.500),
            # ur=type.Point(497012.500, 7229987.500),
            # ll=type.Point(255012.500, 7019987.500),
            # lr=type.Point(497012.500, 7229987.500)
            # ),
            # checkpoint_availability=0,
            # datum='GDA94',
            #     ellipsoid='GRS80',
            #     point_in_pixel='UL',
            #     projection='UTM',
            #     zone=-54
            # )
        ),
        image=type.ImageMetadata(
            satellite_ref_point_start=type.Point(98, 78),
            satellite_ref_point_end=type.Point(98, 79),
            cloud_cover_percentage=0.76494375,
            cloud_cover_details='122391 count',

            sun_elevation=33.0061002772,
            sun_azimuth=38.2433049177,

            bands={
                'W': type.BandMetadata(
                    path=Path('LS7_ETM_WATER_140_-027_2013-07-24T00-32-27.952897.tif'),
                    checksum_md5='992e0cdab6e64c5834b24284089fd08b'
                    # TODO: Nodata value?
                )
            }
        ),
        lineage=type.LineageMetadata(
            algorithm=type.AlgorithmMetadata(name='WOfS', version='1.3', parameters={}),
            machine=type.MachineMetadata(),
            source_datasets={
                # TODO: LS7 dataset?
            }
        )
    )


def _build_ls8_nbar():
    _reset_runtime_id()
    nbar = type.DatasetMetadata(
        id_=uuid.UUID("249ae098-bd88-11e4-beaa-1040f381a756"),
        size_bytes=622208 * 1024,
        ga_label='LS8_OLI_TIRS_NBAR_P54_GANBAR01-015_101_078_20141012',
        product_type='GANBAR01',
        platform=type.PlatformMetadata(code='LANDSAT-8'),
        instrument=type.InstrumentMetadata(
            name='OLI_TIRS',
            type_="Multi-Spectral",
            operation_mode='PUSH-BROOM'
        ),
        # acquisition=type.AcquisitionMetadata(),
        format_=type.FormatMetadata(name='GeoTiff', version=1),
        extent=type.ExtentMetadata(
            reference_system='WGS84',
            coord=type.Polygon(
                ul=type.Coord(lat=-24.97,
                              lon=133.97969),
                ur=type.Coord(lat=-24.96826,
                              lon=136.24838),
                lr=type.Coord(lat=-26.96338,
                              lon=136.26962),
                ll=type.Coord(lat=-26.96528,
                              lon=133.96233)
            ),
            from_dt=dateutil.parser.parse("2014-10-12T00:55:54"),
            to_dt=dateutil.parser.parse("2014-10-12T00:56:18"),

        ),
        grid_spatial=type.GridSpatialMetadata(
            dimensions=[
                type.DimensionMetadata(name='sample', resolution=25.0, size=9161),
                type.DimensionMetadata(name='line', resolution=25.0, size=9161)
            ],
            projection=type.ProjectionMetadata(
                centre_point=type.Point(511512.500000, 7127487.500000),
                geo_ref_points=type.Polygon(
                    ul=type.Point(397012.5, 7237987.5),
                    ur=type.Point(626012.5, 7237987.5),
                    ll=type.Point(397012.5, 7016987.5),
                    lr=type.Point(626012.5, 7016987.5)
                ),
                datum='GDA94',
                ellipsoid='GRS80',
                point_in_pixel='UL',
                map_projection='UTM',
                resampling_option='CUBIC_CONVOLUTION',
                zone=-53
            )
        ),
        browse={
            'medium':
                type.BrowseMetadata(
                    path=Path('LS8_OLI_TIRS_NBAR_P54_GANBAR01-015_101_078_20141012.tif'),
                    file_type='image/jpg',
                    checksum_md5='bbb81e0bc01baf029a7c99323593f53c',
                    sample_pixel_resolution=219.75,
                    red_band=7,
                    green_band=5,
                    blue_band=2
                ),
            'full':
                type.BrowseMetadata(
                    path=Path('LS8_OLI_TIRS_NBAR_P54_GANBAR01-015_101_078_20141012_FR.tif'),
                    file_type='image/jpg',
                    checksum_md5='92a1716e4f9bb0773b0916c37f4a2e4f',
                    sample_pixel_resolution=25.0,
                    red_band=7,
                    green_band=5,
                    blue_band=2
                )
        },
        image=type.ImageMetadata(
            satellite_ref_point_start=type.Point(101, 78),
            cloud_cover_percentage=0.01,
            cloud_cover_details=None,

            # TODO: What are these two?
            viewing_incidence_angle_long_track=0,
            viewing_incidence_angle_x_track=0,


            bands=collections.OrderedDict([
                ('1', type.BandMetadata(
                    path=Path('product/LS8_OLI_TIRS_NBAR_P54_GANBAR01-015_101_078_20141012_B1.tif'),
                    checksum_md5='4cea161eb35c002452bdeaa3753a5e59'
                )),
                ('2', type.BandMetadata(
                    path=Path('product/LS8_OLI_TIRS_NBAR_P54_GANBAR01-015_101_078_20141012_B2.tif'),
                    checksum_md5='b5780462ecba5e9c43dc55b03dfdfd70'
                )),
                ('3', type.BandMetadata(
                    path=Path('product/LS8_OLI_TIRS_NBAR_P54_GANBAR01-015_101_078_20141012_B3.tif'),
                    checksum_md5='2a34cdff7db38a980172e2d17d9637be'
                )),
                ('4', type.BandMetadata(
                    path=Path('product/LS8_OLI_TIRS_NBAR_P54_GANBAR01-015_101_078_20141012_B4.tif'),
                    checksum_md5='aab7a8a7ce9f4e1f35641cd2c366e2ab'
                )),
                ('5', type.BandMetadata(
                    path=Path('product/LS8_OLI_TIRS_NBAR_P54_GANBAR01-015_101_078_20141012_B5.tif'),
                    checksum_md5='c6ce34bf51df96b88a2f03e37613430e'
                )),
                ('6', type.BandMetadata(
                    path=Path('product/LS8_OLI_TIRS_NBAR_P54_GANBAR01-015_101_078_20141012_B6.tif'),
                    checksum_md5='6a56906c8030e0555b250e460656f83b'
                )),
                ('7', type.BandMetadata(
                    path=Path('product/LS8_OLI_TIRS_NBAR_P54_GANBAR01-015_101_078_20141012_B7.tif'),
                    checksum_md5='3331a567e5402661296374ba028b93a7'
                ))
            ])
        ),
        lineage=type.LineageMetadata(
            algorithm=type.AlgorithmMetadata(
                name='GANBAR',
                version='3.2.1',
                parameters={
                }
            ),
            machine=type.MachineMetadata(),
            source_datasets={
                'ortho': _build_ls8_ortho()
            },
            ancillary=[
            ]
        )
    )
    return nbar


class PackageTypeTests(unittest.TestCase):

    def test_equivalence(self):

        ls8_raw = _build_ls8_raw()
        self.assertEqual(ls8_raw, ls8_raw, msg='RAW mismatch')

        self.assertNotEqual(ls8_raw, _build_ls7_wofs(), msg='Different datasets should not be equal')

        ls8_nbar = _build_ls8_nbar()
        self.assertEqual(ls8_nbar, ls8_nbar, msg='NBAR mismatch')


    def test_serialise(self):

        ls8_raw = _build_ls8_raw()

        # To YAML then from YAML. Is equal?
        self.assertEqual(ls8_raw, ls8_raw, msg='RAW mismatch')

        self.assertNotEqual(ls8_raw, _build_ls7_wofs(), msg='Different datasets should not be equal')

        ls8_nbar = _build_ls8_nbar()
        self.assertEqual(ls8_nbar, ls8_nbar, msg='NBAR mismatch')



if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    _serialise_to_file('nbar', _build_ls8_nbar())
    _serialise_to_file('wofs', _build_ls7_wofs())

    import doctest
    doctest.testmod(type)
    unittest.main()
