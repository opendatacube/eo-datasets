# coding=utf-8
from __future__ import absolute_import
import logging
import re
import string
import datetime

from pathlib import Path

from eodatasets.metadata import mdf, mtl, adsfolder, rccfile, \
    passinfo, pds, npphdf5, image as md_image
from eodatasets.metadata import _GROUNDSTATION_LIST
from eodatasets import type as ptype, metadata

_LOG = logging.getLogger(__name__)


def find_file(path, file_pattern):
    # Crude but effective. TODO: multiple/no result handling.
    return next(path.glob(file_pattern))


class DatasetDriver(object):
    def get_id(self):
        """
        A short identifier for this type of dataset.

        eg. 'ortho'

        :rtype: str
        """
        raise NotImplementedError()

    def fill_metadata(self, dataset, path):
        """
        Populate the given dataset metadata from the path.

        :type dataset: ptype.DatasetMetadata
        :type path: Path
        """
        raise NotImplementedError()

    def get_ga_label(self, dataset):
        """
        Generate the GA Label ("dataset id") for a dataset.
        :type dataset:  ptype.DatasetMetadata
        :rtype: str
        """
        raise NotImplementedError()

    def expected_source(self):
        """
        Expected source dataset (driver).
        :rtype: DatasetDriver
        """
        raise NotImplementedError()

    def translate_path(self, dataset, file_path):
        """
        Translate an input filename if desired.

        Returning None will exclude the file from the output package.

        :type dataset: ptype.DatasetMetadata
        :type file_path: Path
        :rtype: Path

        >>> # Test default behaviour: all files included unchanged, suffix is lowercase.
        >>> DatasetDriver().translate_path(None, Path('/tmp/fake_path.TXT'))
        PosixPath('/tmp/fake_path.TXT')
        >>> DatasetDriver().translate_path(None, Path('/tmp/passinfo'))
        PosixPath('/tmp/passinfo')
        """
        # Default: no modification to filename.
        return file_path

    def to_band(self, dataset, path):
        """
        Create a band definition for the given output file.

        Return None if file should not be included as a band
        (the file will still be included in the package).

        :type dataset: ptype.DatasetMetadata
        :type path: Path
        :param path: The filename of the input file.
        :rtype: ptype.BandMetadata or None
        """
        raise NotImplementedError()

    def browse_image_bands(self, d):
        """
        Band ids for for an rgb browse image.
        :type d: ptype.DatasetMetadata
        :rtype (str, str, str)
        """
        # Defaults for satellites. Different products may override this.
        # These values come from the ARG25 spec.
        _satellite_browse_bands = {
            'LANDSAT_5': ('7', '4', '1'),
            'LANDSAT_7': ('7', '4', '1'),
            'LANDSAT_8': ('7', '5', '2'),
        }
        browse_bands = _satellite_browse_bands.get(d.platform.code)
        if not browse_bands:
            raise ValueError('Unknown browse bands for satellite %s' % d.platform.code)

        return browse_bands

    def __eq__(self, other):
        if self.__class__ != other.__class__:
            return False

        return self.__dict__ == other.__dict__


def get_groundstation_code(gsi):
    """
    Translate a GSI code into an EODS domain code.

    Domain codes are used in dataset_ids.

    It will also translate common gsi aliases if needed.

    :type gsi: str
    :rtype: str

    >>> get_groundstation_code('ASA')
    '002'
    >>> get_groundstation_code('HOA')
    '011'
    >>> # Aliases should work too.
    >>> get_groundstation_code('ALSP')
    '002'
    """
    groundstation = metadata.get_groundstation(gsi)
    if not groundstation:
        return None

    return groundstation.eods_domain_code


def _format_path_row(start_point, end_point=None):
    """
    Format path-row for display in a dataset id.

    :type start_point: ptype.Point or None
    :type end_point: ptype.Point or None
    :rtype: (str, str)

    >>> _format_path_row(ptype.Point(78, 132))
    ('078', '132')
    >>> _format_path_row(ptype.Point(12, 4))
    ('012', '004')
    >>> # Show the range of rows
    >>> _format_path_row(ptype.Point(78, 78), end_point=ptype.Point(78, 80))
    ('078', '078-080')
    >>> # Identical rows: don't show a range
    >>> _format_path_row(ptype.Point(78, 132), end_point=ptype.Point(78, 132))
    ('078', '132')
    >>> # This is odd behaviour, but we're doing it for consistency with the old codebases.
    >>> # Lack of path/rows are represented as single-digit zeros.
    >>> _format_path_row(None)
    ('0', '0')
    >>> _format_path_row(ptype.Point(None, None))
    ('0', '0')
    """
    if start_point is None:
        return '0', '0'

    def _format_val(val):
        if val:
            return '%03d' % val
        else:
            return '0'

    path = _format_val(start_point.x)
    rows = _format_val(start_point.y)

    # Add ending row if different.
    if end_point and start_point.y != end_point.y:
        rows += '-' + _format_val(end_point.y)

    return path, rows


def _get_process_code(dataset):
    """
    :type dataset: ptype.DatasetMetadata
    :return:
    """
    level = dataset.product_level

    if level:
        level = level.upper()

    orientation = None
    if dataset.grid_spatial and dataset.grid_spatial.projection:
        orientation = dataset.grid_spatial.projection.orientation

    if level == 'L1T':
        return 'OTH', 'P51'

    if orientation == 'NORTH_UP':
        if level == 'L1G':
            return 'SYS', 'P31'
        if level == 'L1GT':
            return 'OTH', 'P41'

    # Path
    if orientation in ('NOMINAL', 'NOM'):
        return 'SYS', 'P11'

    if dataset.ga_level == 'P00':
        return 'satellite_telemetry_data', 'P00'

    _LOG.warn('No process code mapped for level/orientation: %r, %r', level, orientation)
    return None, None


def _get_short_satellite_code(platform_code):
    """
    Get shortened form of satellite, as used in GA Dataset IDs. (eg. 'LS7')
    :param platform_code:
    :return:

    >>> _get_short_satellite_code('LANDSAT_8')
    'LS8'
    >>> _get_short_satellite_code('LANDSAT_5')
    'LS5'
    >>> _get_short_satellite_code('LANDSAT_7')
    'LS7'
    >>> _get_short_satellite_code('AQUA')
    'AQUA'
    >>> _get_short_satellite_code('TERRA')
    'TERRA'
    >>> _get_short_satellite_code('Invalid')
    Traceback (most recent call last):
    ...
    ValueError: Unknown platform code 'Invalid'
    """
    if platform_code.startswith('LANDSAT_'):
        return 'LS' + platform_code.split('_')[-1]

    if platform_code in ('AQUA', 'TERRA', 'NPP'):
        return platform_code

    raise ValueError('Unknown platform code %r' % platform_code)


def _fill_dataset_label(dataset, format_str, **additionals):

    path, row = _format_path_row(
        start_point=dataset.image.satellite_ref_point_start if dataset.image else None,
        end_point=dataset.image.satellite_ref_point_end if dataset.image else None
    )

    def _format_dt(d):
        if not d:
            return None
        return d.strftime("%Y%m%dT%H%M%S")

    def _format_day(dataset_):
        day = (dataset_.extent and dataset_.extent.center_dt) or \
              (dataset_.acquisition and dataset_.acquisition.aos)
        return day.strftime('%Y%m%d')

    level, ga_level = _get_process_code(dataset)

    if not ga_level:
        ga_level = dataset.ga_level

    station_code = None
    start = None
    end = None
    orbit = None
    if dataset.acquisition:
        if dataset.acquisition.groundstation:
            station_code = get_groundstation_code(dataset.acquisition.groundstation.code)
        if dataset.acquisition.aos:
            start = _format_dt(dataset.acquisition.aos)
        if dataset.acquisition.los:
            end = _format_dt(dataset.acquisition.los)

        orbit = dataset.acquisition.platform_orbit

    formatted_params = {
        'satnumber': _get_short_satellite_code(dataset.platform.code),
        'sensor': _remove_chars(string.punctuation, dataset.instrument.name),
        'format': dataset.format_.name.upper(),
        'level': level,
        'galevel': ga_level,
        'usgs': dataset.usgs,
        'path': path,
        'rows': row,
        'orbit': orbit,
        'stationcode': station_code,
        'startdt': start,
        'enddt': end,
        'rmsstring': dataset.rms_string,
        'day': _format_day(dataset),
    }
    formatted_params.update(additionals)
    return format_str.format(**formatted_params)


def _remove_chars(chars, s):
    """
    :param chars: string of characters to remove.
    :param s: input string
    :rtype: str

    >>> _remove_chars(string.punctuation, 'OLI_TIRS+')
    'OLITIRS'
    >>> _remove_chars('_', 'A_B_C')
    'ABC'
    >>> _remove_chars(string.punctuation, None)
    """
    if not s:
        return s
    return re.sub('[' + re.escape(''.join(chars)) + ']', '', s)


class RawDriver(DatasetDriver):
    def get_id(self):
        return 'satellite_telemetry_data'

    def expected_source(self):
        # Raw dataset has no source.
        return None

    def get_ga_label(self, dataset):
        """
        :type dataset: ptype.DatasetMetadata
        :rtype: str
        """
        _LOG.info('Labelling dataset: %r', dataset)
        # Examples for each Landsat raw:
        # 'LS8_OLITIRS_STD-MD_P00_LC81160740742015089ASA00_116_074-084_20150330T022553Z20150330T022657'
        # 'LS7_ETM_STD-RCC_P00_L7ET2005007020028ASA123_0_0_20050107T020028Z20050107T020719'
        # 'LS5_TM_STD-RCC_P00_L5TB2005152015110ASA111_0_0_20050601T015110Z20050107T020719'

        # Raw datasets have a strange extra column derived from ADS folder names.
        # It's an interval id for Landsat products, and mixtures of orbits/rms-strings otherwise.
        folder_identifier = []
        if dataset.usgs:
            folder_identifier.append(dataset.usgs.interval_id)
        else:
            if dataset.acquisition.platform_orbit:
                folder_identifier.append(str(dataset.acquisition.platform_orbit))
            if dataset.rms_string:
                folder_identifier.append(dataset.rms_string)
            if dataset.acquisition.groundstation:
                folder_identifier.append(dataset.acquisition.groundstation.code)

        return _fill_dataset_label(
            dataset,
            '{satnumber}_{sensor}_STD-{format}_P00_{folderident}_{path}_{rows}_{startdt}Z{enddt}',
            folderident='.'.join(folder_identifier)
        )

    def fill_metadata(self, dataset, path):
        dataset = adsfolder.extract_md(dataset, path)
        dataset = rccfile.extract_md(dataset, path)
        dataset = mdf.extract_md(dataset, path)
        dataset = passinfo.extract_md(dataset, path)
        dataset = pds.extract_md(dataset, path)
        dataset = npphdf5.extract_md(dataset, path)

        # TODO: Antenna coords for groundstation? Heading?
        # TODO: Bands? (or eg. I/Q files?)
        return dataset

    def to_band(self, dataset, path):
        # We don't record any bands for a raw dataset (yet?)
        return None


class OrthoDriver(DatasetDriver):
    def get_id(self):
        return 'ortho'

    def expected_source(self):
        return RawDriver()

    def fill_metadata(self, d, package_directory):
        """
        :type package_directory: Path
        :type d: ptype.DatasetMetadata
        :return:
        """
        mtl_path = find_file(package_directory, '*_MTL.txt')
        _LOG.info('Reading MTL %r', mtl_path)

        d = mtl.populate_from_mtl(d, mtl_path)

        return d

    def translate_path(self, dataset, file_path):
        """
        Exclude .aux.xml paths.
        :type dataset: ptype.DatasetMetadata
        :type file_path: Path
        :rtype: Path | None

        >>> OrthoDriver().translate_path(None, Path('something.TIF'))
        PosixPath('something.TIF')
        >>> OrthoDriver().translate_path(None, Path('something.tif'))
        PosixPath('something.tif')
        >>> OrthoDriver().translate_path(None, Path('something.TIF.aux.xml'))
        """
        # Inherit default behaviour
        file_path = super(OrthoDriver, self).translate_path(dataset, file_path)

        if not file_path:
            return file_path

        if file_path.name.endswith('.aux.xml'):
            return None

        return file_path

    def to_band(self, dataset, path):
        """
        :type dataset: ptype.DatasetMetadata
        :type final_path: pathlib.Path
        :rtype: ptype.BandMetadata

        >>> OrthoDriver().to_band(None, Path('/tmp/out/LT51030782005002ASA00_B3.TIF'))
        BandMetadata(path=PosixPath('/tmp/out/LT51030782005002ASA00_B3.TIF'), number='3')
        >>> OrthoDriver().to_band(None, Path('/tmp/out/LC81090852015088LGN00_B10.tif'))
        BandMetadata(path=PosixPath('/tmp/out/LC81090852015088LGN00_B10.tif'), number='10')
        >>> OrthoDriver().to_band(None, Path('/data/output/LE70900782007292ASA00_B6_VCID_2.TIF'))
        BandMetadata(path=PosixPath('/data/output/LE70900782007292ASA00_B6_VCID_2.TIF'), number='6_vcid_2')
        >>> # No bands for non-tiff files.
        >>> OrthoDriver().to_band(None, Path('/tmp/out/LC81090852015088LGN00_MTL.txt'))
        >>> OrthoDriver().to_band(None, Path('/tmp/out/passinfo'))
        """
        if path.suffix.lower() != '.tif':
            return None

        name = path.stem.lower()
        # Images end in a band number (eg '_B12.tif'). Extract it.
        position = name.rfind('_b')
        if position == -1:
            raise ValueError('Unexpected tif image in ortho: %r' % path)
        band_number = name[position+2:]

        return ptype.BandMetadata(path=path, number=band_number)

    def get_ga_label(self, dataset):
        # Examples:
        # "LS8_OLITIRS_OTH_P41_GALPGS01-002_101_078_20141012"
        # "LS7_ETM_SYS_P31_GALPGS01-002_114_73_20050107"
        #     "LS5_TM_OTH_P51_GALPGS01-002_113_063_20050601"

        return _fill_dataset_label(
            dataset,
            '{satnumber}_{sensor}_{level}_{galevel}_GALPGS01-{stationcode}_{path}_{rows}_{day}'
        )


def borrow_single_sourced_fields(dataset, source_dataset):
    """
    Copy common metadata fields from a source dataset.

    The fields copied assume a non-composite dataset with only one source.

    :type dataset: ptype.DatasetMetadata
    :type source_dataset: ptype.DatasetMetadata
    :rtype: ptype.DatasetMetadata
    """

    if not dataset.image:
        dataset.image = ptype.ImageMetadata(bands={})
    if not dataset.extent:
        dataset.extent = ptype.ExtentMetadata()
    dataset.extent.steal_fields_from(source_dataset.extent)
    dataset.platform = source_dataset.platform
    dataset.instrument = source_dataset.instrument
    if not dataset.acquisition:
        dataset.acquisition = ptype.AcquisitionMetadata()
    dataset.acquisition.steal_fields_from(source_dataset.acquisition)
    if not dataset.image.satellite_ref_point_start:
        dataset.image.satellite_ref_point_start = source_dataset.image.satellite_ref_point_start
        dataset.image.satellite_ref_point_end = source_dataset.image.satellite_ref_point_end

    return dataset


class NbarDriver(DatasetDriver):
    def __init__(self, subset_name):
        # Subset is typically "brdf" or "terrain" -- which NBAR portion to package.
        self.subset_name = subset_name

    def get_id(self):
        return 'nbar_{}'.format(self.subset_name)

    def expected_source(self):
        return OrthoDriver()

    def _file_is_pertinent(self, file_path):
        return file_path.name.startswith('reflectance_{}'.format(self.subset_name))

    def get_ga_label(self, dataset):
        # Example: LS8_OLITIRS_NBAR_P51_GALPGS01-032_090_085_20140115

        codes = {
            'terrain': 'TNBAR',
            'brdf': 'NBAR'
        }

        nbar_type = codes.get(self.subset_name)
        if not nbar_type:
            raise ValueError('Unknown nbar subset type: %r. Expected one of %r' % (self.subset_name, codes.keys()))

        return _fill_dataset_label(
            dataset,
            '{satnumber}_{sensor}_{nbartype}_{galevel}_GALPGS01-{stationcode}_{path}_{rows}_{day}',
            nbartype=nbar_type
        )

    def _read_band_number(self, file_path):
        """
        :type file_path: Path
        :return:
        >>> NbarDriver('brdf')._read_band_number(Path('reflectance_brdf_2.bin'))
        '2'
        >>> NbarDriver('brdf')._read_band_number(Path('reflectance_terrain_7.bin'))
        '7'
        >>> p = Path('/tmp/something/LS8_OLITIRS_NBAR_P54_GALPGS01-002_112_079_20140126_B4.tif')
        >>> NbarDriver('brdf')._read_band_number(p)
        '4'
        """
        number = file_path.stem.split('_')[-1].lower()

        if number.startswith('b'):
            return number[1:]
        else:
            return number

    def translate_path(self, dataset, file_path):
        """
        :type dataset: ptype.DatasetMetadata
        :type file_path: Path
        :rtype: Path
        >>> from tests.metadata.mtl.test_ls8 import EXPECTED_OUT as ls8_dataset
        >>> NbarDriver('terrain').translate_path(ls8_dataset, Path('reflectance_terrain_7.bin'))
        PosixPath('LS8_OLITIRS_TNBAR_P51_GALPGS01-032_101_078_20141012_B7.tif')
        >>> # Should return None, as this is a BRDF driver instance.
        >>> NbarDriver('brdf').translate_path(ls8_dataset, Path('reflectance_terrain_7.bin'))
        """
        # Skip hidden files and envi headers. (envi files are converted to tif during copy)
        if file_path.suffix != '.bin':
            return None

        if not self._file_is_pertinent(file_path):
            return None

        ga_label = self.get_ga_label(dataset)
        band_number = self._read_band_number(file_path)

        return file_path.with_name('%s_B%s.tif' % (ga_label, band_number))

    def to_band(self, dataset, path):
        """
        :type dataset: ptype.DatasetMetadata
        :type path: Path
        :rtype: ptype.BandMetadata

        >>> p = Path('/tmp/something/reflectance_terrain_3.bin')
        >>> NbarDriver('terrain').to_band(None, p).number
        '3'
        >>> NbarDriver('terrain').to_band(None, p).path
        PosixPath('/tmp/something/reflectance_terrain_3.bin')
        >>> p = Path('/tmp/something/LS8_OLITIRS_NBAR_P54_GALPGS01-002_112_079_20140126_B4.tif')
        >>> NbarDriver('terrain').to_band(None, p).number
        '4'
        """
        return ptype.BandMetadata(path=path, number=self._read_band_number(path))

    def fill_metadata(self, dataset, path):
        """
        :type dataset: ptype.DatasetMetadata
        :type path: Path
        :rtype: ptype.DatasetMetadata
        """

        if not dataset.image:
            dataset.image = ptype.ImageMetadata(bands={})

        # Copy relevant fields from source ortho.
        if 'ortho' in dataset.lineage.source_datasets:
            ortho = dataset.lineage.source_datasets['ortho']
            borrow_single_sourced_fields(dataset, ortho)

        # All NBARs are P54. (source: Lan Wei)
        dataset.ga_level = 'P54'

        dataset.format_ = ptype.FormatMetadata('GeoTIFF')

        md_image.populate_from_image_metadata(dataset)

        return dataset


class EODSDriver(DatasetDriver):
    def get_id(self):
        return "EODS"

    def expected_source(self):
        """
        Expected source dataset (driver).
        :rtype: DatasetDriver
        """
        return None

    def get_ga_label(self, dataset):
        return dataset.ga_label

    def to_band(self, dataset, path):
        """
        :type dataset: ptype.DatasetMetadata
        :type final_path: pathlib.Path
        :rtype: ptype.BandMetadata
        """
        if path.suffix.lower() != '.tif':
            return None

        name = path.stem
        # Images end in a band number (eg '_B12.tif'). Extract it.
        position = name.rfind('_')
        if position == -1:
            raise ValueError('Unexpected tif image in eods: %r' % path)
        if re.match(r"[Bb]\d+", name[position+1:]):
            band_number = name[position+2:]
        else:
            band_number = name[position+1:]

        return ptype.BandMetadata(path=path, number=band_number)

    def fill_metadata(self, dataset, path):
        """
        :type dataset: ptype.DatasetMetadata
        :type path: Path
        :rtype: ptype.DatasetMetadata
        """

        fields = re.match(
            (
                r"(?P<vehicle>LS[578])"
                r"_(?P<instrument>OLI_TIRS|OLI|TIRS|TM|ETM)"
                r"_(?P<type>NBAR|PQ|FC)"
                r"_(?P<level>[^-_]*)"
                r"_(?P<product>[^-_]*)"
                r"-(?P<groundstation>[0-9]{3})"
                r"_(?P<path>[0-9]{3})"
                r"_(?P<row>[0-9]{3})"
                r"_(?P<date>[12][0-9]{7})"
                r"(_(?P<version>[0-9]+))?"
                "$"
            ),
            path.stem).groupdict()

        dataset.ga_level = fields["level"]
        dataset.ga_label = path.stem
        dataset.format_ = ptype.FormatMetadata(name='GeoTiff')

        if not dataset.platform:
            dataset.platform = ptype.PlatformMetadata()
        dataset.platform.code = "LANDSAT_" + fields["vehicle"][2]

        if not dataset.instrument:
            dataset.instrument = ptype.InstrumentMetadata()
        dataset.instrument.name = fields["instrument"]

        if not dataset.image:
            dataset.image = ptype.ImageMetadata(bands={})
        dataset.image.satellite_ref_point_start = ptype.Point(int(fields["path"]), int(fields["row"]))
        dataset.image.satellite_ref_point_end = ptype.Point(int(fields["path"]), int(fields["row"]))

        for image_path in path.joinpath("scene01").iterdir():
            band = self.to_band(dataset, image_path)
            if band:
                dataset.image.bands[band.number] = band
        md_image.populate_from_image_metadata(dataset)

        if not dataset.acquisition:
            dataset.acquisition = ptype.AcquisitionMetadata()

        for _station in _GROUNDSTATION_LIST:
            if _station["eods_domain_code"] == fields["groundstation"]:
                dataset.acquisition.groundstation = ptype.GroundstationMetadata(code=_station["code"])
                break

        if not dataset.extent:
            dataset.extent = ptype.ExtentMetadata()

        def els2date(els, fmt):
            if not els:
                return None
            return datetime.datetime.strptime(els[0].text, fmt)

        import xml.etree.cElementTree as etree
        doc = etree.parse(str(path.joinpath('metadata.xml')))
        aos = els2date(doc.findall("./ACQUISITIONINFORMATION/EVENT/AOS"), "%Y%m%dT%H:%M:%S")
        los = els2date(doc.findall("./ACQUISITIONINFORMATION/EVENT/LOS"), "%Y%m%dT%H:%M:%S")
        start_time = els2date(doc.findall("./EXEXTENT/TEMPORALEXTENTFROM"), "%Y%m%d %H:%M:%S")
        end_time = els2date(doc.findall("./EXEXTENT/TEMPORALEXTENTTO"), "%Y%m%d %H:%M:%S")

        # check if the dates in the metadata file are at least as accurate as what we have
        filename_time = datetime.datetime.strptime(fields["date"], "%Y%m%d")
        if abs(start_time - filename_time).days == 0:
            dataset.acquisition.aos = aos
            dataset.acquisition.los = los
            dataset.extent.center_dt = start_time + (end_time - start_time)/2
        else:
            dataset.acquisition.aos = filename_time.date()
            dataset.acquisition.los = dataset.acquisition.aos
            if dataset.extent and not dataset.extent.center_dt:
                dataset.extent.center_dt = dataset.acquisition.aos

        return dataset


class PqaDriver(DatasetDriver):

    def get_id(self):
        return 'pqa'

    def expected_source(self):
        return NbarDriver('brdf')

    def get_ga_label(self, dataset):
        # Eg. 'LS8_OLI_TIRS_PQ_P55_GAPQ01-032_090_081_20140726'
        return _fill_dataset_label(
            dataset,
            '{satnumber}_{sensor}_PQ_{galevel}_GAPQ01-{stationcode}_{path}_{rows}_{day}',
        )

    def fill_metadata(self, dataset, path):
        """
        :type dataset: ptype.DatasetMetadata
        :type path: Path
        :rtype: ptype.DatasetMetadata
        """
        dataset.ga_level = 'P55'

        # Copy relevant fields from source nbar.
        if 'nbar_brdf' in dataset.lineage.source_datasets:
            ortho = dataset.lineage.source_datasets['nbar_brdf']
            borrow_single_sourced_fields(dataset, ortho)

        dataset.format_ = ptype.FormatMetadata('GeoTIFF')

        return dataset

    def translate_path(self, dataset, file_path):
        """
        :type dataset: ptype.DatasetMetadata
        :type file_path: pathlib.Path
        :return:
        """
        # Tif file will be renamed to contain the ga_label.
        suffix = file_path.suffix.lower()
        if '.tif' == suffix:
            ga_label = self.get_ga_label(dataset)
            return file_path.with_name(ga_label+suffix)
        else:
            # All other files are kept in the package (log files etc, if any).
            return file_path

    def to_band(self, dataset, path):
        """
        :type dataset: ptype.DatasetMetadata
        :type path: Path
        :param path: The filename of the input file.
        :rtype: ptype.BandMetadata
        """
        if path.suffix != '.tif':
            return None

        return ptype.BandMetadata(path=path, number='pqa')

    def browse_image_bands(self, d):
        return 'pqa',


PACKAGE_DRIVERS = {
    'raw': RawDriver(),
    'pqa': PqaDriver(),
    'ortho': OrthoDriver(),
    'nbar_brdf': NbarDriver('brdf'),
    'nbar_terrain': NbarDriver('terrain'),
    'eods': EODSDriver()
}
