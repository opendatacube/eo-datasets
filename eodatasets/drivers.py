
from eodatasets.metadata import mdf, mtl, adsfolder, rccfile, passinfo, image as md_image
from eodatasets import type as ptype
import logging
from pathlib import Path

_LOG = logging.getLogger(__name__)


def find_file(path, file_pattern):
    # Crude but effective. TODO: multiple/no result handling.
    return path.glob(file_pattern).next()


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

    def expected_source(self):
        """
        Expected source dataset (driver).
        :rtype: DatasetDriver
        """
        raise NotImplementedError()

    def file_is_pertinent(self, file_path):
        """
        Should the given file be included in this dataset package?
        :type file_path: Path
        :rtype: bool
        """
        return True

    def browse_image_bands(self, d):
        """
        Band ids for for an rgb browse image.
        :type d: ptype.DatasetMetadata
        :rtype (str, str, str)
        """
        # Defaults for satellites. Different products may override this.
        # These values come from the ARG25 spec.
        _SATELLITE_BROWSE_BANDS = {
            'LANDSAT_5': ('7', '4', '1'),
            'LANDSAT_7': ('7', '4', '1'),
            'LANDSAT_8': ('7', '5', '2'),
            }
        browse_bands = _SATELLITE_BROWSE_BANDS.get(d.platform.code)
        if not browse_bands:
            raise ValueError('Unknown browse bands for satellite %s' % d.platform.code)

        return browse_bands


class RawDriver(DatasetDriver):
    def get_id(self):
        return 'raw'

    def expected_source(self):
        # Raw dataset has no source.
        return None

    def fill_metadata(self, dataset, path):
        dataset = adsfolder.extract_md(dataset, path)
        dataset = rccfile.extract_md(dataset, path)
        dataset = mdf.extract_md(dataset, path)
        dataset = passinfo.extract_md(dataset, path)

        # TODO: Antenna coords for groundstation? Heading?
        # TODO: Bands? (or eg. I/Q files?)
        return dataset


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
        return mtl.populate_from_mtl(d, mtl_path)


class NbarDriver(DatasetDriver):
    def __init__(self, subset_name):
        # Subset is typically "brdf" or "terrain" -- which NBAR portion to package.
        self.subset_name = subset_name

    def get_id(self):
        return 'nbar_{}'.format(self.subset_name)

    def expected_source(self):
        return OrthoDriver()

    def file_is_pertinent(self, file_path):
        return file_path.name.startswith('reflectance_{}'.format(self.subset_name))

    @staticmethod
    def _find_nbar_bands(package_directory):
        bands = {}
        for band in Path(package_directory).glob('*.tif'):
            band_number = band.stem.split('_')[-1]
            bands[band_number] = ptype.BandMetadata(path=band.absolute(), number=band_number)
        return bands

    def fill_metadata(self, dataset, path):
        """
        :type dataset: ptype.DatasetMetadata
        :type path: Path
        :rtype: ptype.DatasetMetadata
        """
        # TODO: Detect
        dataset.platform = ptype.PlatformMetadata(code='LANDSAT_8')
        dataset.instrument = ptype.InstrumentMetadata(name='OLI_TIRS')
        # d.product_type
        if not dataset.image:
            dataset.image = ptype.ImageMetadata(bands={})

        dataset.image.bands.update(self._find_nbar_bands(path))
        md_image.populate_from_image_metadata(dataset)
        return dataset


PACKAGE_DRIVERS = {
    'raw': RawDriver(),
    'ortho': OrthoDriver(),
    'nbar_brdf': NbarDriver('brdf'),
    'nbar_terrain': NbarDriver('terrain')
}
