import re
import datetime
from pathlib import Path
import uuid

import click
import rasterio
from shapely.geometry.polygon import Polygon
from xml.etree import ElementTree

from eodatasets.serialise import write_yaml_from_dict
from eodatasets.prepare.utils import read_paths_from_file


MCD43A1_NS = uuid.UUID(hex='80dc431b-fc6c-4e6f-bf08-585eba1d8dc9')


def parse_xml(filepath: Path):
    """
    Extracts metadata attributes from the xml document distributed
    alongside the MCD43A1 tiles.
    """
    polygon = []

    root = ElementTree.parse(str(filepath)).getroot()

    for point in root.findall('*//SpatialDomainContainer/HorizontalSpatialDomainContainer/GPolygon/Boundary/'):
        polygon.append((
            float(point.find('PointLongitude').text),
            float(point.find('PointLatitude').text)
        ))

    granule_id = root.find('*//ECSDataGranule/LocalGranuleID').text
    instrument = root.find('*//Platform/Instrument/InstrumentShortName').text
    platform = root.find('*//Platform/PlatformShortName').text
    start_date = root.find('*//RangeDateTime/RangeBeginningDate').text
    start_time = root.find('*//RangeDateTime/RangeBeginningTime').text
    end_date = root.find('*//RangeDateTime/RangeEndingDate').text
    end_time = root.find('*//RangeDateTime/RangeEndingTime').text

    creation_dt = root.find('*//InsertTime').text

    return {
        'granule_id': granule_id,
        'instrument': instrument,
        'platform': platform,
        'from_dt': (
            datetime.datetime
            .strptime(start_date + ' ' + start_time, '%Y-%m-%d %H:%M:%S.%f')
            .replace(tzinfo=datetime.timezone.utc)
        ),
        'to_dt': (
            datetime.datetime
            .strptime(end_date + ' ' + end_time, '%Y-%m-%d %H:%M:%S.%f')
            .replace(tzinfo=datetime.timezone.utc)
        ),
        'creation_dt': (
            datetime.datetime
            .strptime(creation_dt, '%Y-%m-%d %H:%M:%S.%f')
            .replace(tzinfo=datetime.timezone.utc)
        ),
        'polygon': Polygon(polygon)
    }


def get_band_info(imagery_file: Path):
    """
    Summarises the available image bands for indexing into datacube
    Separate references are provided for each of the brdf parameter bands:
        volumetric (vol), isometric (iso) and geometric (geo)

    """
    band_info = {
        'bands': {},
    }
    with rasterio.open(imagery_file, 'r') as collection:
        for ds in collection.subdatasets:
            raster_params = re.match('(?P<fmt>HDF4_EOS:EOS_GRID):(?P<path>[^:]+):(?P<layer>.*)$', ds)
            if '_Quality_' in raster_params['layer']:
                name = raster_params['layer'].split(':')[-1]
                band_info['bands'][name] = {
                    'path': Path(raster_params['path']).name,
                    'layer': raster_params['layer']
                }
            else:
                name = raster_params['layer'].split(':')[-1]
                # BRDF parameter bands are isotropic, volumetric and geometric
                for idx, band_name in enumerate(['iso', 'vol', 'geo'], 1):
                    band_info['bands'][name + '_' + band_name] = {
                        'path': Path(raster_params['path']).name,
                        'layer': raster_params['layer'],
                        'band': idx
                    }
    return band_info


def get_bounds(polygon):
    """
    Returns the bounds of the polygon
    """
    min_lon, min_lat, max_lon, max_lat = \
        polygon.bounds

    return {
        'll': {
            'lat': min_lat,
            'lon': min_lon,
        },
        'lr': {
            'lat': min_lat,
            'lon': max_lon,
        },
        'ur': {
            'lat': max_lat,
            'lon': max_lon,
        },
        'ul': {
            'lat': max_lat,
            'lon': min_lon,
        }
    }


def rio_spatial_projection(src_dataset: str):
    """
    Returns the spatial projection from a rasterio readable dataset
    """
    with rasterio.open(src_dataset, 'r') as fd:
        return _spatial_projection(fd.shape, fd.transform, fd.crs.wkt)


def _spatial_projection(shape, transform, spatial_reference):
    """
    Returns spatial bounds in the provided spatial reference
    """

    def _get_point(x, y):
        x, y = (x, y) * transform
        return {'x': x, 'y': y}

    return {
        'geo_ref_points': {
            'ul': _get_point(0, 0),
            'll': _get_point(0, shape[0]),
            'ur': _get_point(shape[1], 0),
            'lr': _get_point(shape[1], shape[0])
        },
        'spatial_reference': spatial_reference
    }


def process_datasets(input_path: Path, xml_file: Path):
    """
    Generates a metadata document for each tile provided,
    requires a path to the input tile (hdf) and the
    corresponding xml document describing the dataset.
    """
    band_info = get_band_info(input_path)
    xml_md = parse_xml(xml_file)

    md = {}
    md['id'] = uuid.uuid5(MCD43A1_NS, xml_md['granule_id'])
    md['label'] = xml_md.get('granule_id', input_path.name)
    md['creation_dt'] = xml_md['creation_dt'].isoformat()
    md['extent'] = {
        'from_dt': xml_md['from_dt'].isoformat(),
        'to_dt': xml_md['to_dt'].isoformat(),
        'coord': get_bounds(xml_md['polygon'])
    }
    md['format'] = {'name': 'HDF4_EOS:EOS_GRID'}
    md['grid_spatial'] = {
        'projection': rio_spatial_projection(
            'HDF4_EOS:EOS_GRID:{}:MOD_Grid_BRDF:BRDF_Albedo_Parameters_Band1'.format(input_path)
        )
    }
    md['image'] = band_info
    md['product_name'] = 'mcd43a1'
    md['product_type'] = 'auxiliary'
    md['sources'] = {}
    md['instrument'] = {
        'name': xml_md['instrument']
    }
    md['platform'] = {
        'code': xml_md['platform']
    }

    return [md]


def _process_datasets(output_dir, datasets, checksum):
    """
    Wrapper function for processing multiple datasets
    """
    for dataset in datasets:
        doc = process_datasets(dataset, Path(str(dataset) + '.xml'))
        outfile = output_dir / (dataset.stem + '.ga-md.yaml')
        write_yaml_from_dict(doc, outfile)


@click.command(help="""\b
        Prepare MODIS MCD43A1 tiles for indexing into a Data Cube.
        This prepare script supports the HDF4_EOS:EOS_GRID datasets
            with associated xml documents

        Example usage: yourscript.py --output [directory] input_file1 input_file2""")
@click.option('--output', 'output_dir', help="Write datasets into this directory",
              type=click.Path(exists=False, writable=True, dir_okay=True))
@click.argument('datasets',
                type=click.Path(exists=True, readable=True, writable=False),
                nargs=-1)
@click.option('--checksum/--no-checksum', help="Checksum the input dataset to confirm match", default=False)
@click.option('-f', 'dataset_listing_files',
              type=click.Path(exists=True, readable=True, writable=False),
              help="file containing a list of input paths (one per line)", multiple=True)
def main(output_dir, datasets, checksum, dataset_listing_files):
    datasets = [Path(p) for p in datasets]
    for listing_file in dataset_listing_files:
        datasets.extend(
            read_paths_from_file(Path(listing_file))
        )

    return _process_datasets(Path(output_dir), datasets, checksum)


if __name__ == '__main__':
    main()
