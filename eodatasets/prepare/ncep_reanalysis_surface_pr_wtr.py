"""
Create index files from the command-line
"""

import datetime
import uuid
import urllib.parse
from pathlib import Path

import click
import rasterio
import rasterio.crs

from eodatasets.serialise import write_yaml_from_dict
from .utils import read_paths_from_file


NOAA_WATER_VAPOUR_NS = uuid.UUID(hex='857bd048-8c86-4670-a2b4-5dbea26d7692')
NOAA_DESCRIPTION = ''


def get_coords(collection, xkey='lon', ykey='lat'):
    """
    transforms the bounds of a collection into an
    encompassing bounding box. Keys for x and y are
    customisable
    """
    return {
        'll': {
            ykey: collection.bounds.bottom,
            xkey: collection.bounds.left
        },
        'lr': {
            ykey: collection.bounds.bottom,
            xkey: collection.bounds.right
        },
        'ul': {
            ykey: collection.bounds.top,
            xkey: collection.bounds.left
        },
        'ur': {
            ykey: collection.bounds.top,
            xkey: collection.bounds.right
        }
    }


def get_uuid(collection, idx):
    """
    Returns a deterministic uuid based on band index and gdal checksum
    """
    origin = collection.tags()['NC_GLOBAL#References']

    return uuid.uuid5(
        NOAA_WATER_VAPOUR_NS,
        '{}?{}'.format(origin, urllib.parse.urlencode({
            'checksum': collection.checksum(idx),
            'band_index': idx
        }))
    )


def process_datasets(dataset: Path):
    """
    Generates a metadata document for each band available
    in the water vapour file.

    Each band is treated as a separate dataset since the
    source file is updated in-place; allowing the file
    to be modified but a static reference is provided for
    each observation

    """
    datasets = []
    creation_dt = (
        datetime.datetime.utcnow()
        .replace(tzinfo=datetime.timezone.utc)
    )

    with rasterio.open(str(dataset), 'r') as collection:
        collection_start_date = (
            datetime.datetime.strptime(
                collection.tags()['time#units'],
                'hours since %Y-%m-%d %H:%M:%S.%f'
            )
        )
        creation_dt = (
            datetime.datetime.utcnow()
            .replace(tzinfo=datetime.timezone.utc)
        )

        for _idx in collection.indexes:
            time_in_hours = int(collection.tags(_idx)['NETCDF_DIM_time'])
            ds_dt = (
                collection_start_date +
                datetime.timedelta(hours=time_in_hours)
            ).replace(tzinfo=datetime.timezone.utc)

            md = {}
            md['id'] = get_uuid(collection, _idx).hex
            md['creation_dt'] = creation_dt.isoformat()
            md['description'] = NOAA_DESCRIPTION
            md['extent'] = {
                'center_dt': ds_dt.isoformat(),
                'coord': get_coords(collection)
            }
            md['grid_spatial'] = {
                'projection': {
                    'geo_ref_points': get_coords(collection, xkey='x', ykey='y'),
                    'spatial_reference': rasterio.crs.CRS(init='EPSG:4236').wkt
                }
            }
            md['format'] = {'name': 'netCDF'}
            md['image'] = {
                'bands': {
                    'water_vapour': {
                        'path': dataset.name,
                        'layer': 'pr_wtr',
                        'band': _idx
                    }
                }
            }
            md['product_type'] = 'auxiliary'
            md['product_name'] = 'ncep_reanalysis_surface_pr_wtr'
            md['sources'] = {}

            datasets.append(md)

    return datasets


def _process_datasets(output_dir, datasets):
    """
    Wrapper function for processing multiple datasets
    """
    for dataset in datasets:
        docs = process_datasets(dataset)
        outfile = output_dir / (dataset.stem + '-metadata.yaml')
        write_yaml_from_dict(docs, outfile)


@click.command(help="""\b
        Prepare NCEP/NCAR reanalysis 1 water pressure datasets for indexing into a Data Cube.
        This prepare scripts supports netCDF water pressure files only.

        Example usage: yourscript.py --output [directory] input_file1 input_file2""")
@click.option('--output', 'output_dir', help="Write datasets into this directory",
              type=click.Path(exists=False, writable=True, dir_okay=True))
@click.argument('datasets',
                type=click.Path(exists=True, readable=True, writable=False),
                nargs=-1)
@click.option('-f', 'dataset_listing_files',
              type=click.Path(exists=True, readable=True, writable=False),
              help="file containing a list of input paths (one per line)", multiple=True)
def main(output_dir, datasets, dataset_listing_files):
    datasets = [Path(p) for p in datasets]
    for listing_file in dataset_listing_files:
        datasets.extend(
            read_paths_from_file(Path(listing_file))
        )

    return _process_datasets(Path(output_dir), datasets)


if __name__ == '__main__':
    main()
