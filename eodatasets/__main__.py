import os
import logging

import click
from pathlib import Path

from eodatasets.package import get_dataset, generate_raw_metadata, generate_ortho_metadata, do_package


_DATASET_PACKAGERS = {
    'raw': generate_raw_metadata,
    'ortho': generate_ortho_metadata,
    # No nbar yet.
    'nbar': lambda d, path: d
}


@click.command()
@click.option('--parent', type=click.Path(exists=True, readable=True, writable=False), multiple=True)
@click.option('--debug', is_flag=True)
@click.argument('type', type=click.Choice(_DATASET_PACKAGERS.keys()))
@click.argument('dataset', type=click.Path(exists=True, readable=True, writable=False), nargs=-1)
@click.argument('destination', type=click.Path(exists=True, readable=True, writable=True), nargs=1)
def run_packaging(parent, debug, type, dataset, destination):
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s')

    if debug:
        logging.getLogger().setLevel(logging.DEBUG)
    else:
        logging.getLogger('eodatasets').setLevel(logging.INFO)

    parent_datasets = {}

    # TODO: detect actual parent datasets.
    if parent:
        parent_datasets.update({'raw': get_dataset(Path(parent[0]))})

    for dataset_path in dataset:
        destination = os.path.join(destination, type)
        if not os.path.exists(destination):
            os.mkdir(destination)

        do_package(
            _DATASET_PACKAGERS[type],
            dataset_path,
            destination,
            source_datasets=parent_datasets
        )

run_packaging()