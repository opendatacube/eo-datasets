import click
import os
from pathlib import Path
import logging

from eodatasets.package import package_ortho, package_nbar, package_raw, get_dataset


_DATASET_PACKAGERS = {
    'ortho': package_ortho,
    'nbar': package_nbar,
    'raw': package_raw
}

@click.command()
@click.option('--ancestor', type=click.Path(exists=True, readable=True, writable=False), multiple=True)
@click.option('--debug', is_flag=True)
@click.argument('type', type=click.Choice(_DATASET_PACKAGERS.keys()))
@click.argument('dataset', type=click.Path(exists=True, readable=True, writable=False), nargs=-1)
@click.argument('destination', type=click.Path(exists=True, readable=True, writable=True), nargs=1)
def run_packaging(ancestor, debug, type, dataset, destination):
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s')

    if debug:
        logging.getLogger().setLevel(logging.DEBUG)
    else:
        logging.getLogger('eodatasets').setLevel(logging.INFO)

    ancestor_datasets = {}

    # TODO: detect actual ancestor types.
    if ancestor:
        ancestor_datasets.update({'raw': get_dataset(Path(ancestor[0]))})

    for dataset_path in dataset:
        destination = os.path.join(destination, type)
        if not os.path.exists(destination):
            os.mkdir(destination)

        _DATASET_PACKAGERS[type](
            dataset_path,
            destination,
            source_datasets=ancestor_datasets
        )


run_packaging()