# coding=utf-8
from __future__ import absolute_import
import os
import logging

import click
from pathlib import Path

from eodatasets import package, drivers, serialise


@click.command()
@click.option('--parent', type=click.Path(exists=True, readable=True, writable=False), multiple=True)
@click.option('--debug', is_flag=True)
@click.option('--in-place', is_flag=True)
@click.option('--hard-link', is_flag=True)
@click.argument('type', type=click.Choice(drivers.PACKAGE_DRIVERS.keys()))
@click.argument('dataset', type=click.Path(exists=True, readable=True, writable=False), nargs=-1)
@click.argument('destination', type=click.Path(exists=True, readable=True, writable=True), nargs=1)
def run_packaging(parent, debug, in_place, hard_link, type, dataset, destination):
    """
    :type parent: str
    :type debug: bool
    :type in_place: bool
    :type type: str
    :type dataset: list of str
    :type destination: str
    """
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s')

    if debug:
        logging.getLogger().setLevel(logging.DEBUG)
    else:
        logging.getLogger('eodatasets').setLevel(logging.INFO)

    parent_datasets = {}

    #: :type: package.DatasetDriver
    driver = drivers.PACKAGE_DRIVERS[type]

    # TODO: Multiple parents?
    if parent:
        source_id = driver.expected_source().get_id()
        parent_datasets.update({source_id: serialise.read_dataset_metadata(Path(parent[0]))})

    # If we're packaging in-place (ie. generating metadata), all listed paths are datasets.
    if in_place:
        dataset = list(dataset)
        dataset.append(destination)
        destination = None

    for dataset_path in dataset:
        if in_place:
            target_folder = dataset_path
        else:
            target_folder = os.path.join(destination, type)
            if not os.path.exists(target_folder):
                os.mkdir(target_folder)

        package.do_package(
            driver,
            dataset_path,
            target_folder,
            source_datasets=parent_datasets,
            hard_link=hard_link
        )

if __name__ == '__main__':
    run_packaging()