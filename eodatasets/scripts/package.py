# coding=utf-8
from __future__ import absolute_import
import os
import logging
import uuid

import click
from pathlib import Path

from eodatasets import package, drivers, serialise


@click.command()
@click.option('--parent',
              type=click.Path(exists=True, readable=True, writable=False),
              multiple=True,
              help='Path of the parent dataset (these datasets were derived from.)')
@click.option('--debug',
              is_flag=True,
              help='Enable debug logging')
@click.option('--hard-link',
              is_flag=True,
              help='Hard-link output files if possible (faster than copying)')
@click.argument('type',
                type=click.Choice(drivers.PACKAGE_DRIVERS.keys()))
@click.argument('dataset',
                type=click.Path(exists=True, readable=True, writable=False),
                nargs=-1)
@click.argument('destination',
                type=click.Path(exists=True, readable=True, writable=True),
                nargs=1)
def run_packaging(parent, debug, hard_link, type, dataset, destination):
    """
    Package the given imagery folders.
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

    for dataset_path in dataset:
        temp_output_dir = os.path.join(destination, '.packagetmp.%s' % uuid.uuid1())
        os.mkdir(temp_output_dir)

        dataset_id = package.package_existing_dataset(
            driver,
            Path(dataset_path),
            Path(temp_output_dir),
            source_datasets=parent_datasets,
            hard_link=hard_link
        )

        actual_output_dir = os.path.join(destination, dataset_id)
        os.rename(temp_output_dir, actual_output_dir)


if __name__ == '__main__':
    # Click fills out the parameters, which confuses pylint.
    # pylint: disable=no-value-for-parameter
    run_packaging()