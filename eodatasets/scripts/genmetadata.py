#!/usr/bin/env python
# coding=utf-8
from __future__ import absolute_import
import logging

import click
from pathlib import Path

from eodatasets import package, drivers, serialise
from eodatasets.scripts import init_logging

_LOG = logging.getLogger('eodatasets')


@click.command()
@click.option('--parent',
              type=click.Path(exists=True, readable=True, writable=False),
              multiple=True,
              help='Path of the parent dataset (these datasets were derived from.)')
@click.option('--debug',
              is_flag=True,
              help='Enable debug logging')
@click.argument('package_type',
                type=click.Choice(drivers.PACKAGE_DRIVERS.keys()))
@click.argument('datasets',
                type=click.Path(exists=True, readable=True, writable=True),
                nargs=-1)
def run(parent, debug, package_type, datasets):
    """
    Package the given imagery folders.
    """
    init_logging(debug)

    parent_datasets = {}

    #: :type: package.DatasetDriver
    driver = drivers.PACKAGE_DRIVERS[package_type]

    # TODO: Multiple parents?
    if parent:
        source_id = driver.expected_source().get_id()
        parent_datasets.update({source_id: serialise.read_dataset_metadata(Path(parent[0]))})

    for dataset_file in datasets:
        dataset_path = Path(dataset_file)
        md_path = package.package_inplace_dataset(
            driver,
            package.init_existing_dataset(dataset_path, parent_datasets),
            dataset_path
        )
        _LOG.info('Wrote metadata file %s', md_path)


if __name__ == '__main__':
    # Click fills out the parameters, which confuses pylint.
    # pylint: disable=no-value-for-parameter
    run()
