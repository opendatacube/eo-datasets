#!/usr/bin/env python
# coding=utf-8
from __future__ import absolute_import

import click
from pathlib import Path

from eodatasets import run as run_package, drivers
from eodatasets.scripts import init_logging


@click.command()
@click.option('--parent',
              type=click.Path(exists=True, readable=True, writable=False),
              multiple=True,
              help='Path of the parent dataset (that these datasets were derived from.)')
@click.option('--debug',
              is_flag=True,
              help='Enable debug logging')
@click.option('--hard-link',
              is_flag=True,
              help='Hard-link output files if possible (faster than copying)')
@click.argument('package_type',
                type=click.Choice(drivers.PACKAGE_DRIVERS.keys()))
@click.argument('dataset',
                type=click.Path(exists=True, readable=True, writable=False),
                nargs=-1)
@click.argument('destination',
                type=click.Path(exists=True, readable=True, writable=True),
                nargs=1)
def run(parent, debug, hard_link, package_type, dataset, destination):
    """
    Package the given imagery folders.
    """
    init_logging(debug)
    run_package.package_existing_data_folder(
        drivers.PACKAGE_DRIVERS[package_type],
        [Path(p) for p in dataset],
        Path(destination),
        [Path(p) for p in parent],
        hard_link=hard_link
    )


if __name__ == '__main__':
    # Click fills out the parameters, which confuses pylint.
    # pylint: disable=no-value-for-parameter
    run()
