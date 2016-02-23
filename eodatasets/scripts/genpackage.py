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
@click.option('--hard-link/--no-hard-link',
              default=False,
              help='Hard-link output files if possible (faster than copying)')
@click.option('--newly-processed/--external-dataset',
              default=False,
              help='Include provenance and processing time for the current machine.')
@click.option('--add-file',
              type=click.Path(exists=True, readable=True, writable=False),
              multiple=True,
              help='Additional file to note in the package (eg. a useful log file).')
@click.argument('package_type',
                type=click.Choice(drivers.PACKAGE_DRIVERS.keys()))
@click.argument('dataset',
                type=click.Path(exists=True, readable=True, writable=False),
                nargs=-1)
@click.argument('destination',
                type=click.Path(exists=True, readable=True, writable=True),
                nargs=1)
def run(parent, debug, hard_link, newly_processed, package_type, dataset, destination, add_file):
    """
    Package the given imagery folders.
    """
    init_logging(debug)

    if newly_processed:
        run_package.package_newly_processed_data_folder(
            driver=drivers.PACKAGE_DRIVERS[package_type],
            input_data_paths=[Path(p) for p in dataset],
            destination_path=Path(destination),
            parent_dataset_paths=[Path(p) for p in parent],
            hard_link=hard_link,
            additional_files=[Path(p) for p in add_file]
        )
    else:
        run_package.package_existing_data_folder(
            driver=drivers.PACKAGE_DRIVERS[package_type],
            input_data_paths=[Path(p) for p in dataset],
            destination_path=Path(destination),
            parent_dataset_paths=[Path(p) for p in parent],
            hard_link=hard_link,
            additional_files=[Path(p) for p in add_file]
        )


if __name__ == '__main__':
    # Click fills out the parameters, which confuses pylint.
    # pylint: disable=no-value-for-parameter
    run()
