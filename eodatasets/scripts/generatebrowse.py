# coding=utf-8
from __future__ import absolute_import
import logging

import click

from eodatasets.browseimage import regenerate_browse_image


@click.command()
@click.option('--debug', is_flag=True)
@click.argument('dataset', type=click.Path(exists=True, readable=True, writable=False), nargs=-1)
def run_regeneration(debug, dataset):
    """
    Regenerate browse images for the given datasets.
    :param debug:
    :param dataset:
    :return:
    """
    logging.basicConfig(level=logging.INFO)
    if debug:
        logging.getLogger().setLevel(logging.DEBUG)

    for d in dataset:
        regenerate_browse_image(d)


if __name__ == '__main__':
    # Click fills out the parameters, which confuses pylint.
    # pylint: disable=no-value-for-parameter
    run_regeneration()
