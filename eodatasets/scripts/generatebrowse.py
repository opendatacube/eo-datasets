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


run_regeneration()

