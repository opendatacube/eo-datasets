# coding=utf-8

import logging


def init_logging(debug):
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s')
    if debug:
        logging.getLogger().setLevel(logging.DEBUG)
    else:
        logging.getLogger('eodatasets').setLevel(logging.INFO)
