# coding=utf-8

from __future__ import absolute_import

import logging


def init_logging(debug):
    logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s")
    if debug:
        logging.getLogger().setLevel(logging.DEBUG)
    else:
        logging.getLogger("eodatasets3").setLevel(logging.INFO)
