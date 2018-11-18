import logging
from argparse import ArgumentParser

import pymongo
# import os
# from datetime import datetime

LOGGER = logging.getLogger(__name__)
s = 2
LOGGER.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt='%d-%b-%y %H:%M:%S')
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
LOGGER.addHandler(ch)


def get_parser():
    parser = ArgumentParser()
    parser.add_argument("--page")
    parser.add_argument("--n", type=int, default=10)
    return parser


if __name__ == "__main__":
    #   Get input page name
    parser = get_parser()
    args = parser.parse_args()
    LOGGER.info('Get data from page: %s' % (args.page))

    #   Connect to mongodb
    client = pymongo.MongoClient("localhost", 27017)
    LOGGER.info('Create connect to MongoDB')

    page_name = args.page
