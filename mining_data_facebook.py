import logging
from argparse import ArgumentParser
import sys
from os import path
from wordcloud import WordCloud, STOPWORDS
# from PIL import Image
# import matplotlib

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
currdir = path.dirname(__file__)


def get_parser():
    parser = ArgumentParser()
    parser.add_argument("--page")
    parser.add_argument("--n", type=int, default=10)
    return parser


def create_wordcloud(text):
    # create set of stopwords
    stopwords = set(STOPWORDS)
    stopwords.add("bit")
    stopwords.add("ly")
    stopwords.add("day")

    # create wordcloud object
    wc = WordCloud(
        background_color="white", max_words=200, stopwords=stopwords)

    # generate wordcloud
    wc.generate(text)

    # save wordcloud
    wc.to_file(path.join(currdir, "wc.png"))


if __name__ == "__main__":
    #   Get input page name
    parser = get_parser()
    args = parser.parse_args()
    LOGGER.info('Get data from page: %s' % (args.page))

    #   Connect to mongodb
    client = pymongo.MongoClient("localhost", 27017)
    LOGGER.info('Create connect to MongoDB')

    page_name = args.page
    mydb = client[page_name]

    # get query
    query = sys.argv[1]
    document = []

    # get text for given query
    cursor = mydb["posts"].find({})
    for post in cursor:
        try:
            document.append(post['message'])
        except Exception as e:
            pass
    document = " ".join(document)
    # generate wordcloud
    LOGGER.info("Creating wordcloud for page: %s" % (page_name))
    create_wordcloud(document)
