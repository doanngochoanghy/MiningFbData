import logging
from argparse import ArgumentParser
# import thread
import pymongo
import facebook
import os
import requests
from datetime import datetime

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


def get_page_id(graph, name_page):
    """TODO: Docstring for get_page_id.
    graph connection to FB
    name_page name of the page
    """
    try:
        return graph.get_object(id=name_page, fields="id")['id']
    except Exception:
        print("This %s page doesn't exist or access_token expired." %
              (page_name))
        return ""


def insert_posts_into_DB(mydb, posts):
    """Insert posts in to MongoClient

    :client: TODO
    :posts: TODO
    :returns: TODO

    """
    for post in posts:
        # downloaded += 1
        post['_id'] = post.pop('id')
        if mydb['posts'].find({'_id': post['_id']}).count() == 0:
            mydb['posts'].insert_one(post)


if __name__ == "__main__":
    #   Get input page name
    parser = get_parser()
    args = parser.parse_args()
    LOGGER.info('Get data from page: %s' % (args.page))

    #   Connect to mongodb
    client = pymongo.MongoClient("localhost", 27017)
    LOGGER.info('Create connect to MongoDB')

    #   Get access token
    access_token = os.environ.get('ACCESS_TOKEN')
    #   Create graph API from fb
    graph = facebook.GraphAPI(access_token=access_token, version="2.7")
    page_name = args.page
    #   Get page_id
    page_id = get_page_id(graph, page_name)
    #   Check page exists
    if page_id == "":
        pass
    else:
        #   Get infomation from page and insert into DB
        fields = [
            'id',
            'name',
            'username',
            'location',
            'about',
            'website',
            'category',
            'checkins',
            'can_checkin',
            'can_post',
            'display_subtext',
            'fan_count',
            'has_whatsapp_number',
            'has_added_app',
        ]
        fields = ','.join(fields)
        infomation = graph.get_object(id=page_id, fields=fields)
        infomation['_id'] = infomation.pop('id')
        mydb = client[page_name]
        mycol = mydb['page']
        if mycol.find({"_id": page_id}).count() == 0:
            mycol.insert_one(infomation)
            LOGGER.info('Insert page "%s" infomation to DB' % (page_name))
        else:
            LOGGER.info('Infomation page "%s" exists in DB' % (page_name))

        #   Get posts from page and insert into DB
        fields = [
            'id',
            'object_id',
            'created_time',
            'type',
            'message',
            'description',
            'link',
            'properties',
            'shares',
            'source',
            'updated_time',
        ]
        fields = ','.join(fields)
        # posts = graph.get_object(id=page_id + "/posts", fields=fields)
        posts = graph.get_connections(
            id=page_id, connection_name="posts", fields=fields, limit=100)
        downloaded = 0
        n = args.n

        #   loop posts of the page and insert into DB
        LOGGER.info("Start insert posts from into DB")
        start = datetime.now()
        LOGGER.debug("Inserting...")
        while n > downloaded:
            try:
                insert_posts_into_DB(mydb, posts['data'])
                # thread.start_new_thread(insert_posts_into_DB,
                #                         (mydb, posts['data']))
                downloaded += len(posts['data'])
                posts = requests.get(posts['paging']['next']).json()
            except Exception as e:
                # raise e
                break
        LOGGER.info("Insert %s post into DB in %s seconds" %
                    (downloaded, (datetime.now() - start).seconds))
