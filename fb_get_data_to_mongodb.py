import logging
from argparse import ArgumentParser
# import thread
from threading import Thread
import Queue
import pymongo
import facebook
import os
import requests
from datetime import datetime
# import time

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

#   Get access token
access_token = os.environ.get('ACCESS_TOKEN')
#   Create graph API from fb
graph = facebook.GraphAPI(access_token=access_token, version="3.0")

#   Connect to mongodb
client = pymongo.MongoClient("localhost", 27017)
LOGGER.info('Create connect to MongoDB')


def get_parser():
    parser = ArgumentParser()
    parser.add_argument("--page")
    parser.add_argument("--n", type=int, default=100)
    return parser


parser = get_parser()
args = parser.parse_args()
LOGGER.info('Get data from page: %s' % (args.page))

page_name = args.page
mydb = client[page_name]


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


def insert_posts_into_DB(posts):
    """Insert posts in to MongoClient

    :client: TODO
    :posts: TODO
    :returns: TODO

    """
    for post in posts:
        post['_id'] = post.pop('id')
        if mydb['posts'].find({'_id': post['_id']}).count() == 0:
            mydb['posts'].insert_one(post)


def insert_post_and_comments(post):
    """TODO: Docstring for get_comments_and_insert_DB.

    :graph: TODO
    :post_id: TODO
    :mydb: TODO
    :returns: TODO

    """
    post['_id'] = post.pop('id')
    post_id = post['_id']
    if mydb['posts'].find({'_id': post_id}).count() == 0:
        mydb['posts'].insert_one(post)
    comments = graph.get_connections(
        id=post_id,
        connection_name='comments',
        fields=comments_fields,
        limit=100)
    count = 0
    while True:
        try:
            insert_comments_into_DB(post_id, comments['data'])
            count += len(comments['data'])
            comments = requests.get(comments['paging']['next']).json()
        except Exception:
            break
    LOGGER.info("Insert %s comments into DB" % (count))
    # time.sleep(1.5)


def insert_comments_into_DB(post_id, comments):
    """Insert comments of posts into MongoClient

    :mydb: TODO
    :post_id: TODO
    :comments: TODO
    :returns: TODO

    """
    for comment in comments:
        comment['_id'] = comment.pop('id')
        comment['post_id'] = post_id
        if mydb['comments'].find({'_id': comment['_id']}).count() == 0:
            mydb['comments'].insert_one(comment)


class CommentsInserter(Thread):
    """Thread to insert comments into DB """

    def __init__(self, queue):
        """

        :queue: TODO

        """
        Thread.__init__(self)

        self._queue = queue

    def run(self):
        while True:
            post = self._queue.get()
            try:
                insert_post_and_comments(post)
            except Exception as e:
                LOGGER.info("Got error: %s" % (e))
            finally:
                self._queue.task_done()


if __name__ == "__main__":
    #   Get input page name
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
            'like_count',
        ]
        comments_fields = [
            'id',
            'created_time',
            'message',
            'like_count',
            'comment_count',
        ]
        fields = ','.join(fields)
        comments_fields = ','.join(comments_fields)
        # posts = graph.get_object(id=page_id + "/posts", fields=fields)
        posts = graph.get_connections(
            id=page_id, connection_name="posts", fields=fields, limit=100)
        downloaded = 0
        n = args.n

        #   loop posts of the page and insert into DB
        LOGGER.info("Start insert posts from into DB")
        start = datetime.now()
        LOGGER.debug("Inserting...")

        queue = Queue.Queue()
        for x in range(2):
            th = CommentsInserter(queue)
            th.daemon = True
            th.start()
        while n > downloaded:
            try:
                # insert_posts_into_DB(posts['data'])
                downloaded += len(posts['data'])
                for post in posts['data']:
                    try:
                        if mydb['posts'].find({
                                '_id': post['id']
                        }).count() == 0:
                            insert_post_and_comments(post)
                            # queue.put(post)
                    except Exception as e:
                        LOGGER.info("Error %s" % (e))
                        continue
                LOGGER.info(("Scan %d requests") % (downloaded))
                posts = requests.get(posts['paging']['next']).json()
            except Exception as e:
                LOGGER.info("post: %s" % (posts))
                LOGGER.info("Error %s" % (e))
                break
        # queue.join()
        LOGGER.info("Insert %s post into DB in %s seconds" %
                    (downloaded, (datetime.now() - start).seconds))
