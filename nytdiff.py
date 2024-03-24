#!/usr/bin/python3

import collections
from datetime import datetime
import hashlib
import json
import logging
import os
import shutil
import sys
import time

from tempfile import TemporaryDirectory

import bleach
import dataset
from PIL import Image
from pytz import timezone
import requests
import tweepy
from simplediff import html_diff
from selenium import webdriver
from selenium.webdriver.common.by import By

from atproto import Client, models

TIMEZONE = 'America/Buenos_Aires'
LOCAL_TZ = timezone(TIMEZONE)
MAX_RETRIES = 10
RETRY_DELAY = 3

if 'TESTING' in os.environ:
    if os.environ['TESTING'] == 'False':
        TESTING = False
    else:
        TESTING = True
else:
    TESTING = True

if 'LOG_FOLDER' in os.environ:
    LOG_FOLDER = os.environ['LOG_FOLDER']
else:
    LOG_FOLDER = ''

PHANTOMJS_PATH = os.environ['PHANTOMJS_PATH']


class BaseParser(object):
    def __init__(self, api, bsky_api=None):
        self.urls = list()
        self.payload = None
        self.articles = dict()
        self.current_ids = set()
        self.filename = str()
        self.db = dataset.connect('sqlite:///titles.db')
        self.api = api
        self.bsky_api = bsky_api

    def test_twitter(self):
        print(self.api.rate_limit_status())
        print(self.api.me().name)

    def remove_old(self, column='id'):
        db_ids = set()
        for nota_db in self.articles_table.find(status='home'):
            db_ids.add(nota_db[column])
        for to_remove in (db_ids - self.current_ids):
            if column == 'id':
                data = dict(id=to_remove, status='removed')
            else:
                data = dict(article_id=to_remove, status='removed')
            self.articles_table.update(data, [column])
            logging.info('Removed %s', to_remove)

    def get_prev_tweet(self, article_id, column):
        if column == 'id':
            search = self.articles_table.find_one(id=article_id)
        else:
            search = self.articles_table.find_one(article_id=article_id)
        if search is None:
            return None
        else:
            if 'tweet_id' in search:
                return search['tweet_id']
            else:
                return None

    def get_bsky_parent(self, article_id, column):
        # Returns a tuple (parent, root) of bluesky "strong refs" for
        # the previously posted article in this thread
        # If no parent is found, returns (None, None)
        if column == 'id':
            search = self.articles_table.find_one(id=article_id)
        else:
            search = self.articles_table.find_one(article_id=article_id)
        if search and search.get('post_uri'):
            post_uri = search['post_uri']
            post_cid = search['post_cid']
            root_uri = search['root_uri']
            root_cid = search['root_cid']
            return (
                models.ComAtprotoRepoStrongRef.Main(uri=post_uri, cid=post_cid),
                models.ComAtprotoRepoStrongRef.Main(uri=root_uri, cid=root_cid),
            )
        else:
            return (None, None)

    def update_tweet_db(self, article_id, tweet_id, column):
        if column == 'id':
            article = {
                'id': article_id,
                'tweet_id': tweet_id
            }
        else:
            article = {
                'article_id': article_id,
                'tweet_id': tweet_id
            }
        self.articles_table.update(article, [column])
        logging.debug('Updated tweet ID in db')

    def update_bsky_db(self, article_id, post_ref, root_ref, column):
        article = {
            column: article_id,
            'post_uri': post_ref.uri,
            'post_cid': post_ref.cid,
            'root_uri': root_ref.uri,
            'root_cid': root_ref.cid,
        }
        self.articles_table.update(article, [column])
        logging.debug('Updated bsky refs in db')

    def media_upload(self, filename):
        if TESTING:
            return 1
        try:
            response = self.api.media_upload(filename)
        except:
            print (sys.exc_info()[0])
            logging.exception('Media upload')
            return False
        return response.media_id_string

    def tweet_with_media(self, text, images, reply_to=None):
        if TESTING:
            print (text, images, reply_to)
            return None
        try:
            if reply_to is not None:
                tweet_id = self.api.update_status(
                    status=text, media_ids=images,
                    in_reply_to_status_id=reply_to)
            else:
                tweet_id = self.api.update_status(
                    status=text, media_ids=images)
        except:
            logging.exception('Tweet with media failed')
            print (sys.exc_info()[0])
            return False
        return tweet_id

    def tweet_text(self, text):
        if TESTING:
            print (text)
        try:
            tweet_id = self.api.update_status(status=text)
        except:
            logging.exception('Tweet text failed')
            print (sys.exc_info()[0])
            return False
        return tweet_id

    def tweet(self, text, article_id, url, column='id'):
        images = list()
        image = self.media_upload('./output/' + self.filename + '.png')
        logging.info('Media ready with ids: %s', image)
        images.append(image)
        logging.info('Text to tweet: %s', text)
        logging.info('Article id: %s', article_id)
        reply_to = self.get_prev_tweet(article_id, column)
        if reply_to is None:
            logging.info('Tweeting url: %s', url)
            tweet = self.tweet_text(url)
            if tweet:
                reply_to = tweet.id
        logging.info('Replying to: %s', reply_to)
        tweet = self.tweet_with_media(text, images, reply_to)
        if tweet:
            logging.info('Id to store: %s', tweet.id)
            self.update_tweet_db(article_id, tweet.id, column)
        return

    def bsky_website_card(self, article_data):
        # Generate a website preview card for the specified url
        # Returns a models.AppBskyEmbedExternal object suitable
        # for passing as the `embed' argument to atproto.send_post
        post_title = article_data['title']
        post_description = article_data['abstract']
        post_uri = article_data['url']
        extra_args = {}
        if 'thumbnail' in article_data:
            r = requests.get(url=article_data['thumbnail'])
            if r.ok:
                thumb = self.bsky_api.upload_blob(r.content)
                extra_args['thumb'] = thumb.blob

        return models.AppBskyEmbedExternal.Main(
            external=models.AppBskyEmbedExternal.External(
                title=post_title,
                description=post_description,
                uri=post_uri,
                **extra_args
            )
        )

    def bsky_post(self, text, article_data, column='id'):
        article_id = article_data['article_id']
        url = article_data['url']
        img_path = './output/' + self.filename + '.png'
        with open(img_path, 'rb') as f:
            img_data = f.read()
        logging.info('Media ready with ids: %s', img_path)
        logging.info('Text to post: %s', text)
        logging.info('Article id: %s', article_id)
        (parent_ref, root_ref) = self.get_bsky_parent(article_id, column)
        logging.info('Parent ref: %s', parent_ref)
        logging.info('Root ref: %s', root_ref)
        if parent_ref is None:
            # No parent, let's start a new thread
            logging.info('Posting url: %s', url)
            post = self.bsky_api.send_post(
                '', embed=self.bsky_website_card(article_data)
            )
            root_ref = models.create_strong_ref(post)
            parent_ref = root_ref

        logging.info('Replying to: %s', parent_ref)
        post = self.bsky_api.send_image(
            text=text,
            image=img_data,
            image_alt='',
            reply_to=models.AppBskyFeedPost.ReplyRef(
                parent=parent_ref, root=root_ref)
        )
        child_ref = models.create_strong_ref(post)
        logging.info('Id to store: %s', child_ref)
        self.update_bsky_db(article_id, child_ref, root_ref, column)

    def get_page(self, url, header=None, payload=None):
        for x in range(MAX_RETRIES):
            try:
                r = requests.get(url=url, headers=header, params=payload)
            except BaseException as e:
                if x == MAX_RETRIES - 1:
                    print ('Max retries reached')
                    logging.warning('Max retries for: %s', url)
                    return None
                if '104' not in str(e):
                    print('Problem with url {}'.format(url))
                    print('Exception: {}'.format(str(e)))
                    logging.exception('Problem getting page')
                    return None
                time.sleep(RETRY_DELAY)
            else:
                break
        return r

    def strip_html(self, html_str):
        """
        a wrapper for bleach.clean() that strips ALL tags from the input
        """
        tags = []
        attr = {}
        strip = True
        return bleach.clean(html_str,
                            tags=tags,
                            attributes=attr,
                            strip=strip)

    def show_diff(self, old, new):
        if len(old) == 0 or len(new) == 0:
            logging.info('Old or New empty')
            return False
        new_hash = hashlib.sha224(new.encode('utf8')).hexdigest()
        logging.info(html_diff(old, new))
        html = """
        <!doctype html>
        <html lang="en">
          <head>
            <meta charset="utf-8">
            <link rel="stylesheet" href="css/styles.css">
          </head>
          <body>
          <p>
          {}
          </p>
          </body>
        </html>
        """.format(html_diff(old, new))
        with TemporaryDirectory(delete=False) as tmpdir:
            tmpfile = os.path.join(tmpdir, 'tmp.html')
            with open(tmpfile, 'w') as f:
                f.write(html)
            for d in ['css', 'fonts', 'img']:
                shutil.copytree(d, os.path.join(tmpdir, d))
            opts = webdriver.chrome.options.Options()
            opts.add_argument("--window-size=400,400")
            driver = webdriver.Chrome(options=opts)
            driver.get('file://{}'.format(tmpfile))
            logging.info('tmpfile is %s', tmpfile)

        e = driver.find_element(By.XPATH, '//p')
        timestamp = str(int(time.time()))
        self.filename = timestamp + new_hash
        e.screenshot('./output/' + self.filename + '.png')
        return True

    def __str__(self):
        return ('\n'.join(self.urls))


class NYTParser(BaseParser):
    def __init__(self, api, nyt_api_key, bsky_api=None):
        BaseParser.__init__(self, api, bsky_api=bsky_api)
        self.urls = ['https://api.nytimes.com/svc/topstories/v2/home.json']
        self.payload = {'api-key': nyt_api_key}
        self.articles_table = self.db['nyt_ids']
        self.versions_table = self.db['nyt_versions']

    def get_thumbnail(self, article):
        # Return the URL for the first thumbnail image in the article.
        for m in article['multimedia']:
            if m['type'] == 'image' and m['width'] < 400:
                return m['url']
        return None

    def json_to_dict(self, article):
        article_dict = dict()
        if not article.get('uri'):
            return None
        article_dict['article_id'] = article['uri']
        article_dict['url'] = article['url']
        article_dict['title'] = article['title']
        article_dict['abstract'] = self.strip_html(article['abstract'])
        article_dict['byline'] = article['byline']
        article_dict['kicker'] = article['kicker']
        article_dict['thumbnail'] = self.get_thumbnail(article)
        od = collections.OrderedDict(sorted(article_dict.items()))
        article_dict['hash'] = hashlib.sha224(
            repr(od.items()).encode('utf-8')).hexdigest()
        article_dict['date_time'] = datetime.now(LOCAL_TZ)
        return article_dict

    def store_data(self, data):
        if self.articles_table.find_one(
                article_id=data['article_id']) is None:  # New
            article = {
                'article_id': data['article_id'],
                'add_dt': data['date_time'],
                'status': 'home',
                'thumbnail': data['thumbnail'],
                'tweet_id': None,
                'post_uri': None,
                'post_cid': None,
                'root_uri': None,
                'root_cid': None,
            }
            self.articles_table.insert(article)
            logging.info('New article tracked: %s', data['url'])
            data['version'] = 1
            self.versions_table.insert(data)
        else:
            # re insert
            if self.articles_table.find_one(article_id=data['article_id'],
                                            status='removed') is not None:
                article = {
                    'article_id': data['article_id'],
                    'add_dt': data['date_time'],
                }

            count = self.versions_table.count(
                self.versions_table.table.columns.article_id == data[
                    'article_id'],
                hash=data['hash'])
            if count == 1:  # Existing
                pass
            else:  # Changed
                result = self.db.query('SELECT * \
                                       FROM nyt_versions\
                                       WHERE article_id = "%s" \
                                       ORDER BY version DESC \
                                       LIMIT 1' % (data['article_id']))
                for row in result:
                    data['version'] = row['version'] + 1
                    self.versions_table.insert(data)
                    url = data['url']
                    if row['url'] != data['url']:
                        if self.show_diff(row['url'], data['url']):
                            tweet_text = 'Change in URL'
                            self.bsky_post(tweet_text, data, 'article_id')
                            #self.tweet(tweet_text, data['article_id'], url,
                            #           'article_id')
                    if row['title'] != data['title']:
                        if self.show_diff(row['title'], data['title']):
                            tweet_text = 'Change in Title'
                            self.bsky_post(tweet_text, data, 'article_id')
                            #self.tweet(tweet_text, data['article_id'], url,
                            #           'article_id')
                    if row['abstract'] != data['abstract']:
                        if self.show_diff(row['abstract'], data['abstract']):
                            tweet_text = 'Change in Abstract'
                            self.bsky_post(tweet_text, data, 'article_id')
                            #self.tweet(tweet_text, data['article_id'], url,
                            #           'article_id')
                    if row['kicker'] != data['kicker']:
                        if self.show_diff(row['kicker'], data['kicker']):
                            tweet_text = 'Change in Kicker'
                            self.bsky_post(tweet_text, data, 'article_id')
                            #self.tweet(tweet_text, data['article_id'], url,
                            #           'article_id')

    def loop_data(self, data):
        if 'results' not in data:
            return False
        for article in data['results']:
            try:
                article_dict = self.json_to_dict(article)
                if article_dict is not None:
                    self.store_data(article_dict)
                    self.current_ids.add(article_dict['article_id'])
            except BaseException as e:
                logging.exception('Problem looping NYT: %s', article)
                print ('Exception: {}'.format(str(e)))
                print('***************')
                print(article)
                print('***************')
                return False
        return True

    def parse_pages(self):
        r = self.get_page(self.urls[0], None, self.payload)
        if r is None or len(r.text) == 0:
            logging.warning('Empty response NYT')
            return
        try:
            data = json.loads(r.text, strict=False)
        except BaseException as e:
            logging.exception('Problem parsing page: %s', r.text)
            print (e)
            print (len(r.text))
            print (type(r.text))
            print (r.text)
            print ('----')
            return
        loop = self.loop_data(data)
        if loop:
            self.remove_old('article_id')


def main():
    # logging
    logging.basicConfig(filename=LOG_FOLDER + 'titlediff.log',
                        format='%(asctime)s %(name)13s %(levelname)8s: ' +
                        '%(message)s',
                        level=logging.INFO)
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.info('Starting script')

    consumer_key = os.environ['NYT_TWITTER_CONSUMER_KEY']
    consumer_secret = os.environ['NYT_TWITTER_CONSUMER_SECRET']
    access_token = os.environ['NYT_TWITTER_ACCESS_TOKEN']
    access_token_secret = os.environ['NYT_TWITTER_ACCESS_TOKEN_SECRET']
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.secure = True
    auth.set_access_token(access_token, access_token_secret)
    nyt_api = tweepy.API(auth)
    logging.debug('NYT Twitter API configured')

    bsky_api = None
    if 'BLUESKY_LOGIN' in os.environ:
        bsky_login = os.environ['BLUESKY_LOGIN']
        bsky_passwd = os.environ['BLUESKY_PASSWD']
        bsky_api = Client(base_url='https://bsky.social')
        try:
            bsky_api.login(bsky_login, bsky_passwd)
        except:
            logging.exception('Bluesky login failed')
            return

    try:
        logging.debug('Starting NYT')
        nyt_api_key = os.environ['NYT_API_KEY']
        nyt = NYTParser(nyt_api, nyt_api_key, bsky_api=bsky_api)
        nyt.parse_pages()
        logging.debug('Finished NYT')
    except:
        logging.exception('NYT')

    logging.info('Finished script')


if __name__ == "__main__":
    main()
