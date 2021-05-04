#!/usr/bin/python3

import collections
from datetime import datetime
import hashlib
import json
import logging
import os
import sys
import time

import bleach
import dataset
from PIL import Image
from pytz import timezone
import requests
import tweepy
from simplediff import string_diff
from selenium import webdriver

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
    def __init__(self, api):
        self.urls = list()
        self.payload = None
        self.articles = dict()
        self.current_ids = set()
        self.filename = str()
        self.db = dataset.connect('sqlite:///titles.db')
        self.api = api

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
            return True
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
            reply_to = tweet.id
        logging.info('Replying to: %s', reply_to)
        tweet = self.tweet_with_media(text, images, reply_to)
        logging.info('Id to store: %s', tweet.id)
        self.update_tweet_db(article_id, tweet.id, column)
        return

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
        styles = []
        strip = True
        return bleach.clean(html_str,
                            tags=tags,
                            attributes=attr,
                            styles=styles,
                            strip=strip)

    def html_diff(old, new):
        """
        Like simplediff.html_diff(), with a tweak: if a hunk
        consists of only deleting or only adding at the beginning
        or end of a word, then it's combined into one hunk.

        Examples:
            Changes from simplediff:
                - Alice, Bob and Charlie
                + Alice, Bob, and Charlie
                simplediff: Alice, <del>Bob</del><ins>Bob,</ins> Charlie
                this diff:  Alice, Bob<del>,</del> Charlie

                - Alice Bob Charlie's Angels And David
                + Alice Bob Charlie David
                simplediff: Alice Bob <del>Charlie's Angels And</del><ins>Charlie</ins> David
                this diff:  Alice Bob Charlie<del>'s Angels And</del> David

            Same as simplediff:
                hunks you wouldn't want simplified:
                  - Alice Bob Charlie
                  + Alice Robert Charlie
                  diff: Alice <del>Bob</del> <ins>Robert</ins> Charlie

                if the change isn't only at the beginning or end:
                  - Alice Bob Charlie
                  + Alice Blob Charlie
                  diff: Alice <del>Bob</del> <ins>Bob</ins> Charlie
                  
                  - Alice Bobby Charlie
                  + Alice bb Charlie
                  diff: Alice <del>Bobby</del> <ins>bb</ins>
                  
                  - Alice Zeneca Charlie
                  + Alice AstraZeneca Charlie's
                  diff: Alice <del>Zeneca Charlie</del> <ins>AstraZeneca Charlie's</ins>
        """
        def hunk_to_html(op, words):
            words = ' '.join(words)
            if op == '-':
                return '<del>{}</del>'.format(words)
            if op == '+':
                return '<ins>{}</ins>'.format(words)
            return words

        hunks = string_diff(old, new)
        html = []
        skip_next = False
        for (prev_op, prev_words), (next_op, next_words) in zip(hunks[:-1], hunks[1:]):
            if prev_op == '-' and next_op == '+':
                if len(prev_words) == 1:
                    [old_word] = prev_words
                    first_new_word, last_new_word = next_words[0], next_words[-1]
                    if first_new_word.startswith(old_word):
                        next_words[0] = old_word + '<ins>' + first_new_word[len(old_word):]
                        html.append(' '.join(next_words) + '</ins>')
                        skip_next = True
                        continue
                    elif last_new_word.endswith(old_word):
                        next_words[-1] = last_new_word[:-len(old_word)] + '</ins>' + old_word
                        html.append('<ins>' + ' '.join(next_words))
                        skip_next = True
                        continue
                if len(next_words) == 1:
                    [new_word] = next_words
                    first_old_word, last_old_word = prev_words[0], prev_words[-1]
                    if first_old_word.startswith(new_word):
                        prev_words[0] = new_word + '<del>' + first_old_word[len(new_word):]
                        html.append(' '.join(prev_words) + '</del>')
                        skip_next = True
                        continue
                    elif last_old_word.endswith(new_word):
                        prev_words[-1] = last_old_word[:-len(new_word)] + '</del>' + new_word
                        html.append('<del>' + ' '.join(prev_words))
                        skip_next = True
                        continue
            if skip_next:
                skip_next = False
                continue
            html.append(hunk_to_html(prev_op, prev_words))

        if not skip_next:
            html.append(hunk_to_html(*(hunks[-1])))

        return ' '.join(html)

    def show_diff(self, old, new):
        if len(old) == 0 or len(new) == 0:
            logging.info('Old or New empty')
            return False
        new_hash = hashlib.sha224(new.encode('utf8')).hexdigest()
        htmldiff = self.html_diff(old, new)
        logging.info(htmldiff)
        html = """
        <!doctype html>
        <html lang="en">
          <head>
            <meta charset="utf-8">
            <link rel="stylesheet" href="./css/styles.css">
          </head>
          <body>
          <p>
          {}
          </p>
          </body>
        </html>
        """.format(htmldiff)
        with open('tmp.html', 'w') as f:
            f.write(html)

        driver = webdriver.PhantomJS(
            executable_path=PHANTOMJS_PATH + 'phantomjs')
        driver.get('tmp.html')
        e = driver.find_element_by_xpath('//p')
        start_height = e.location['y']
        block_height = e.size['height']
        end_height = start_height
        start_width = e.location['x']
        block_width = e.size['width']
        end_width = start_width
        total_height = start_height + block_height + end_height
        total_width = start_width + block_width + end_width
        timestamp = str(int(time.time()))
        driver.save_screenshot('./tmp.png')
        img = Image.open('./tmp.png')
        img2 = img.crop((0, 0, total_width, total_height))
        if int(total_width) > int(total_height * 2):
            background = Image.new('RGBA', (total_width, int(total_width / 2)),
                                   (255, 255, 255, 0))
            bg_w, bg_h = background.size
            offset = (int((bg_w - total_width) / 2),
                      int((bg_h - total_height) / 2))
        else:
            background = Image.new('RGBA', (total_width, total_height),
                                   (255, 255, 255, 0))
            bg_w, bg_h = background.size
            offset = (int((bg_w - total_width) / 2),
                      int((bg_h - total_height) / 2))
        background.paste(img2, offset)
        self.filename = timestamp + new_hash
        background.save('./output/' + self.filename + '.png')
        return True

    def __str__(self):
        return ('\n'.join(self.urls))


class NYTParser(BaseParser):
    def __init__(self, api, nyt_api_key):
        BaseParser.__init__(self, api)
        self.urls = ['https://api.nytimes.com/svc/topstories/v2/home.json']
        self.payload = {'api-key': nyt_api_key}
        self.articles_table = self.db['nyt_ids']
        self.versions_table = self.db['nyt_versions']

    def json_to_dict(self, article):
        article_dict = dict()
        if 'short_url' not in article:
            return None
        article_dict['article_id'] = article['short_url'].split('/')[-1]
        article_dict['url'] = article['url']
        article_dict['title'] = article['title']
        article_dict['abstract'] = self.strip_html(article['abstract'])
        article_dict['byline'] = article['byline']
        article_dict['kicker'] = article['kicker']
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
                'tweet_id': None
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
                            self.tweet(tweet_text, data['article_id'], url,
                                       'article_id')
                    if row['title'] != data['title']:
                        if self.show_diff(row['title'], data['title']):
                            tweet_text = 'Change in Title'
                            self.tweet(tweet_text, data['article_id'], url,
                                       'article_id')
                    if row['abstract'] != data['abstract']:
                        if self.show_diff(row['abstract'], data['abstract']):
                            tweet_text = 'Change in Abstract'
                            self.tweet(tweet_text, data['article_id'], url,
                                       'article_id')
                    if row['kicker'] != data['kicker']:
                        if self.show_diff(row['kicker'], data['kicker']):
                            tweet_text = 'Change in Kicker'
                            self.tweet(tweet_text, data['article_id'], url,
                                       'article_id')

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

    try:
        logging.debug('Starting NYT')
        nyt_api_key = os.environ['NYT_API_KEY']
        nyt = NYTParser(nyt_api, nyt_api_key)
        nyt.parse_pages()
        logging.debug('Finished NYT')
    except:
        logging.exception('NYT')

    logging.info('Finished script')


if __name__ == "__main__":
    main()
