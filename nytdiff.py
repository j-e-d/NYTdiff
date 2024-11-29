#!/usr/bin/python3

import collections
import hashlib
import json
import logging
import os
import shutil
import sys
import time
from datetime import datetime
from tempfile import TemporaryDirectory

import bleach
import dataset
import requests
import tweepy
from atproto import Client, models
from PIL import Image
from pytz import timezone
from simplediff import html_diff
from selenium import webdriver
from selenium.webdriver.common.by import By

TIMEZONE = "America/Buenos_Aires"
LOCAL_TZ = timezone(TIMEZONE)
MAX_RETRIES = 10
RETRY_DELAY = 3

if "TESTING" in os.environ:
    if os.environ["TESTING"] == "False":
        TESTING = False
    else:
        TESTING = True
else:
    TESTING = True

if "LOG_FOLDER" in os.environ:
    LOG_FOLDER = os.environ["LOG_FOLDER"]
else:
    LOG_FOLDER = ""

PHANTOMJS_PATH = os.environ["PHANTOMJS_PATH"]


class BaseParser(object):
    def __init__(self, api, client, bsky_api=None):
        self.urls = list()
        self.payload = None
        self.articles = dict()
        self.current_ids = set()
        self.filename = str()
        self.db = dataset.connect("sqlite:///titles.db")
        self.api = api
        self.client = client
        self.bsky_api = bsky_api

    def remove_old(self, column="id"):
        db_ids = set()
        for nota_db in self.articles_table.find(status="home"):
            db_ids.add(nota_db[column])
        for to_remove in db_ids - self.current_ids:
            if column == "id":
                data = dict(id=to_remove, status="removed")
            else:
                data = dict(article_id=to_remove, status="removed")
            self.articles_table.update(data, [column])
            logging.info("Removed %s", to_remove)

    def get_prev_tweet(self, article_id, column):
        if column == "id":
            search = self.articles_table.find_one(id=article_id)
        else:
            search = self.articles_table.find_one(article_id=article_id)
        if search is None:
            return None
        else:
            if "tweet_id" in search:
                return search["tweet_id"]
            else:
                return None

    def get_bsky_parent(self, article_id, column):
        # Returns a tuple (parent, root) of bluesky "strong refs" for
        # the previously posted article in this thread
        # If no parent is found, returns (None, None)
        if column == "id":
            search = self.articles_table.find_one(id=article_id)
        else:
            search = self.articles_table.find_one(article_id=article_id)
        if search and search.get("post_uri"):
            post_uri = search["post_uri"]
            post_cid = search["post_cid"]
            root_uri = search["root_uri"]
            root_cid = search["root_cid"]
            return (
                models.ComAtprotoRepoStrongRef.Main(uri=post_uri, cid=post_cid),
                models.ComAtprotoRepoStrongRef.Main(uri=root_uri, cid=root_cid),
            )
        else:
            return (None, None)

    def update_tweet_db(self, article_id, tweet_id, column):
        if column == "id":
            article = {"id": article_id, "tweet_id": tweet_id}
        else:
            article = {"article_id": article_id, "tweet_id": tweet_id}
        self.articles_table.update(article, [column])
        logging.debug("Updated tweet ID in db")

    def update_bsky_db(self, article_id, post_ref, root_ref, column):
        article = {
            column: article_id,
            "post_uri": post_ref.uri,
            "post_cid": post_ref.cid,
            "root_uri": root_ref.uri,
            "root_cid": root_ref.cid,
        }
        self.articles_table.update(article, [column])
        logging.debug("Updated bsky refs in db")

    def media_upload(self, filename):
        if TESTING:
            return 1
        try:
            response = self.api.media_upload(filename)
        except:
            print(sys.exc_info()[0])
            logging.exception("Media upload")
            return False
        return response.media_id_string

    def tweet_with_media(self, text, images, reply_to=None):
        if TESTING:
            print(text, images, reply_to)
            return True
        try:
            if reply_to is not None:
                tweet = self.client.create_tweet(
                    text=text, media_ids=images, in_reply_to_tweet_id=reply_to
                )
            else:
                tweet = self.client.create_tweet(text=text, media_ids=images)
        except:
            logging.exception("Tweet with media failed")
            print(sys.exc_info()[0])
            return False
        return tweet

    def tweet_text(self, text):
        if TESTING:
            print(text)
            return True
        try:
            tweet = self.client.create_tweet(text=text)
        except:
            logging.exception("Tweet text failed")
            print(sys.exc_info()[0])
            return False
        return tweet

    def media_metadata(self, image, alt_text):
        if TESTING:
            print(image, alt_text)
            return True
        try:
            self.api.create_media_metadata(image, alt_text)
        except:
            logging.exception("Tweet text failed")
            print(sys.exc_info()[0])
            return False
        return True

    def tweet(
        self, text, article_id, url, column="id", alt_text=None, archive_url=None
    ):
        if not self.client:
            return
        images = list()
        image = self.media_upload("./output/" + self.filename + ".png")
        logging.info("Media ready with ids: %s", image)
        images.append(image)
        if alt_text is not None:
            logging.info("Alt text to add: %s", alt_text)
            self.media_metadata(image, alt_text)
        logging.info("Text to tweet: %s", text)
        logging.info("Article id: %s", article_id)
        reply_to = self.get_prev_tweet(article_id, column)
        if reply_to is None:
            if archive_url is not None:
                url = url + " " + archive_url
            logging.info("Tweeting url/s: %s", url)
            tweet = self.tweet_text(url)
            if tweet is False:
                return
            reply_to = tweet.data["id"]
        logging.info("Replying to: %s", reply_to)
        tweet = self.tweet_with_media(text, images, reply_to)
        if tweet is False:
            return
        logging.info("Id to store: %s", tweet.data["id"])
        self.update_tweet_db(article_id, tweet.data["id"], column)
        return

    def bsky_website_card(self, article_data):
        # Generate a website preview card for the specified url
        # Returns a models.AppBskyEmbedExternal object suitable
        # for passing as the `embed' argument to atproto.send_post
        post_title = article_data["title"]
        post_description = article_data["abstract"]
        post_uri = article_data["url"]
        extra_args = {}
        if article_data.get('thumbnail'):
            r = requests.get(url=article_data['thumbnail'])
            if r.ok:
                thumb = self.bsky_api.upload_blob(r.content)
                extra_args["thumb"] = thumb.blob

        return models.AppBskyEmbedExternal.Main(
            external=models.AppBskyEmbedExternal.External(
                title=post_title,
                description=post_description,
                uri=post_uri,
                **extra_args,
            )
        )

    def bsky_post(self, text, article_data, column="id", alt_text=""):
        if not self.bsky_api:
            return
        article_id = article_data["article_id"]
        url = article_data["url"]

        # Collect image data for the thumbnail
        img_path = "./output/" + self.filename + ".png"
        with open(img_path, "rb") as f:
            img_data = f.read()
            img_blob = self.bsky_api.upload_blob(img_data).blob
        logging.info("Media ready with ids: %s", img_path)
        logging.info("Text to post: %s", text)
        logging.info("Article id: %s", article_id)
        (parent_ref, root_ref) = self.get_bsky_parent(article_id, column)
        logging.info("Parent ref: %s", parent_ref)
        logging.info("Root ref: %s", root_ref)
        if parent_ref is None:
            # No parent, let's start a new thread
            logging.info("Posting url: %s", url)
            post = self.bsky_api.send_post(
                "", embed=self.bsky_website_card(article_data)
            )
            root_ref = models.create_strong_ref(post)
            parent_ref = root_ref

        logging.info("Replying to: %s", parent_ref)

        # Prepare an image upload with aspect ratio hints
        with Image.open(img_path) as img:
            aspect_ratio = models.AppBskyEmbedDefs.AspectRatio(
                height=img.height,
                width=img.width
            )
        image_embed = models.AppBskyEmbedImages.Image(
            alt=alt_text,
            image=img_blob,
            aspect_ratio=aspect_ratio
        )
        post = self.bsky_api.send_post(
            text=text,
            embed=models.AppBskyEmbedImages.Main(images=[image_embed]),
            reply_to=models.AppBskyFeedPost.ReplyRef(parent=parent_ref, root=root_ref),
        )
        child_ref = models.create_strong_ref(post)
        logging.info("Id to store: %s", child_ref)
        self.update_bsky_db(article_id, child_ref, root_ref, column)

    def get_page(self, url, header=None, payload=None):
        for x in range(MAX_RETRIES):
            try:
                r = requests.get(url=url, headers=header, params=payload)
            except BaseException as e:
                if x == MAX_RETRIES - 1:
                    print("Max retries reached")
                    logging.warning("Max retries for: %s", url)
                    return None
                if "104" not in str(e):
                    print("Problem with url {}".format(url))
                    print("Exception: {}".format(str(e)))
                    logging.exception("Problem getting page")
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
        return bleach.clean(html_str, tags=tags, attributes=attr, strip=strip)

    def show_diff(self, old, new):
        if old is None or new is None or len(old) == 0 or len(new) == 0:
            logging.info("Old or New empty")
            return False
        new_hash = hashlib.sha224(new.encode("utf8")).hexdigest()
        html_diff_result = html_diff(old, new)
        logging.info(html_diff_result)
        if "</ins>" not in html_diff_result and "</del>" not in html_diff_result:
            logging.info("No diff to show")
            return False
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
        """.format(
            html_diff_result
        )
        with TemporaryDirectory(delete=False) as tmpdir:
            tmpfile = os.path.join(tmpdir, "tmp.html")
            with open(tmpfile, "w") as f:
                f.write(html)
            for d in ["css", "fonts", "img"]:
                shutil.copytree(d, os.path.join(tmpdir, d))
            opts = webdriver.chrome.options.Options()
            opts.add_argument("--headless")
            driver = webdriver.Chrome(options=opts)
            driver.get("file://{}".format(tmpfile))
            logging.info("tmpfile is %s", tmpfile)

        e = driver.find_element(By.XPATH, "//p")
        timestamp = str(int(time.time()))
        self.filename = timestamp + new_hash
        e.screenshot("./output/" + self.filename + ".png")
        return True

    def __str__(self):
        return "\n".join(self.urls)


class NYTParser(BaseParser):
    def __init__(self, nyt_api_key, api, client, bsky_api=None):
        BaseParser.__init__(self, api, client, bsky_api=bsky_api)
        self.urls = ["https://api.nytimes.com/svc/topstories/v2/home.json"]
        self.payload = {"api-key": nyt_api_key}
        self.articles_table = self.db["nyt_ids"]
        self.versions_table = self.db["nyt_versions"]

    def get_thumbnail(self, article):
        # Return the URL for the first thumbnail image in the article.
        # Choose the largest sub-600-pixel image available (Bluesky thumbnails
        # are resized to 560 pixels)
        thumb_url = None
        thumb_width = 0
        if article.get('multimedia'):
            for m in article['multimedia']:
                if m['type'] != 'image':
                    continue
                if m['width'] > 600:
                    continue
                if m['width'] > thumb_width:
                    thumb_width = m['width']
                    thumb_url = m['url']
        return thumb_url

    def json_to_dict(self, article):
        article_dict = dict()
        if not article.get("short_url") and not article.get("uri"):
            return None
        article_dict["short_url"] = article["short_url"].split("/")[-1]
        article_dict["article_id"] = article["uri"]
        if "html>" in article_dict["short_url"]:
            logging.warning("Problem extracting short_url of: %s", article)
            return None
        article_dict["url"] = article["url"]
        article_dict["title"] = article["title"]
        article_dict["abstract"] = self.strip_html(article["abstract"])
        article_dict["byline"] = article["byline"]
        article_dict["kicker"] = article["kicker"]
        article_dict["thumbnail"] = self.get_thumbnail(article)
        od = collections.OrderedDict(sorted(article_dict.items()))
        article_dict["hash"] = hashlib.sha224(
            repr(od.items()).encode("utf-8")
        ).hexdigest()
        article_dict["date_time"] = datetime.now(LOCAL_TZ)
        return article_dict

    def generate_alt_text(self, old, new):
        return "Before: {}\nAfter: {}".format(old, new)

    def store_data(self, data):
        if self.articles_table.find_one(article_id=data["article_id"]) is None:  # New
            article = {
                "article_id": data["article_id"],
                "add_dt": data["date_time"],
                "status": "home",
                "thumbnail": data["thumbnail"],
                "tweet_id": None,
                "post_uri": None,
                "post_cid": None,
                "root_uri": None,
                "root_cid": None,
            }
            self.articles_table.insert(article)
            logging.info("New article tracked: %s", data["url"])
            data["version"] = 1
            self.versions_table.insert(data)
        else:
            # re insert
            if (
                self.articles_table.find_one(
                    article_id=data["article_id"], status="removed"
                )
                is not None
            ):
                article = {
                    "article_id": data["article_id"],
                    "add_dt": data["date_time"],
                }

            count = self.versions_table.count(
                self.versions_table.table.columns.article_id == data["article_id"],
                hash=data["hash"],
            )
            if count == 1:  # Existing
                pass
            else:  # Changed
                result = self.db.query(
                    'SELECT * \
                                       FROM nyt_versions\
                                       WHERE article_id = "%s" \
                                       ORDER BY version DESC \
                                       LIMIT 1'
                    % (data["article_id"])
                )
                for row in result:
                    data["version"] = row["version"] + 1
                    self.versions_table.insert(data)
                    url = data["url"]
                    if (
                        row["url"].split("nytimes.com/")[1]
                        != data["url"].split("nytimes.com/")[1]
                    ):
                        if self.show_diff(
                            row["url"].split("nytimes.com/")[1],
                            data["url"].split("nytimes.com/")[1],
                        ):
                            tweet_text = "Change in URL"
                            old_text = row["url"].split("nytimes.com/")[1]
                            new_text = data["url"].split("nytimes.com/")[1]
                            alt_text = self.generate_alt_text(old_text, new_text)
                            self.bsky_post(
                                tweet_text, data, "article_id", alt_text)
                            self.tweet(
                                tweet_text,
                                data["article_id"],
                                url,
                                "article_id",
                                alt_text,
                            )
                    if row["title"] != data["title"]:
                        if self.show_diff(row["title"], data["title"]):
                            tweet_text = "Change in Headline"
                            alt_text = self.generate_alt_text(
                                row["title"], data["title"]
                            )
                            self.bsky_post(
                                tweet_text, data, "article_id", alt_text)
                            self.tweet(
                                tweet_text,
                                data["article_id"],
                                url,
                                "article_id",
                                alt_text,
                            )
                    if row["abstract"] != data["abstract"]:
                        if self.show_diff(row["abstract"], data["abstract"]):
                            tweet_text = "Change in Abstract"
                            alt_text = self.generate_alt_text(
                                row["abstract"], data["abstract"]
                            )
                            self.bsky_post(
                                tweet_text, data, "article_id", alt_text)
                            self.tweet(
                                tweet_text,
                                data["article_id"],
                                url,
                                "article_id",
                                alt_text,
                            )
                    if row["kicker"] != data["kicker"]:
                        if self.show_diff(row["kicker"], data["kicker"]):
                            tweet_text = "Change in Kicker"
                            alt_text = self.generate_alt_text(
                                row["kicker"], data["kicker"]
                            )
                            self.bsky_post(
                                tweet_text, data, "article_id", alt_text)
                            self.tweet(
                                tweet_text,
                                data["article_id"],
                                url,
                                "article_id",
                                alt_text,
                            )
        return data["article_id"]

    def loop_data(self, data):
        if "results" not in data:
            return False
        for article in data["results"]:
            try:
                article_dict = self.json_to_dict(article)
                if article_dict is not None and "/zh-hans/" not in article_dict["url"]:
                    article_id = self.store_data(article_dict)
                    self.current_ids.add(article_id)
            except BaseException as e:
                logging.exception("Problem looping NYT: %s", article)
                print("Exception: {}".format(str(e)))
                print("***************")
                print(article)
                print("***************")
                return False
        return True

    def parse_pages(self):
        r = self.get_page(self.urls[0], None, self.payload)
        if r is None or len(r.text) == 0:
            logging.warning("Empty response NYT")
            return
        if r.status_code != 200:
            logging.warning(f"Non 200 response: {r.status_code}, text: {r.text}")
        try:
            data = json.loads(r.text, strict=False)
        except BaseException as e:
            logging.exception("Problem parsing page: %s", r.text)
            print(e)
            print(len(r.text))
            print(type(r.text))
            print(r.text)
            print("----")
            return
        loop = self.loop_data(data)
        if loop:
            self.remove_old("article_id")


def main():
    # logging
    logging.basicConfig(
        filename=LOG_FOLDER + "titlediff.log",
        format="%(asctime)s %(name)13s %(levelname)8s: " + "%(message)s",
        level=logging.INFO,
    )
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.info("Starting script")

    nyt_api = None
    nyt_client = None
    if os.environ.get("NYT_TWITTER_CONSUMER_KEY"):
        consumer_key = os.environ["NYT_TWITTER_CONSUMER_KEY"]
        consumer_secret = os.environ["NYT_TWITTER_CONSUMER_SECRET"]
        access_token = os.environ["NYT_TWITTER_ACCESS_TOKEN"]
        access_token_secret = os.environ["NYT_TWITTER_ACCESS_TOKEN_SECRET"]
        bearer_token = os.environ["NYT_BEARER_TOKEN"]
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.secure = True
        auth.set_access_token(access_token, access_token_secret)
        nyt_api = tweepy.API(auth)
        nyt_client = tweepy.Client(
            bearer_token=bearer_token,
            consumer_key=consumer_key,
            consumer_secret=consumer_secret,
            access_token=access_token,
            access_token_secret=access_token_secret,
        )
        logging.debug("NYT Twitter API configured")

    bsky_api = None
    if "BLUESKY_LOGIN" in os.environ:
        bsky_login = os.environ["BLUESKY_LOGIN"]
        bsky_passwd = os.environ["BLUESKY_PASSWD"]
        bsky_api = Client(base_url="https://bsky.social")
        try:
            bsky_api.login(bsky_login, bsky_passwd)
        except:
            logging.exception("Bluesky login failed")
            return

    try:
        logging.debug("Starting NYT")
        nyt_api_key = os.environ["NYT_API_KEY"]
        nyt = NYTParser(
            nyt_api_key=nyt_api_key, api=nyt_api, client=nyt_client, bsky_api=bsky_api
        )
        nyt.parse_pages()
        logging.debug("Finished NYT")
    except:
        logging.exception("NYT")

    logging.info("Finished script")


if __name__ == "__main__":
    main()
