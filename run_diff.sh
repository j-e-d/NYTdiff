#!/bin/bash
export TESTING=True

export NYT_TWITTER_CONSUMER_KEY=""
export NYT_TWITTER_CONSUMER_SECRET=""
export NYT_TWITTER_ACCESS_TOKEN=""
export NYT_TWITTER_ACCESS_TOKEN_SECRET=""

export NYT_API_KEY=""
export RSS_URL=""

export PHANTOMJS_PATH="./"

python nytdiff.py
