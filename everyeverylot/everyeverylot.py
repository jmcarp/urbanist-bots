#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import os
import random
from typing import Optional

import tweepy
import twitter_bot_utils


USERS = [
    # "EveryLotDYT",  # inactive
    # "EveryLotEtown",  # inactive
    # "EveryLotMpls",  # inactive
    "everylotCHS",
    "everylotRaleigh",
    "everylotSATX",
    "everylotSAV",
    # "everylot_akron",  # inactive
    "everylot_balt",
    "everylot_cle",
    "everylot_dallas",
    "everylot_dc",
    # "everylot_lorain",  # inactive
    "everylot_rva",
    # "everylotalbany",  # inactive
    # "everylotanaheim",  # inactive
    "everylotaustin",
    "everylotbline",
    "everylotboston",
    # "everylotcambma",  # inactive
    "everylotchicago",
    "everylotcinci",
    "everylotcol",
    # "everylotcuse",  # inactive
    "everylotcville",
    # "everylotftc",  # inactive
    "everylotgal",
    "everylotgary",
    "everylothartfd",
    "everylothtx",
    # "everylotinTLH",  # inactive
    "everylotjc",
    "everylotla",
    "everylotmke",
    "everylotmoco",
    "everylotmotown",
    "everylotnyc",
    "everylotoma",
    "everylotpdx",
    "everylotphilly",
    "everylotpvd",
    "everylotsf",
    "everylotstl",
]

SAMPLE_SIZE = 10


def get_top_tweet(
    api, users, until: datetime.datetime, since: datetime.datetime
) -> Optional[tweepy.Status]:
    tweets = [tweet for user in users for tweet in get_tweets(api, user, until, since)]
    if len(tweets) > 0:
        return max(tweets, key=lambda tweet: tweet.favorite_count + tweet.retweet_count)
    else:
        return None


def get_tweets(api, user: str, until: datetime.datetime, since: datetime.datetime):
    max_id = None
    while True:
        batch = api.search(
            q=f"from:{user}", max_id=max_id, until=until.date(), count=100
        )
        if len(batch) == 0:
            return
        for tweet in batch:
            if since <= tweet.created_at < until:
                yield tweet
            if tweet.created_at < since:
                return
        if len(batch) < 100:
            break
        max_id = batch[-1].id
    return tweets


if __name__ == "__main__":
    api = twitter_bot_utils.API(screen_name="everyeverylot", config_file=os.getenv("TWITTER_CONFIG_PATH"))

    users = random.sample(USERS, SAMPLE_SIZE)
    now = datetime.datetime.utcnow().replace(minute=0, second=0, microsecond=0)
    until = now - datetime.timedelta(days=1)
    since = until - datetime.timedelta(days=1)
    top_tweet = get_top_tweet(api, users, until, since)
    if top_tweet is not None:
        until_str = until.strftime("%Y-%m-%d %H:%M")
        since_str = since.strftime("%Y-%m-%d %H:%M")
        tweet_url = (
            f"https://twitter.com/{top_tweet.user.screen_name}/status/{top_tweet.id}"
        )
        status = f"Top lot bot tweet from {since_str} to {until_str}: {tweet_url}"
        api.update_status(status=status)
