from functools import lru_cache
from typing import Iterable
import pandas as pd
import tweepy
import praw
from tweepy.models import Status as Tweet
from praw.models.reddit.submission import Submission as RedditPost
import yaml
from datetime import timedelta

config = yaml.safe_load(open("secrets.yaml"))


@lru_cache(maxsize=128)
def get_twitter_client():
    #twitter_auth = tweepy.OAuth1UserHandler(
    #    config["twitter"]["API_key"],
    #    config["twitter"]["API_secret"],
    #    config["twitter"]["access_token"],
    #    config["twitter"]["access_secret"],
    #)
    #cli = tweepy.API(twitter_auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    cli = tweepy.Client(bearer_token=config["twitter"]["token"],
                        consumer_key=config["twitter"]["API_key"],
                        consumer_secret=config["twitter"]["API_secret"],
                        access_token=config["twitter"]["access_token"],
                        access_token_secret=config["twitter"]["access_secret"],
                        wait_on_rate_limit=True)
    return cli


def get_tweets(user, start_time, duration_min):
    cli = get_twitter_client()

    retweet_filter='-filter:retweets'
    q = user + retweet_filter
    tweets = cli.search_all_tweets(q, end_time=start_time, start_time=start_time-timedelta(minutes=duration_min))
    return list(tweets)


def tweets_to_df(tweets: Iterable[Tweet]):
    return pd.DataFrame(
        {
            # "obj": tweet,
            "id": tweet.id_str,
            "created_at": tweet.created_at,
            "source": f"tweeter/{tweet.author.screen_name}",
            "text": tweet.full_text,
            "url": (tweet.entities["urls"] or [{}])[0].get("display_url"),
        }
        for tweet in tweets
    )


@lru_cache(maxsize=128)
def get_reddit_client():
    return praw.Reddit(
        client_id=config["reddit"]["client_id"],
        client_secret=config["reddit"]["client_secret"],
        user_agent="random snouglou",
    )


def get_reddits(subreddit_name, feed, limit):
    assert feed in ["hot", "new", "top"]
    cli = get_reddit_client()
    subreddit = cli.subreddit(subreddit_name)
    return list(getattr(subreddit, feed)(limit=limit))  # subreddit.feed(limit)


def reddits_to_df(submissions: Iterable[RedditPost]):
    return pd.DataFrame(
        {
            # "obj": subm,
            "id": subm.id,
            "created_at": subm.created_utc,
            "source": f"reddit/{subm.subreddit.display_name}",
            "text": subm.title,
            "url": subm.url,
        }
        for subm in submissions
    )
