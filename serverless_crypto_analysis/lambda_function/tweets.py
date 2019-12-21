import json
import logging
import os
import random

import boto3
import requests
from bs4 import BeautifulSoup
from twython import Twython

from serverless_crypto_analysis.utils import s3_utils


def get_tweet(rank, **kwargs):
    default_tweet = '{} is a #cryptonewcomer in the top {} coins for the first time ever. Congratulations {} !' \
                    '\nfollow @DeltaCryptoClu2 to know more newcomers'.format(kwargs.get('name'), rank,
                                                                              kwargs.get('tweet_id'))

    n50_1 = '{} is now in the top {} for the first time ever! {} is playing with the big boys now ... ' \
            '\n#cryptonewcomer\ndeltacryptoclub.com'.format(kwargs.get('name'), rank,
                                                            kwargs.get('tweet_id'))

    n200_1 = '{} just made it in the top {}. {} is steadily coming out of the dark and making a name' \
             '\n#cryptonewcomer\ndeltacryptoclub.com'.format(kwargs.get('name'), rank,
                                                             kwargs.get('tweet_id'))

    n100_1 = '{} just crossed the top {} of coinmarketcap coins. {} things are getting serious' \
             '\n#cryptonewcomer\ndeltacryptoclub.com'.format(kwargs.get('name'), rank,
                                                             kwargs.get('tweet_id'))

    tweets = {
        50: [default_tweet, n50_1],
        100: [default_tweet, n100_1],
        200: [default_tweet, n200_1]
    }

    tweets_for_rank = tweets.get(rank, default_tweet)
    return random.choice(tweets_for_rank)


def get_tweet_id(id_):
    """
    we get the class twitter-timeline from the social page of coinmarketcap
    :param id_: coinmarketcap coin id
    :return: twitter id
    """
    r = requests.get("https://coinmarketcap.com/currencies/{}/#social".format(id_))
    soup = BeautifulSoup(r.text, 'lxml')
    twitter_cell = soup.find('a', class_='twitter-timeline')
    if twitter_cell is not None:
        twitter_id = '@' + twitter_cell.text.rsplit('@')[1]
        return twitter_id
    return ''


def get_tokens(token_name, type=""):
    conn = boto3.client('secretsmanager')
    secret_string = conn.get_secret_value(SecretId=token_name).get("SecretString")
    if type == "json":
        return json.loads(secret_string)
    return secret_string


def get_coinmarketcap_tokens(newcomer_id, coinmarketcap_token):
    import requests

    url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/info?slug={}".format(newcomer_id)

    payload = 'slug={}'.format(newcomer_id)
    headers = {
        'X-CMC_PRO_API_KEY': coinmarketcap_token,
        'Accept': 'application/json'
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    print(response.text)
    return json.loads(response.text)


def post_tweet(text):
    tokens = get_tokens()
    twitter = Twython(**tokens)
    twitter.update_status(status=text)


def get_key_from_event(event):
    key = event['Records'][0]['s3']['object']['key']
    print("key: {}".format(key))
    return key


def get_bucket_from_event(event):
    bucket = event['Records'][0]['s3']['bucket']['name']
    print("bucket: {}".format(bucket))
    return bucket


def get_rank_from_s3_key(key):
    rank = int(key.split("/")[-2].split("=")[1])
    print("rank: {}".format(rank))
    return rank


def lambda_handler(event, context):
    print("event:{}".format(event))
    twitter_token = get_tokens(os.environ["TWITTER_TOKEN"], "json")
    coinmarketcap_token = get_tokens(os.environ["COINMARKETCAP_TOKEN"])

    bucket = get_bucket_from_event(event)
    key = get_key_from_event(event)
    rank = get_rank_from_s3_key(key)

    newcomer_id = s3_utils.get_object_from_s3(bucket, key)
    coin_data = get_coinmarketcap_tokens(newcomer_id, coinmarketcap_token)
    name = coin_data.get("data").get("1").get("name")
    tweet_id = get_tweet_id(newcomer_id)
    twitter = Twython(**twitter_token)
    kwargs = {'name': name, 'tweet_id': tweet_id}
    text = get_tweet(rank, **kwargs)
    logging.info('tweet : {}'.format(text))

    response = twitter.update_status(status=text)
    logging.info(response)
