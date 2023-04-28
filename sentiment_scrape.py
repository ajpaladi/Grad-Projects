import os
from datetime import date
import pandas as pd
import threading
import itertools
from datetime import datetime, timedelta
from datetime import date
import plotly.express as px
import tweepy

#################################
#### PART 1: Authentication #####
#################################

twitter_auth_keys = {
    "consumer_key"        : "MNn3GpSJiaqHVaUIvMR4KRyc5",
    "consumer_secret"     : "lzlHFVqNb5tLSpsyYrQBJqDEfkuQxjgPKXZreGNtZTP7QIsm4o",
    "access_token"        : "1123779250014834690-QvukGyvVNNivc84QoZIERY67pVYob3",
    "access_token_secret" : "vpT9fRzn3QnqpbqE0761b7RVc77WRBh3p4dYqVVWgQZwS"
}

auth = tweepy.OAuthHandler(
        twitter_auth_keys['consumer_key'],
        twitter_auth_keys['consumer_secret']
        )
auth.set_access_token(
        twitter_auth_keys['access_token'],
        twitter_auth_keys['access_token_secret']
        )
api = tweepy.API(auth)

#################################
#### PART 2: Tweet Frequency ####
#################################

#general tweet frequency / per week / per day 

news_dict = {'agency':[], 'tweets_w':[], 'tweets_d':[], 'tweets_h':[]}
news_list = ['cnn', 'foxnews', 'washingtonpost', 'reuters']
for n in news_list:
    print(n)
    tweets = tweepy.Cursor(api.user_timeline, screen_name=n, exclude_replies=True, tweet_mode='extended').items(5000)
    tweets_list = []
    for tweet in tweets:
        tweets_list.append(tweet.full_text)
    agency = n
    tweets_w = len(tweets_list)
    tweets_d = len(tweets_list)/7
    tweets_h = len(tweets_list)/168
    news_dict['agency'].append(agency)
    news_dict['tweets_w'].append(tweets_w)
    news_dict['tweets_d'].append(tweets_d)
    news_dict['tweets_h'].append(tweets_h)
    
report = pd.DataFrame(dict([ (k,pd.Series(v)) for k,v in news_dict.items() ]))
report.to_csv('frequency.csv')

###############################
#### PART 3: Tweet Content ####
###############################

#Tucker Carlson fired    
#Don Lemon fired   
#Joe Biden reelection   
#trump indictment   

news_dict = {'agency':[], 'topic':[], 'tweet_count':[], 'tweet_content':[]}
a_list = ['cnn', 'foxnews', 'washingtonpost', 'reuters']
s_words = ['Tucker Carlson', 'Don Lemon', 'Joe Biden', 'Donald Trump']

for a in a_list:
    for sw in s_words:
        tweets = tweepy.Cursor(api.search_tweets, q=sw + " from:" + a, lang="en", tweet_mode='extended').items(1000)
        tweets_list = []
        for tweet in tweets:
            tweets_list.append(tweet.full_text)
        agency = a
        topic = sw
        tweet_count = len(tweets_list)
        tweet_content = tweets_list
        news_dict['agency'].append(agency)
        news_dict['topic'].append(topic)
        news_dict['tweet_count'].append(tweet_count)
        news_dict['tweet_content'].append(tweet_content)
    
report = pd.DataFrame(dict([ (k,pd.Series(v)) for k,v in news_dict.items() ]))
report = report.explode('tweet_content')
report.to_csv('content.csv')

####################################
#### PART 4: Sentiment Analysis ####
####################################

#performing sentiment analysis

import nltk
#nltk.download('vader_lexicon')
from nltk.sentiment.vader import SentimentIntensityAnalyzer

tweets_df = pd.read_csv('content.csv')
tweets_df = tweets_df.dropna(subset=['tweet_content'])

sid = SentimentIntensityAnalyzer()


def get_sentiment_score(tweet):
    return sid.polarity_scores(tweet)['compound']

tweets_df['sentiment_score'] = tweets_df['tweet_content'].apply(get_sentiment_score)
tweets_df.to_csv('sentiment.csv')

#groupby
result = tweets_df.groupby(['agency', 'topic']).mean()

