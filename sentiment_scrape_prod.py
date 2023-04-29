import os
from datetime import date
import pandas as pd
import threading
import itertools
from datetime import datetime, timedelta
from datetime import date
import plotly.express as px
import tweepy
from pyspark.sql import SparkSession
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer

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

spark = SparkSession.builder.appName("NewsFrequency").getOrCreate()
sc = spark.sparkContext
news_list = ['cnn', 'foxnews', 'washingtonpost', 'reuters']

def get_frequency_values(n):
    tweets = tweepy.Cursor(api.user_timeline, screen_name=n, exclude_replies=True, tweet_mode='extended').items(5000)
    tweets_list = []
    for tweet in tweets:
        tweets_list.append(tweet.full_text)
    agency = n
    tweets_w = len(tweets_list)
    tweets_d = len(tweets_list)/7
    tweets_h = len(tweets_list)/168
    return [agency, tweets_w, tweets_d, tweets_h]

news_rdd = sc.parallelize(news_list)

frequency_rdd = news_rdd.map(get_frequency_values)

frequency = frequency_rdd.toDF(["agency", "tweets_w", "tweets_d", "tweets_h"])
frequency.show()

frequency.write.csv("frequency.csv", header=True)

###############################
#### PART 3: Tweet Content ####
###############################

a_list = ['cnn', 'foxnews', 'washingtonpost', 'reuters']
s_words = ['Tucker Carlson', 'Don Lemon', 'Joe Biden', 'Donald Trump']

spark = SparkSession.builder.appName("TweetContent").getOrCreate()

# create RDD from a_list and s_words
a_rdd = spark.sparkContext.parallelize(a_list)
sw_rdd = spark.sparkContext.parallelize(s_words)

# apply cartesian transformation to RDDs
asw_rdd = a_rdd.cartesian(sw_rdd)

# define function to retrieve tweet content
def get_tweet_content(a, sw):
    tweets = tweepy.Cursor(api.search_tweets, q=sw + " from:" + a, lang="en", tweet_mode='extended').items(1000)
    tweets_list = []
    for tweet in tweets:
        tweets_list.append(tweet.full_text)
    return (a, sw, len(tweets_list), tweets_list)

# apply map transformation to RDD
content_rdd = asw_rdd.map(lambda x: get_tweet_content(x[0], x[1]))

# convert RDD to PySpark DataFrame
content = content_rdd.toDF(["agency", "topic", "tweet_count", "tweet_content"])

# explode tweet_content column
content = content.withColumn("tweet_content", explode("tweet_content"))
content.show()

# save DataFrame to CSV file
content.write.csv("content.csv", header=True)

####################################
#### PART 4: Sentiment Analysis ####
####################################

spark = SparkSession.builder.appName("TweetSentiment").getOrCreate()
tweets_df = spark.read.csv("content.csv", header=True)
tweets_df = tweets_df.dropna(subset=['tweet_content'])

sid = SentimentIntensityAnalyzer()
get_sentiment_score = udf(lambda tweet: sid.polarity_scores(tweet)['compound'], DoubleType())
tweets_df = tweets_df.withColumn("sentiment_score", get_sentiment_score("tweet_content"))

tweets_df.write.csv("sentiment.csv", header=True)
result = tweets_df.groupby(['agency', 'topic']).mean()
result.show()
