
from kafka import KafkaConsumer
import pandas as pd
import json
import pandas as pd
import nltk
import re
from nltk.sentiment.vader import SentimentIntensityAnalyzer as Vader
from flask import Flask
from flask import jsonify
import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt
from textblob import TextBlob

def main():
    '''
    main function initiates a kafka consumer, initialize the tweetdata database.
    Consumer consumes tweets from producer extracts features, cleanses the tweet text,
    calculates sentiments and loads the data into postgres database
    '''
    # set-up a Kafka consumer
    consumer = KafkaConsumer('manchester')

    #create a pandas data frame
    tweets = pd.DataFrame()
    langDict = {}
    for msg in consumer:
        tweet = json.loads(msg.value)
        if ('text' in tweet) and (tweet['lang'] == 'en'):
            cleaned_tweet = ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet['text']).split())
            sentiment = TextBlob(cleaned_tweet).sentiment
            f = open("tweets.txt", "a")
            f.writelines("***********************\n")
            f.write(cleaned_tweet)
            f.write('\n')
            f.write(str(sentiment))
            f.write('\n')

if __name__ == "__main__":
    main()