
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


# app = Flask(__name__)
# @app.route("/")
# def home():
#     # set-up a Kafka consumer
#     consumer = KafkaConsumer('trump')
#     # tweets,conn, dbcur = initialize(db_name = "tweetdata")
#     for msg in consumer:
#         # output = []
#         # output.append(json.loads(msg.value))
#         return json.dumps(json.loads(msg.value))
#         # print '\n'
#     # return "Hello, World!"
# @app.route("/salvador")
# def salvador():
#     return "Hello, Salvador"
# if __name__ == "__main__":
#     app.run(debug=True)

def main():
    '''
    main function initiates a kafka consumer, initialize the tweetdata database.
    Consumer consumes tweets from producer extracts features, cleanses the tweet text,
    calculates sentiments and loads the data into postgres database
    '''
    # set-up a Kafka consumer
    consumer = KafkaConsumer('trump')
    # tweets,conn, dbcur = initialize(db_name = "tweetdata")
    tweets = pd.DataFrame()
    langDict = {}
    for msg in consumer:
        # print msg.value['timestamp_ms']
        # output = []
        # output.append(json.loads(msg.value))
        tweet = json.loads(msg.value)
        # print (list(tweet.keys()))
        # print (tweet)
        if 'text' in tweet:
            # if tweet['lang'] in langDict:
            #     langDict[tweet['lang']] += 1
            #     displayGraph(langDict)
            # else:
            #     langDict[tweet['lang']] = 1
            #     displayGraph(langDict)
            cleaned_tweet = ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet['text']).split())
            print (cleaned_tweet)
            # displayGraph(tweet['lang'])
        # tweets['text'] = map(lambda tweet: tweet['t ext'])
        # tweets['lang'] = map(lambda tweet: tweet['lang'])
        # print output
        # print '\n'

        # # Function to extract features from tweets
        # extracttweetfeatures(tweets, output)

        # # Text cleaning
        # cleantweettext(tweets)

        # # Calculate sentiment using nltk vader sentiment library
        # calculatesentiments(tweets)

        # # Create data frame
        # cleanse_dataframe_and_load(tweets, conn, dbcur)


# def displayGraph(langDict):
#     # fig, ax = plt.subplots()
#     # ax.tick_params(axis='x', labelsize=15)
#     # ax.tick_params(axis='y', labelsize=10)
#     # ax.set_xlabel('Languages', fontsize=15)
#     # ax.set_ylabel('Number of tweets' , fontsize=15)
#     # ax.set_title('Top 5 languages', fontsize=15, fontweight='bold')
#     # langDict[:5].plot(ax=ax, kind='bar', color='red')
#     plt.plot(langDict.keys(),langDict.values())
#     plt.title('ID model model accuracy')
#     plt.ylabel('number')
#     plt.xlabel('language')
#     plt.legend(['train', 'test'], loc='upper left')
#     plt.savefig('ID modelo: model accuracy.png')


if __name__ == "__main__":
    # app.run(debug=True)
    main()