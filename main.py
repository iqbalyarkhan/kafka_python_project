from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer
from kafka.client import SimpleClient
from kafka.consumer import SimpleConsumer
from kafka.producer import SimpleProducer

client = SimpleClient("localhost:8080")
producer = SimpleProducer(client)
consumer_key = ""
consumer_secret = ""
access_token = ""
access_secret = ""

def main():
            '''
            main function initiates a kafka consumer, initialize the tweetdata database.
            Consumer consumes tweets from producer extracts features, cleanses the tweet text,
            calculates sentiments and loads the data into postgres database
            '''
            # set-up a Kafka consumer
            consumer = KafkaConsumer('movies')
            tweets,conn, dbcur = initialize(db_name = "tweetdata")
            for msg in consumer:
                output = []
                output.append(json.loads(msg.value))
                print output
                print '\n'

                # Function to extract features from tweets
                extracttweetfeatures(tweets, output)

                # Text cleaning
                cleantweettext(tweets)

                # Calculate sentiment using nltk vader sentiment library
                calculatesentiments(tweets)

                # Create data frame
                cleanse_dataframe_and_load(tweets, conn, dbcur)

        if __name__ == "__main__":
        main()
               
               