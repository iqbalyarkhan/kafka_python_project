from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import SimpleProducer, KafkaClient

# client = SimpleClient("localhost:9092")
# producer = SimpleProducer(client)

#Import the necessary methods from tweepy library

class StdOutListener(StreamListener):
    def on_data(self, data):
        producer.send_messages("manchester", data.encode('utf-8'))
        return True
    def on_error(self, status):
        print (status)

if __name__ == '__main__':
    kafka = KafkaClient("localhost:9092")
    producer = SimpleProducer(kafka)
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)
    stream.filter(track=['manchester'])
