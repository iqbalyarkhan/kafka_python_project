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
consumer_key = ""
consumer_secret = ""
access_token = ""
access_token_secret = ""
#Import the necessary methods from tweepy library

class StdOutListener(StreamListener):
    def on_data(self, data):
        producer.send_messages("trump", data.encode('utf-8'))
        # print (data)
        return True
    def on_error(self, status):
        print (status)

kafka = KafkaClient("localhost:9092")
producer = SimpleProducer(kafka)
l = StdOutListener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, l)
stream.filter(track="trump")



#*************** THIS WORKS BY ITSELF *******************#
#This is a basic listener that just prints received tweets to stdout.
# class StdOutListener(StreamListener):

#     def on_data(self, data):
#         print data
#         return True

#     def on_error(self, status):
#         print status


# if __name__ == '__main__':

#     #This handles Twitter authetification and the connection to Twitter Streaming API
#     l = StdOutListener()
#     auth = OAuthHandler(consumer_key, consumer_secret)
#     auth.set_access_token(access_token, access_token_secret)
#     stream = Stream(auth, l)

#     #This line filter Twitter Streams to capture data by the keywords: 'python', 'javascript', 'ruby'
#     stream.filter(track=['python', 'javascript', 'ruby'])
#     producer = KafkaProducer(bootstrap_servers='localhost:8080')
#     topic = "twitter"
#     producer.send(topic, stream)

#*************** THIS WORKS BY ITSELF *******************#
# from tweepy.streaming import StreamListener
# from tweepy import OAuthHandler
# from tweepy import Stream
# from kafka import SimpleProducer, KafkaClient

# consumer_key = "ZDi0OgYJDMX2M2LRT5IuQxksK"
# consumer_secret = "GCatF6dlW8IWourUqwq242eM4P7Cv7IzdI2CnEKPidHeSWhaab"
# access_token = "118624642-anzPcTxzNJvv5gbNUUswe7Ttf4rn7kw8vmesjQAW"
# access_secret = "3MHZP7fSEqkjC4nxf8YSCJsMtLZkU6xKKIolpvkZ7gkVR"

# class StdOutListener(StreamListener):
#     def on_data(self, data):
#         producer.send_messages("trump", data.encode('utf-8'))
#         print (data)
#         return True
#     def on_error(self, status):
#         print (status)

# kafka = KafkaClient("localhost:9092")
# producer = SimpleProducer(kafka)
# l = StdOutListener()
# auth = OAuthHandler(consumer_key, consumer_secret)
# auth.set_access_token(access_token, access_secret)
# stream = Stream(auth, l)
# stream.filter(track="trump")