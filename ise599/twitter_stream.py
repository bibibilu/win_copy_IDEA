# Import the tweepy library
import tweepy
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from emoji import emojize

# Variables that contains the user credentials to access Twitter API
ACCESS_TOKEN = '1181344137271861249-OoHqqVZmcyX0q8dIT5O7GVe0UrM8j0'
ACCESS_SECRET = 'R3tASseldcJ0E6Johe43Xol2tLLZxyJn8K5xJM59nZb10'
CONSUMER_KEY = 'z9dyabTzr1hiGrHmWftycvejj'
CONSUMER_SECRET = 'KkZiDwxHnd7L5GqRMdFTJzJZqRu7M6nY7nPfOgncvKRE8IZlDb'

# Setup tweepy to authenticate with Twitter credentials:

# auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
# auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)

# Create the api to connect to twitter with your creadentials
# api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True, compression=True)


# # # # TWITTER STREAMER # # # #
class TwitterStreamer():
    """
    Class for streaming and processing live tweets.
    """
    def __init__(self):
        pass

    def stream_tweets(self, fetched_tweets_filename, hash_tag_list):
        # This handles Twitter authetification and the connection to Twitter Streaming API
        listener = StdOutListener(fetched_tweets_filename)
        auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
        auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
        stream = Stream(auth, listener)

        # This line filter Twitter Streams to capture data by the keywords:
        stream.filter(track=hash_tag_list)


# # # # TWITTER STREAM LISTENER # # # #
class StdOutListener(StreamListener):
    """
    This is a basic listener that just prints received tweets to stdout.
    """
    def __init__(self, fetched_tweets_filename):
        self.fetched_tweets_filename = fetched_tweets_filename

    def on_data(self, data):
        try:
            print(data)
            with open(self.fetched_tweets_filename, 'a') as tf:
                tf.write(data)
            return True
        except BaseException as e:
            print("Error on_data %s" % str(e))
        return True


    def on_error(self, status):
        print(status)


if __name__ == '__main__':

    # Authenticate using config.py and connect to Twitter Streaming API.
    hash_tag_list = [emojize(':thumbs_up:'),emojize(':thumbs_up:'),emojize(':raised_fist:'),
                     emojize(':raised_hand:'),emojize(':victory_hand:'),
                     emojize('😉'),emojize(':cloud:'),
                     emojize('😡'),emojize(':raised_fist:')]
    fetched_tweets_filename = "tweets.txt"

    twitter_streamer = TwitterStreamer()
    twitter_streamer.stream_tweets(fetched_tweets_filename, hash_tag_list)



# print(emojize(':thumbs_up:'),emojize(':raised_fist:'),
#       emojize(':raised_hand:'),emojize(':victory_hand:'),
#       emojize('😉'),emojize(':cloud:'),
#       emojize('😡'),emojize(':raised_fist:'))
#
# for i in range(0x1f600,0x1f650):
#     print(chr(i),end=" ")
#     if i%16==15:
#         print()