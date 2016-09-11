#!/usr/bin/env python3

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

AUTH_FILE = "../twitter.auth"
#this areas covers the state of sao paulo and part of mg, rj and pr, brazil
SP_AREA = [-53.342250,-25.271552,-43.388637,-19.235468]

#expects format acc_tok,acc_tok_sec,cons_key,cons_sec
def get_keys(filename, delim=","):
    with open(filename, "r") as f:
        return f.read().strip().split(delim)

#basic listener that just prints received tweets to stdout.
class StdOutListener(StreamListener):
    def on_data(self, data):
        print(data)
        return True

    def on_error(self, status):
        print(status)

def get_tweets(keys, filters=None):
    #getting user credentials
    access_token, access_token_secret, consumer_key, consumer_secret = keys

    #this handles twitter auth and the connection to twitter streaming api
    listener = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    #getting stream object
    stream = Stream(auth, listener)

    #getting twitter stream
    if not filters:
        stream.sample()
    else:
        stream.filter(**filters)

if __name__ == '__main__':
    keys = get_keys(AUTH_FILE)
    filters = {"locations": SP_AREA}

    get_tweets(keys, filters)
