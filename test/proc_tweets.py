#!/usr/bin/env python2.7

import json

def is_hashtag(word):
    return word.startswith("#")

def hashtags(text, delim=" "):
    return list(map(lambda w: w[1:], filter(is_hashtag, text.split(delim))))

def sep_hashtags(text):
    ht = hashtags(text)
    return " ".join(filter(lambda w: not is_hashtag(w), text.split())), ht

def update_freq(words_dict, words):
    for word in words:
        try:
            words_dict[word] += 1
        except KeyError:
            words_dict[word] = 1

if __name__ == "__main__":
    filename = "./lel.txt"
    tweets = []
    words_dict = {}

    with open(filename, "r") as f:
        for line in f:
            try:
                tweet = json.loads(line)
                tweets.append(tweet)
            except:
                continue

    for tweet in tweets:
        try:
            msg = tweet["text"].encode("utf-8")
            words = msg.split()
            update_freq(words_dict, words)
            print(msg)
        except:
            continue
            raise

    words_list = [(k, v) for k, v in words_dict.iteritems()]
    words_list.sort(key=lambda x: x[1], reverse=True)
    #for w in words_list:
    #    print w

    print("done")
