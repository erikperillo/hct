#/usr/bin/env python3

#import ggplot as ggp
import matplotlib.animation as am
from matplotlib import style
style.use("ggplot")
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import threading
import datetime
import socket
import random
import oarg
import time
import json
import sys
import os
#spark api
try:
    from pyspark import SparkContext
    from pyspark.streaming import StreamingContext
except ImportError:
    print("WARNING: could not import pyspark")
#twitter streaming api
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

#file directory
FILE_DIR = os.path.dirname(os.path.realpath(__file__))

#supported locations for filtering
LOCATIONS = {
    #this area covers the state of sao paulo and part of mg, rj and pr (brazil)
    "sp": [-53.342250, -25.271552, -43.388637, -19.235468]
}

def get_twitter_keys(filename, delim=","):
    """
    Gets authentication keys for twitter API use.
    Expects format: acc_tok,acc_tok_sec,cons_key,cons_sec
    """
    with open(filename, "r") as f:
        return f.read().strip().split(delim)

def is_hashtag(word):
    return word.startswith("#")

#basic listener that just prints received tweets to stdout.
class StdOutListener(StreamListener):
    def __init__(self, conn, debug=False):
        self.conn = conn
        self.debug = debug

    def on_data(self, data):
        """
        Handles new data comming from twitter
        """
        try:
            #converting data to a dict type
            tweet = json.loads(data)
            #getting message content of tweet
            message = tweet["text"]
            if self.debug:
                print("@%s: '%s'" % (tweet["user"]["screen_name"], message))
            #splitting message into words
            words = message.lower().strip().split()
            #print(words)
            #sending each word via socket
            for word in words:
                buff = (word + "\n").encode("utf-8")
                self.conn.sendall(buff)
            #time.sleep(0.1*random.randint(1, 30))
            return True
        except:
            return False

    def on_error(self, status):
        print(status)

class TwitterThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def set_socket(self, host="", port=0, listen_n=5):
        """
        Sets up TCP socket between twitter data and Spark.
        """
        #initializing
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        dest = (host, port)
        #binding
        self.sock.bind(dest)
        #listening
        self.sock.listen(listen_n)

        return self.sock.getsockname()

    def set_stream_params(self, auth_keys, filters=None, debug=False):
        self.auth_keys = auth_keys
        self.filters = filters
        self.debug = debug

    def get_twitter_stream(self, debug=False):
        """
        Sets twitter streaming listener.
        """
        #getting connection from spark
        conn, client = self.sock.accept()
        #print("connected to", client)

        #getting listener
        listener = StdOutListener(conn, debug=debug)
        #getting keys for authentication
        acc_tok, acc_tok_sec, cons_key, cons_sec = self.auth_keys
        #authentication
        auth = OAuthHandler(cons_key, cons_sec)
        auth.set_access_token(acc_tok, acc_tok_sec)

        #getting stream
        stream = Stream(auth, listener)

        return stream

    def run(self):
        """
        Routine that gets data from twitter.
        """
        #getting stream object
        stream = self.get_twitter_stream(self.debug) 

        #getting twitter stream
        if self.filters:
            stream.filter(**self.filters)
        else:
            stream.sample()

def get_hashtag(text):
    """
    Maps hashtag into pair (hashtag, 1).
    """
    try:
        if is_hashtag(text):
            return (text, 1)
        else:
            return ("None", 1)
    except:
        return ("Error", 1)

def print_sorted(time, rdd, num=10):
    """
    Prints RDD in sorted order.
    """
    sorted_rdd = rdd.sortBy(lambda x: x[1], ascending=False)

    taken = sorted_rdd.take(num)
    print("-----------")
    for rec in taken:
        print(rec[0], "->", rec[1])
    print("-----------")

df = pd.DataFrame()
df.index.name = "time"
sdf = pd.DataFrame()
fig, ax = plt.subplots()
ax.set_ylim(0.0, 30)
ax.set_xlim(0.0, 100)
line = ax.plot([], [], lw=2)[0]
updated = False

def df_update():
    global sdf
    global updated

    while True:
        if updated:
            #print("sdf:", sdf)
            print("loop: updated")
            updated = False
            yield sdf

        time.sleep(0.2)

def run(data):
    global line

    xmin, xmax = ax.get_xlim()
    ymin, ymax = ax.get_ylim()

    print("line=", line)
    line.set_xdata(data["time"])
    line.set_ydata(data["frequency"])

    if(max(data["time"]) >= xmax - 10):
        ax.set_xlim(xmin+10, xmax+10)
    if(max(data["frequency"]) >= ymax - 10):
        ax.set_ylim(ymin+10, ymax+10)

    return line

def compute_df(time, rdd, start, num=10, exclude="None", max_rows=1000):
    """
    Shows tweets.
    """
    global df
    global sdf
    global updated

    rdd = rdd.filter(lambda x: x[0] != exclude)
    sorted_rdd = rdd.sortBy(lambda x: x[1], ascending=False)
    taken = sorted_rdd.take(num)

    print("time:", time)
    try:
        hts, freq = zip(*taken)
        print("hts:", hts)
        print("freq:", freq)
    except ValueError:
        return

    for ht in hts:
        if not ht in df:
            df[ht] = len(df)*[0]

    for col in df:
        if col != "time" and not col in hts:
            print("dropping col", col)
            df.drop(col, 1, inplace=True)
    
    elapsed = (time - start).total_seconds()
    #print([[elapsed] + list(freq)], ["time"] + list(hts))
    #new_col = pd.DataFrame([[elapsed] + list(freq)], columns=["time"] + list(hts))
    new_col = pd.DataFrame([list(freq)], columns=list(hts))
    new_col.index = [elapsed]
    new_col.index.name = "time"
    #print(new_col)
    #print()
    df = df.append(new_col)
    df = df[-max_rows:]
    #print(df, "\nrindex:")
    sdf = df.stack().reset_index()
    sdf.columns = ["time", "hashtag", "frequency"]
    updated = True
    #print(sdf)
    #rint("SIZE:", len(sdf))
    #plt = ggp.ggplot(sdf, ggp.aes(x="time", y="frequency")) 
    #plt += ggp.geom_point()
    #print(plt)
    #print(plt)

def get_locations_dict(locations):
    locs = []

    if not locations:
        return {} 

    for loc in locations:
        if not loc in LOCATIONS:
            error("no location '%s'\navailable locations: %s" %\
                (loc, ", ".join(list(LOCATIONS.keys()))))
        locs.extend(LOCATIONS[loc])

    return {"locations": locs}

def get_keywords_dict(keywords):
    if not keywords:
        return {}
    return {"track": keywords} 

def get_filters_dict(locations_str, keywords_str):
    filters = {}
    locations = locations_str.split(",") if locations_str else []
    keywords = keywords_str.split(",") if keywords_str else []
    
    filters.update(get_locations_dict(locations)) 
    filters.update(get_keywords_dict(keywords))
    
    return filters

def error(msg, code=1):
    print("error:", msg)
    exit(code)

def main():
    #command line arguments
    sock_host = oarg.Oarg("-h --host", "", "socket host name", 0)
    sock_port = oarg.Oarg("-p --port", 0, "socket port number", 1)
    auth_file = oarg.Oarg("-a --auth-file", 
        os.path.join(FILE_DIR, "..", "twitter.auth"), 
        "twitter authentication file", 2)
    locations = oarg.Oarg("-l --locations", "", 
        "locations to filter (comma-separated, no space)")
    keywords = oarg.Oarg("-k --keywords", "", 
        "keywords to filter (comma-separated, no space)")
    batch_interval = oarg.Oarg("-i --batch-interval", 2.0, "batch interval")
    show_tweets = oarg.Oarg("-s --show-tweets", False, "show tweets")
    hlp = oarg.Oarg("-h --help", False, "this help message")

    oarg.parse(delim=":")

    #help message
    if hlp.val:
        oarg.describe_args("options:", def_val=True)
        exit()

    #initializing sparkcontext with a name
    spc = SparkContext(appName="HashTagCounter")
    #creating streamingcontext with selected batch interval 
    stc = StreamingContext(spc, batch_interval.val)
    #checkpointing feature
    stc.checkpoint("checkpoint")
    #remove INFO msg
    spc.setLogLevel("ERROR")

    #getting socket thread
    twitter_thr = TwitterThread()
    #setting up stream parameters
    twitter_auth_keys = get_twitter_keys(auth_file.val)
    filters = get_filters_dict(locations.val, keywords.val)
    twitter_thr.set_stream_params(twitter_auth_keys, filters, show_tweets.val)
    print("will filter tweets using:", filters)
    #setting up socket
    host, port = twitter_thr.set_socket(sock_host.val, sock_port.val)

    #creating a DStream to connect to hostname:port
    lines = stc.socketTextStream(host, port)
    #function used to update the state
    updateFunction = \
        lambda new_values, running_count: sum(new_values) + (running_count or 0)
    #update all the current counts of hashtags
    running_counts = lines.map(get_hashtag).updateStateByKey(updateFunction)

    #twitter thread start
    twitter_thr.start()

    start = datetime.datetime.now()
    compute = lambda time, rdd: compute_df(time, rdd, start)
    #print the current state
    #running_counts.foreachRDD(print_sorted)
    running_counts.foreachRDD(compute)

    #start the computation
    stc.start()

    global fig
    ani = am.FuncAnimation(fig, run, df_update)
    #for __ in df_update():
    #    pass
    plt.show()

    #wait for the computation to terminate
    stc.awaitTermination()

def lol(df):
    df["x"] = np.array(range(10, 20))
    print(df["x"], type(df["x"].values))

def test():
    x = range(10)
    df = pd.DataFrame({"x": x, "y": [_x**2 for _x in x]})
    print(df) 
    lol(df)
    print(df) 

if __name__ == "__main__":
    main()
    #test()
