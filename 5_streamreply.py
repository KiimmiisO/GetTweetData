#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Feb 23 21:56:09 2018

@author: nantanasriphromting
"""

import sys
import tweepy
import json
import psycopg2
import appConfig

# Replace the API_KEY and API_SECRET with your application's key and secret.
auth = tweepy.AppAuthHandler(appConfig.tokenConfig.consumer_key, appConfig.tokenConfig.consumer_secret)

api = tweepy.API(auth, wait_on_rate_limit=True,wait_on_rate_limit_notify=True)

if (not api):
    print ("Can't Authenticate")
    sys.exit(-1)

conn = conn = psycopg2.connect(host=appConfig.dbConfig.hostname, user=appConfig.dbConfig.username, password=appConfig.dbConfig.password, dbname=appConfig.dbConfig.database)
cur = conn.cursor()
cur.execute("SELECT tweet_id,tweet_user_id,in_reply_to_status_id_str,tweet_text from twitter_tweets where in_reply_to_status_id_str is not null and is_process=false")
total_inserted=0
for  tweet_id,tweet_user_id,id_str,t_text in cur.fetchall() :
# Find the last tweet
    in_reply_to_status_id_str=id_str
    #print('in_reply_to_status_id_str:',in_reply_to_status_id_str)
    print('reply text:',tweet_id,'main text:',in_reply_to_status_id_str)
    while True:
        try:
            last_tweet = api.get_status(in_reply_to_status_id_str)
            print('reply text:',in_reply_to_status_id_str,'main text:',last_tweet.in_reply_to_status_id_str)
            cur.execute("INSERT INTO reply_data (tweet_id,reply_to_tweet_id, tweet_user_id,data,loaded_date,is_process) VALUES (%s,%s,%s, %s,CURRENT_DATE,false)", (tweet_id,last_tweet.id, tweet_user_id,json.dumps(last_tweet._json,ensure_ascii=False), ))
            conn.commit()
            total_inserted += 1
            in_reply_to_status_id_str=last_tweet.in_reply_to_status_id_str
            print("total_inserted {0} tweets".format(total_inserted))
            if not last_tweet.in_reply_to_status_id_str:
                print('original')
                break
        except:
            break
    print(tweet_id)
    cur.execute("UPDATE twitter_tweets SET is_process=true WHERE tweet_id={0}".format(tweet_id))
    conn.commit() 
cur.close()
conn.close()  
print('Done.')    
        