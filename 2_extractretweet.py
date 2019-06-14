#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb 20 12:34:45 2018

@author: nantanasriphromting
"""


import psycopg2
import time
from datetime import datetime
import appConfig
import json


try:
    print('try to connect')
    conn = psycopg2.connect(host=appConfig.dbConfig.hostname, user=appConfig.dbConfig.username, password=appConfig.dbConfig.password, dbname=appConfig.dbConfig.database)
    cur = conn.cursor()
    cur.execute("SELECT tid,data,hashtag_id from tweets_data where is_process=false")
except:
    print ("I am unable to connect to the database")
    
total_record=0
retweet_data=cur.fetchall()
print("fetch data")
for tid, tweetjson, hashtag_id in retweet_data :
    #print("tid {0}".format(tid))
    #print(tweetjson)
    try:
        if 'retweeted_status' not in tweetjson:
            continue
        # tweetobj = json.loads(tweetjson)
        retweet_id = tweetjson['id']
        retweet_id_str = tweetjson['id_str']
        # print(retweet_id_str)
        retweet_user_id = tweetjson['user']['id']
        retweet_user_id_str = tweetjson['user']['id_str']
        retweet_name = tweetjson['user']['name']
        retweet_screen_name = tweetjson['user']['screen_name']
        # print(retweet_screen_name)
        retweet_text = tweetjson['text']
        created_at = appConfig.set_date(tweetjson['created_at'])
        tweets_user_id = tweetjson['retweeted_status']['user']['id']
        tweets_user_id_str = tweetjson['retweeted_status']['user']['id_str']
        tweets_id = tweetjson['retweeted_status']['id']
        tweets_id_str = tweetjson['retweeted_status']['id_str']
        word_id = hashtag_id

        # print("SELECT retweet_id FROM twitter_retweets WHERE retweet_id={0}".format(retweet_id))
        cur.execute("SELECT retweet_id FROM twitter_retweets WHERE retweet_id={0}".format(retweet_id))
        print("check duplicate {0}".format(retweet_id))
        if cur.fetchone() is None:
            #print("not duplicate")
            cur.execute("INSERT INTO twitter_retweets (retweet_id,retweet_id_str,retweet_user_id,retweet_user_id_str,retweet_name,retweet_screen_name,retweet_text,created_at,tweets_user_id,tweets_user_id_str,tweets_id,tweets_id_str,hashtag_id,loaded_date) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,current_date)", (retweet_id,retweet_id_str,retweet_user_id,retweet_user_id_str,retweet_name,retweet_screen_name,retweet_text,created_at,tweets_user_id,tweets_user_id_str,tweets_id,tweets_id_str,word_id))
            conn.commit()
            total_record=total_record+1
            print('inserted :',retweet_id)
    except Exception as ex:
        print ("somting failed", ex)
        continue
cur.close()
conn.close()    
print('Done:', total_record)