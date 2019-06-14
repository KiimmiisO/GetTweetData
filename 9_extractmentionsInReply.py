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

try:
    print('try to connect')
    conn = psycopg2.connect(host=appConfig.dbConfig.hostname, user=appConfig.dbConfig.username, password=appConfig.dbConfig.password, dbname=appConfig.dbConfig.database)
    cur = conn.cursor()
    cur.execute("SELECT t.hashtag_id,r.data from reply_data r inner join twitter_tweets t on r.tweet_id=t.tweet_id where r.is_process=false")
except:
    print ("I am unable to connect to the database")
    
total_record=0

for word_id, tweet in cur.fetchall() :
    try:
        #----- tweets user
        #print(tweet)
        if len(tweet['entities']['user_mentions']) > 0:
                for mention in tweet['entities']['user_mentions']:
                    tweet_user_id= tweet['user']['id']
                    tweet_user_id_str=tweet['user']['id_str']
                    mention_user_id=mention['id']
                    mention_user_id_str =mention['id_str']
                    mention_name=mention['name']
                    mention_screen_name=mention['screen_name']
                    mention_text=tweet['text']
                    tweets_id=tweet['id']
                    tweets_id_str=tweet['id_str']
                    hashtag_id=word_id
                    created_at=appConfig.set_date(tweet['created_at']) 
                    
                    column_str="tweet_user_id,tweet_user_id_str ,mention_user_id,mention_user_id_str,mention_name,mention_screen_name,mention_text" 
                    column_str+=",tweets_id ,tweets_id_str,hashtag_id ,created_at,loaded_date"
                    cur.execute("SELECT tweet_user_id FROM twitter_mentions WHERE tweet_user_id=%s and mention_user_id=%s and tweets_id=%s", (tweet_user_id,mention_user_id,tweets_id, ))
                    #ignore the duplicate
                    if cur.fetchone() is None:
                        cur.execute("INSERT INTO twitter_mentions ("+column_str+") VALUES (%s,%s,%s,%s,%s ,%s,%s,%s,%s,%s ,%s,current_date)", (tweet_user_id,tweet_user_id_str ,mention_user_id
                                    ,mention_user_id_str,mention_name,mention_screen_name,mention_text,tweets_id ,tweets_id_str,hashtag_id ,created_at))
                        conn.commit()
                        total_record+=1
                        print('inserted :',total_record) 
        
    except Exception as ex:
        print('tweet_id:',tweet['id'])
        print ("somting failed",ex)
        break
cur.close()
conn.close()    
print('Done:',total_record)
            
            
            