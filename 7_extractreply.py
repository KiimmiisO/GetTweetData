#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Feb 23 21:56:09 2018

@author: nantanasriphromting
"""

import psycopg2
import time
from datetime import datetime
import appConfig



conn = psycopg2.connect( host=appConfig.dbConfig.hostname, user=appConfig.dbConfig.username, password=appConfig.dbConfig.password, dbname=appConfig.dbConfig.database )
cur = conn.cursor()
cur.execute("SELECT t.hashtag_id,r.tweet_id,r.data from reply_data r inner join twitter_tweets t on r.tweet_id=t.tweet_id where r.is_process=false")
total_record=0
for  hashtag_id,tweet_id,tweet in cur.fetchall() :
    reply_to_tweet_id=tweet['id']
    reply_to_tweet_id_str =tweet['id_str']
    reply_to_user_id=tweet['user']['id']
    reply_to_user_id_str=tweet['user']['id_str']
    word_id =hashtag_id
    reply_to_tweet_text=tweet['text']
    lang =tweet['lang']
    retweet_count =tweet['retweet_count']
    favorite_count=tweet['favorite_count']
    retweeted =tweet['retweeted']
    favorited=tweet['favorited']
    possibly_sensitive=None
    coordinates=tweet['coordinates']
    
    if tweet['place'] is not None:
        
        place_country =tweet['place']['country']
        place_country_code =tweet['place']['country_code']
        place_full_name =tweet['place']['full_name']
        place_id =tweet['place']['id']
        place_name=tweet['place']['name']
        place_type=None
        bounding_box=tweet['place']['bounding_box']
    else:
        place_country =None
        place_country_code =None
        place_full_name =None
        place_id =None
        place_name=None
        place_type=None
        bounding_box=None
        
    
    is_quote_status =tweet['is_quote_status']
    in_reply_to_status_id =tweet['in_reply_to_status_id']
    in_reply_to_status_id_str =tweet['in_reply_to_status_id_str']
    in_reply_to_user_id_str=tweet['in_reply_to_user_id_str']
    in_reply_to_user_id =tweet['in_reply_to_user_id']
    in_reply_to_screen_name=tweet['in_reply_to_screen_name']
    tweet_source=tweet['source']
    truncated=tweet['truncated']
    create_at=appConfig.set_date(tweet['created_at'])
    
   
    column_str="tweet_id,reply_to_tweet_id ,reply_to_tweet_id_str,reply_to_user_id,reply_to_user_id_str,hashtag_id,reply_to_tweet_text,lang" 
    column_str+=",retweet_count ,favorite_count,retweeted ,favorited"
    column_str+=",possibly_sensitive,place_country ,place_country_code ,place_full_name ,place_id ,place_name,place_type"
    column_str+=",is_quote_status ,in_reply_to_status_id_str ,in_reply_to_status_id,in_reply_to_user_id_str,in_reply_to_user_id"
    column_str+=",in_reply_to_screen_name ,tweet_source,truncated,create_at,loaded_date"
    print(column_str)
    cur.execute("SELECT tweet_id FROM twitter_replies WHERE tweet_id=%s and reply_to_tweet_id=%s", (tweet_id,reply_to_tweet_id_str, ))
            #ignore the duplicate
    if cur.fetchone() is None:
        cur.execute("INSERT INTO twitter_replies ("+column_str+") VALUES (%s,%s,%s,%s,%s ,%s,%s,%s,%s,%s,%s ,%s,%s,%s,%s ,%s ,%s ,%s ,%s,%s,%s,%s ,%s ,%s,%s,%s,%s, %s,CURRENT_DATE)", (tweet_id,reply_to_tweet_id ,reply_to_tweet_id_str,reply_to_user_id,reply_to_user_id_str,word_id,reply_to_tweet_text,lang 
                                            ,retweet_count ,favorite_count,retweeted ,favorited
                                            ,possibly_sensitive,place_country ,place_country_code ,place_full_name ,place_id ,place_name,place_type
                                            ,is_quote_status ,in_reply_to_status_id_str ,in_reply_to_status_id,in_reply_to_user_id_str,in_reply_to_user_id
                                            ,in_reply_to_screen_name ,tweet_source,truncated,create_at))
        conn.commit()
        total_record+=1
        print('inserted :',total_record)
    
cur.close()
conn.close()       