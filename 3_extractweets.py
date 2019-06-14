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
    cur.execute("SELECT tid,data,hashtag_id from tweets_data where is_process=false")
except:
    print ("I am unable to connect to the database")

column_str = "tweet_id,tweet_id_str ,tweet_user_id,tweet_user_id_str,hashtag_id,tweet_text,lang"
column_str += ",retweet_count ,favorite_count,retweeted ,favorited"
column_str += ",possibly_sensitive,place_country ,place_country_code ,place_full_name ,place_id ,place_name,place_type"
column_str += ",is_quote_status ,in_reply_to_status_id_str ,in_reply_to_status_id,in_reply_to_user_id_str,in_reply_to_user_id"
column_str += ",in_reply_to_screen_name ,tweet_source,truncated,create_at,loaded_date"
total_record = 0
print(column_str)

for tid, tweet,hashtag_id in cur.fetchall():
    try:
        #----- tweets user
        if 'retweeted_status' not in tweet:
            tweet_id = tweet['id']
            tweet_id_str = tweet['id_str']
            tweet_user_id = tweet['user']['id']
            tweet_user_id_str = tweet['user']['id_str']
            word_id = hashtag_id
            tweet_text = tweet['text']
            lang = tweet['lang']
            retweet_count = tweet['retweet_count']
            favorite_count = tweet['favorite_count']
            #quote_count=tweet['quote_count']
            #reply_count =tweet['reply_count']
            retweeted = tweet['retweeted']
            favorited = tweet['favorited']
            possibly_sensitive = None
            coordinates = tweet['coordinates']
            
            if tweet['place'] is not None:
                place_country = tweet['place']['country']
                place_country_code = tweet['place']['country_code']
                place_full_name = tweet['place']['full_name']
                place_id = tweet['place']['id']
                place_name = tweet['place']['name']
                place_type = None
                bounding_box = tweet['place']['bounding_box']
            else:
                place_country = None
                place_country_code = None
                place_full_name = None
                place_id = None
                place_name = None
                place_type = None
                bounding_box = None
                
            
            is_quote_status = tweet['is_quote_status']
            in_reply_to_status_id = tweet['in_reply_to_status_id']
            in_reply_to_status_id_str = tweet['in_reply_to_status_id_str']
            in_reply_to_user_id_str = tweet['in_reply_to_user_id_str']
            in_reply_to_user_id = tweet['in_reply_to_user_id']
            in_reply_to_screen_name = tweet['in_reply_to_screen_name']
            tweet_source = tweet['source']
            truncated = tweet['truncated']
            create_at = appConfig.set_date(tweet['created_at'])
            
           
            
            cur.execute("SELECT tweet_id FROM twitter_tweets WHERE tweet_id=%s", (tweet_id, ))
                    #ignore the duplicate
            if cur.fetchone() is None:
                cur.execute("INSERT INTO twitter_tweets ("+column_str+") VALUES (%s,%s,%s,%s,%s ,%s,%s,%s,%s,%s ,%s,%s,%s,%s ,%s ,%s ,%s ,%s,%s,%s,%s ,%s ,%s,%s,%s,%s, %s,CURRENT_DATE)", (tweet_id,tweet_id_str ,tweet_user_id,tweet_user_id_str,word_id,tweet_text,lang 
                                                    ,retweet_count ,favorite_count,retweeted ,favorited
                                                    ,possibly_sensitive,place_country ,place_country_code ,place_full_name ,place_id ,place_name,place_type
                                                    ,is_quote_status ,in_reply_to_status_id_str ,in_reply_to_status_id,in_reply_to_user_id_str,in_reply_to_user_id
                                                    ,in_reply_to_screen_name ,tweet_source,truncated,create_at))
                conn.commit()
                total_record+=1
                print('inserted :',total_record)
        else:
           
            tweet_id=tweet['retweeted_status']['id']
            tweet_id_str =tweet['retweeted_status']['id_str']
            tweet_user_id=tweet['retweeted_status']['user']['id']
            tweet_user_id_str=tweet['retweeted_status']['user']['id_str']
            word_id =hashtag_id
            tweet_text=tweet['retweeted_status']['text']
            lang =tweet['retweeted_status']['lang']
            retweet_count =tweet['retweeted_status']['retweet_count']
            favorite_count=tweet['retweeted_status']['favorite_count']
            retweeted =tweet['retweeted_status']['retweeted']
            favorited=tweet['retweeted_status']['favorited']
            possibly_sensitive=None
                
            coordinates=tweet['retweeted_status']['coordinates']
            
            #print(tweet['retweeted_status']['place']) 
            if tweet['retweeted_status']['place'] is not None:
                
                place_country =tweet['retweeted_status']['place']['country']
                place_country_code =tweet['retweeted_status']['place']['country_code']
                place_full_name =tweet['retweeted_status']['place']['full_name']
                place_id =tweet['retweeted_status']['place']['id']
                place_name=tweet['retweeted_status']['place']['name']
                place_type=None
                bounding_box=tweet['retweeted_status']['place']['bounding_box']
            else:
                place_country =None
                place_country_code =None
                place_full_name =None
                place_id =None
                place_name=None
                place_type=None
                bounding_box=None
      
            is_quote_status =tweet['retweeted_status']['is_quote_status']
            in_reply_to_status_id =tweet['retweeted_status']['in_reply_to_status_id']
            in_reply_to_status_id_str =tweet['retweeted_status']['in_reply_to_status_id_str']
            in_reply_to_user_id_str=tweet['retweeted_status']['in_reply_to_user_id_str']
            in_reply_to_user_id =tweet['retweeted_status']['in_reply_to_user_id']
            in_reply_to_screen_name=tweet['retweeted_status']['in_reply_to_screen_name']
            tweet_source=tweet['retweeted_status']['source']
            truncated=tweet['retweeted_status']['truncated']
            create_at=appConfig.set_date(tweet['retweeted_status']['created_at'])
            
            cur.execute("SELECT tweet_id FROM twitter_tweets WHERE tweet_id=%s", (tweet_id, ))
                    #ignore the duplicate
            if cur.fetchone() is None:
                cur.execute("INSERT INTO twitter_tweets ("+column_str+") VALUES (%s,%s,%s,%s,%s ,%s,%s,%s,%s,%s ,%s,%s,%s,%s ,%s ,%s ,%s ,%s,%s,%s,%s ,%s ,%s,%s,%s,%s, %s,current_date)", (tweet_id,tweet_id_str ,tweet_user_id,tweet_user_id_str,word_id,tweet_text,lang 
                                                    ,retweet_count ,favorite_count,retweeted ,favorited
                                                    ,possibly_sensitive,place_country ,place_country_code ,place_full_name ,place_id ,place_name,place_type
                                                    ,is_quote_status ,in_reply_to_status_id_str ,in_reply_to_status_id,in_reply_to_user_id_str,in_reply_to_user_id
                                                    ,in_reply_to_screen_name ,tweet_source,truncated,create_at))
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
            
            
            