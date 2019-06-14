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
    
total_record=0
column_str="user_id,user_id_str,user_name,screen_name,user_location,url,description,protected,verified,followers_count,friends_count,listed_count,favourites_count,statuses_count,utc_offset,time_zone,geo_enabled,lang,contributors_enabled,profile_background_color,profile_background_image_url,profile_background_image_url_https,profile_background_tile,profile_banner_url,profile_image_url,profile_image_url_https,profile_link_color,profile_sidebar_border_color,profile_sidebar_fill_color,profile_text_color,profile_use_background_image,default_profile,default_profile_image,is_translator,user_following,notifications,loaded_date"

for tid, tweet,hashtag_id in cur.fetchall() :
    try:
        #----- tweets user
        
        user_id =tweet['user']['id']
        user_id_str =tweet['user']['id_str']
        user_name =tweet['user']['name']
        screen_name =tweet['user']['screen_name']
        user_location =tweet['user']['location']
        url =tweet['user']['url']
        description =tweet['user']['description']
        protected =tweet['user']['protected']
        verified =tweet['user']['verified']
        followers_count  =tweet['user']['followers_count']
        friends_count =tweet['user']['friends_count']
        listed_count  =tweet['user']['listed_count']
        favourites_count  =tweet['user']['favourites_count']
        statuses_count  =tweet['user']['statuses_count']
        utc_offset =tweet['user']['utc_offset']
        time_zone  =tweet['user']['time_zone']
        geo_enabled  =tweet['user']['geo_enabled']
        lang =tweet['user']['lang']
        contributors_enabled =tweet['user']['contributors_enabled']
        profile_background_color  =tweet['user']['profile_background_color']
        profile_background_image_url  =tweet['user']['profile_background_image_url']
        profile_background_image_url_https  =tweet['user']['profile_background_image_url_https']
        profile_background_tile  =tweet['user']['profile_background_tile']
        if 'profile_banner_url' in tweet['user']:
            profile_banner_url  =tweet['user']['profile_banner_url']
        else:
            profile_banner_url=None
        if 'profile_image_url' in tweet['user']:    
            profile_image_url  =tweet['user']['profile_image_url']
        else:
            profile_image_url  =None
        if 'profile_image_url_https' in tweet['user']:
            profile_image_url_https  =tweet['user']['profile_image_url_https']
        else:
            profile_image_url_https=None
        if 'profile_link_color' in tweet['user']:
            profile_link_color  =tweet['user']['profile_link_color']
        else:
            profile_link_color=None
        if 'profile_sidebar_border_color' in tweet['user']:
            profile_sidebar_border_color  =tweet['user']['profile_sidebar_border_color']
        else:
            profile_sidebar_border_color=None
        if 'profile_sidebar_fill_color' in tweet['user']:
            profile_sidebar_fill_color  =tweet['user']['profile_sidebar_fill_color']
        else:
            profile_sidebar_fill_color=None
        if 'profile_text_color' in tweet['user']:
            profile_text_color =tweet['user']['profile_text_color']
        else:
            profile_text_color=None
        if 'profile_use_background_image' in tweet['user']:
            profile_use_background_image  =tweet['user']['profile_use_background_image']
        else:
            profile_use_background_image=None
        if 'default_profile' in tweet['user']:
            default_profile  =tweet['user']['default_profile']
        else:
            default_profile=None
        if 'default_profile_image' in tweet['user']:
            default_profile_image  =tweet['user']['default_profile_image']
        else:
            default_profile_image=None
           # withheld_in_countries  =tweet['user']['withheld_in_countries']
            #withheld_scope  =tweet['user']['withheld_scope']
        is_translator  =tweet['user']['is_translator']
        user_following  =tweet['user']['following']
        notifications  =tweet['user']['notifications']
        
        try:
            cur.execute("SELECT user_id FROM twitter_users WHERE user_id=%s", (user_id, ))
                    #ignore the duplicate
            
            if cur.fetchone() is None:
                cur.execute("INSERT INTO twitter_users ("+column_str+") VALUES (%s,%s,%s,%s ,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s ,%s,%s,%s,current_date)", (user_id,user_id_str,user_name,screen_name,user_location,url,description,protected,verified,followers_count,friends_count,listed_count,favourites_count,statuses_count,utc_offset,time_zone,geo_enabled,lang,contributors_enabled,profile_background_color,profile_background_image_url,profile_background_image_url_https,profile_background_tile,profile_banner_url,profile_image_url,profile_image_url_https,profile_link_color,profile_sidebar_border_color,profile_sidebar_fill_color,profile_text_color,profile_use_background_image,default_profile,default_profile_image,is_translator,user_following,notifications))
                conn.commit()
                total_record+=1
                print('inserted :',total_record)
            #----- user in retweet
        except psycopg2.OperationalError as e:
            print('user_id:',user_id)
            print('Unable to connect!\n{0}').format(e)
            
   # do stuff
        if 'retweeted_status' in tweet:
            user_id =tweet['retweeted_status']['user']['id']
            user_id_str =tweet['retweeted_status']['user']['id_str']
            user_name =tweet['retweeted_status']['user']['name']
            screen_name =tweet['retweeted_status']['user']['screen_name']
            user_location =tweet['retweeted_status']['user']['location']
            url =tweet['retweeted_status']['user']['url']
            description =tweet['retweeted_status']['user']['description']
            protected =tweet['retweeted_status']['user']['protected']
            verified =tweet['retweeted_status']['user']['verified']
            followers_count  =tweet['retweeted_status']['user']['followers_count']
            friends_count =tweet['retweeted_status']['user']['friends_count']
            listed_count  =tweet['retweeted_status']['user']['listed_count']
            favourites_count  =tweet['retweeted_status']['user']['favourites_count']
            statuses_count  =tweet['retweeted_status']['user']['statuses_count']
            utc_offset =tweet['retweeted_status']['user']['utc_offset']
            time_zone  =tweet['retweeted_status']['user']['time_zone']
            geo_enabled  =tweet['retweeted_status']['user']['geo_enabled']
            lang =tweet['retweeted_status']['user']['lang']
            contributors_enabled =tweet['retweeted_status']['user']['contributors_enabled']
            profile_background_color  =tweet['retweeted_status']['user']['profile_background_color']
            profile_background_image_url  =tweet['retweeted_status']['user']['profile_background_image_url']
            profile_background_image_url_https  =tweet['retweeted_status']['user']['profile_background_image_url_https']
            profile_background_tile  =tweet['retweeted_status']['user']['profile_background_tile']
            if 'profile_banner_url' in tweet['retweeted_status']['user']:
                profile_banner_url  =tweet['retweeted_status']['user']['profile_banner_url']
            else:
                profile_banner_url=None
            if 'profile_image_url' in tweet['retweeted_status']['user']:    
                profile_image_url  =tweet['retweeted_status']['user']['profile_image_url']
            else:
                profile_image_url  =None
            if 'profile_image_url_https' in tweet['retweeted_status']['user']:
                profile_image_url_https  =tweet['retweeted_status']['user']['profile_image_url_https']
            else:
                profile_image_url_https=None
            if 'profile_link_color' in tweet['retweeted_status']['user']:
                profile_link_color  =tweet['retweeted_status']['user']['profile_link_color']
            else:
                profile_link_color=None
            if 'profile_sidebar_border_color' in tweet['retweeted_status']['user']:
                profile_sidebar_border_color  =tweet['retweeted_status']['user']['profile_sidebar_border_color']
            else:
                profile_sidebar_border_color=None
            if 'profile_sidebar_fill_color' in tweet['retweeted_status']['user']:
                profile_sidebar_fill_color  =tweet['retweeted_status']['user']['profile_sidebar_fill_color']
            else:
                profile_sidebar_fill_color=None
            if 'profile_text_color' in tweet['retweeted_status']['user']:
                profile_text_color =tweet['retweeted_status']['user']['profile_text_color']
            else:
                profile_text_color=None
            if 'profile_use_background_image' in tweet['retweeted_status']['user']:
                profile_use_background_image  =tweet['retweeted_status']['user']['profile_use_background_image']
            else:
                profile_use_background_image=None
            if 'default_profile' in tweet['retweeted_status']['user']:
                default_profile  =tweet['retweeted_status']['user']['default_profile']
            else:
                default_profile=None
            if 'default_profile_image' in tweet['retweeted_status']['user']:
                default_profile_image  =tweet['retweeted_status']['user']['default_profile_image']
            else:
                default_profile_image=None
               # withheld_in_countries  =tweet['user']['withheld_in_countries']
                #withheld_scope  =tweet['user']['withheld_scope']
            is_translator  =tweet['retweeted_status']['user']['is_translator']
            user_following  =tweet['retweeted_status']['user']['following']
            notifications  =tweet['retweeted_status']['user']['notifications']
        
            cur.execute("SELECT user_id FROM twitter_users WHERE user_id=%s", (user_id, ))
                    #ignore the duplicate
            if cur.fetchone() is None:
                cur.execute("INSERT INTO twitter_users ("+column_str+") VALUES (%s,%s,%s,%s ,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s ,%s,%s,%s,current_date)", (user_id,user_id_str,user_name,screen_name,user_location,url,description,protected,verified,followers_count,friends_count,listed_count,favourites_count,statuses_count,utc_offset,time_zone,geo_enabled,lang,contributors_enabled,profile_background_color,profile_background_image_url,profile_background_image_url_https,profile_background_tile,profile_banner_url,profile_image_url,profile_image_url_https,profile_link_color,profile_sidebar_border_color,profile_sidebar_fill_color,profile_text_color,profile_use_background_image,default_profile,default_profile_image,is_translator,user_following,notifications))
                conn.commit()
                total_record+=1
                print('inserted :',total_record)
    except Exception as ex:
        print('user_id:',user_id)
        print ("somting failed",ex)
        break
cur.close()
conn.close()    
print('Done:',total_record)
            
            
            