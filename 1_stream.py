# -*- coding: utf-8 -*-
"""
Created on Fri Feb 16 13:25:09 2018

@author: nantanasriphromting
"""
import sys
import tweepy
import json
import psycopg2
import appConfig

# Replace the API_KEY an d API_SECRET with your application's key and secret.
auth = tweepy.AppAuthHandler(appConfig.tokenConfig.consumer_key, appConfig.tokenConfig.consumer_secret)

api = tweepy.API(auth, wait_on_rate_limit=True,wait_on_rate_limit_notify=True)

if (not api):
    print ("Can't Authenticate")
    sys.exit(-1)

# Continue with rest of code
    
#searchQuery = '#BlackPanther'  # this is what we're searching for
maxTweets = 10000000 # Some arbitrary large number
tweetsPerQry = 100  # this is the max the API permits

# If results from a specific ID onwards are reqd, set since_id to that ID.
# else default to no lower limit, go as far back as API allows
sinceId = None

# If results only below a specific ID are, set max_id to that ID.
# else default to no upper limit, start from the most recent tweet matching the search query.


print("Downloading max {0} tweets".format(maxTweets))
tweetInsert=0
tweetCount = 0
word_count=0
conn = psycopg2.connect(host=appConfig.dbConfig.hostname, user=appConfig.dbConfig.username, password=appConfig.dbConfig.password, dbname=appConfig.dbConfig.database)
cur = conn.cursor()
cur.execute("SELECT hashtag_id,hashtag_text,hashtag.movie_id from hashtag inner join movies on hashtag.movie_id=movies.movie_id where  movies.is_active='Y' order by movies.movie_id  ")
alldata=cur.fetchall()
for hashtag_id, hashtag_text,movie_id in alldata :
    print('hashtag:',hashtag_text)
    max_id = -1
    word_count+=1
    cur.execute("SELECT MAX(CAST(NULLIF(tid,'-1') AS INT8)) AS tid FROM tweets_data WHERE hashtag_id=%s and lang='th'", (hashtag_id, ))
    for tid in cur.fetchall():
        sinceId = tid[0]

   # if sinceId<0:
     #   sinceId=None
    print('sinceId:',sinceId)
    #tweetCount = 0
    #tweetInsert=0;

    while tweetCount < maxTweets:
        try:
            if (max_id <= 0):
                if (not sinceId):
                    new_tweets = api.search(q=hashtag_text, count=tweetsPerQry,lang='th')
                else:
                    new_tweets = api.search(q=hashtag_text, count=tweetsPerQry,lang='th',
                                            since_id=sinceId)
            else:
                if (not sinceId):
                    new_tweets = api.search(q=hashtag_text, count=tweetsPerQry,lang='th',
                                            max_id=str(max_id - 1))
                else:
                    new_tweets = api.search(q=hashtag_text, count=tweetsPerQry,lang='th',
                                            max_id=str(max_id - 1),
                                            since_id=sinceId)
            if not new_tweets:
                print("No more tweets found")
                break
            #print(len(new_tweets))
            for tweet in new_tweets:
                #print (tweet.id_str)
                cur.execute("SELECT * FROM tweets_data WHERE tid='{0}'".format(tweet.id_str))
                #print('id_str:',tweet.id_str)
                    #ignore the duplicate
                if cur.fetchone() is not None:
                    #print("duplicate {0}".format(tweet.id_str))
                    continue
                cur.execute("INSERT INTO tweets_data (tid, data,hashtag_id,movie_id,loaded_date,lang,is_process) VALUES (%s, %s,%s,%s,CURRENT_DATE,'th',false)", (tweet.id_str, json.dumps(tweet._json,ensure_ascii=False),hashtag_id,movie_id ))
                tweetInsert =tweetInsert+1
                conn.commit()
            tweetCount += len(new_tweets)
            print("{3}{2} Downloaded {0} tweets Inserted {1} tweets".format(tweetCount,tweetInsert,hashtag_text,word_count))
            #print("Downloaded {0} tweets".format(tweetCount))
            max_id = new_tweets[-1].id
            #print("sinceId==>",sinceId)
        except tweepy.TweepError as e:
            # Just exit if any error
            print("some error : " + str(e))
            break
    cur.execute("UPDATE hashtag SET lastest_load_date=CURRENT_DATE WHERE hashtag_id={0} ".format(hashtag_id))
    conn.commit()
cur.close()
conn.close()

print("Downloaded {0} tweets Inserted {1} tweets total {2} hashtags".format(tweetCount,tweetInsert,word_count))


    
    