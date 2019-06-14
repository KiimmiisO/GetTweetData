#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 02 10:27:20 2019

@author: piyanush khanthawichai
"""


import psycopg2
import appConfig


try:
    print('try to connect')
    conn = psycopg2.connect(host=appConfig.dbConfig.hostname, user=appConfig.dbConfig.username, password=appConfig.dbConfig.password, dbname=appConfig.dbConfig.database)
    cur = conn.cursor()
    # cur.execute("insert into tbhitsalgorithm " +
    #             "select *, 0,0,0 from(" +
    #             "select tweets_user_id_str, retweet_user_id_str from twitter_retweets " +
    #             "where hashtag_id between 230 and 279 " +
    #             "union " +
    #             "select in_reply_to_user_id_str, reply_to_user_id_str from twitter_replies " +
    #             "where hashtag_id between 230 and 279 " +
    #             "and in_reply_to_user_id_str <> '' " +
    #             "union " +
    #             "select mention_user_id_str, tweet_user_id_str from twitter_mentions " +
    #             "where hashtag_id between 230 and 279 )q " +
    #             "order by tweets_user_id_str")
    # conn.commit()

    # cur.execute("select tweet_user_id_str, count(*) as count  from tbhitsalgorithm group by tweet_user_id_str")
    # commands = []
    # for res in cur.fetchall():
    #     # print(res[1])
    #     commands.append("update tbhitsalgorithm set authority_value = {0} where tweet_user_id_str = '{1}'".format(res[1], res[0]))
    #     # cur.execute("update tbhitsalgorithm set authority_value = {0} where tweet_user_id_str = '{1}'".format(res[1], res[0]")
    #     # conn.commit()
    # for command in commands:
    #     print(command)
    #     cur.execute(command)
    #     conn.commit()

    cur.execute("select retweet_user_id_string, sum(authority_value) as authority_value from tbhitsalgorithm group by retweet_user_id_string")
    hubs = []
    for res in cur.fetchall():
        hubs.append(
            "update tbhitsalgorithm set hub_value = {0} where retweet_user_id_string = '{1}'".format(res[1], res[0]))
    for hub in hubs:
        print(hub)
        cur.execute(hub)
        conn.commit()

    cur.execute(
        "select tweet_user_id_str, sum(hub_value) as hub_value from tbhitsalgorithm group by tweet_user_id_str")
    authos = []
    for res in cur.fetchall():
        # print(res[1])
        authos.append(
            "update tbhitsalgorithm set new_authority_value = {0} where tweet_user_id_str = '{1}'".format(res[1], res[0]))

    for autho in authos:
        print(autho)
        cur.execute(autho)
        conn.commit()



except:
    print ("I am unable to connect to the database")


cur.close()
conn.close()    
print('Done!!')
            
            
            