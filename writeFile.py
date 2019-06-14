import _pickle as pickle
import json
import psycopg2
import psycopg2.extras
import appConfig
from collections import namedtuple


def create_record(obj, fields):
    ''' given obj from db returns named tuple with fields mapped to values '''
    Record = namedtuple("Record", fields)
    mappings = dict(zip(fields, obj))
    return Record(**mappings)


class Database:
    result = []

    def __init__(self):
        self.conn = psycopg2.connect(
            host=appConfig.dbConfig.hostname,
            user=appConfig.dbConfig.username,
            password=appConfig.dbConfig.password,
            dbname=appConfig.dbConfig.database)

    def execute(self, sql):
        cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(sql)

        headers = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        cur.close()
        for row in rows:
            self.result.append(create_record(row, headers))

    def close(self):
        self.conn.close()


def write_to_file(filename, data):
    with open(filename, mode='wb') as f:
        pickle.dump(data, f)


def read_file(filename):
    with open(filename, mode='rb') as f:
        data = pickle.load(f)
        print(data)


def write_user(filename, data):
    data_key = {}
    for row in data:
        user_name = row.user_name
        screen_name = row.screen_name
        source_user_id_str = row.source_user_id_str
        data_key[source_user_id_str] = {
            "user_name": user_name,
            "screen_name": screen_name
        }
    write_to_file(filename, data_key)


def write_adj_list(filename, data):
    data_key = {}
    for row in data:
        indegree = row.indegree
        outdegree = row.outdegree
        source_user_id_str = row.source_user_id_str
        if source_user_id_str not in data_key:
            data_key.update({source_user_id_str: {
                "indegree": [],
                "outdegree": []
            }})
        if indegree:
            data_key[source_user_id_str]["indegree"].append(indegree)
        if outdegree:
            data_key[source_user_id_str]["outdegree"].append(outdegree)

    write_to_file(filename, data_key)


def map_user(source, destination):
    with open(source, mode='rb') as f:
        data = pickle.load(f)
        # index = 0
        res = {}
        for idx, key in enumerate(data):
            res[idx] = key
        write_to_file(destination, res)


def convert_dict_to_json(file_path):
    with open(file_path, 'rb') as fpkl, open('%s.json' % file_path, 'w') as fjson:
        data = pickle.load(fpkl)
        json.dump(data, fjson, ensure_ascii=False, sort_keys=True, indent=4)


def main():
    user_file_name = "data/user.pickle"
    map_file_name = "data/map.pickle"
    # database = Database()
    # database.execute(
    #     """select q.source_user_id_str, tu.user_name, tu.screen_name from (
    #     select tweets_user_id_str as source_user_id_str, retweet_user_id_str as indegree, '' as outdegree from twitter_retweets
    #     where hashtag_id > 230
    #     union
    #     select in_reply_to_user_id_str as source_user_id_str, reply_to_user_id_str as indegree, '' as outdegree from twitter_replies
    #     where hashtag_id > 230
    #     and in_reply_to_user_id_str <> ''
    #     union
    #     select mention_user_id_str as source_user_id_str, tweet_user_id_str as indegree, '' as outdegree from twitter_mentions
    #     where hashtag_id > 230
    #     union
    #     select retweet_user_id_str as source_user_id_str, '' as indegree, tweets_user_id_str as outdegree from twitter_retweets
    #     where hashtag_id > 230
    #     union
    #     select reply_to_user_id_str as source_user_id_str, '' as indegree, in_reply_to_user_id_str as outdegree from twitter_replies
    #     where hashtag_id > 230
    #     and in_reply_to_user_id_str <> ''
    #     union
    #     select tweet_user_id_str as source_user_id_str, '' as indegree, mention_user_id_str as outdegree from twitter_mentions
    #     where hashtag_id > 230
    #     )q
    #     left join twitter_users tu on q.source_user_id_str = tu.user_id_str
    #     order by source_user_id_str """
    # )
    # database.close()
    # write_user(user_file_name, database.result)
    #
    # read_file(user_file_name)
    # map_user(user_file_name, map_file_name)

    database = Database()
    # database.execute(
    #     """select source_user_id_str, destination_user_id_str as indegree, '' as outdegree from twitter_indegree
    #     union
    #     select source_user_id_str, '' as indegree, destination_user_id_str as twitter_outdegree from twitter_outdegree
    #     order by source_user_id_str"""
    # )
    database.execute(
        # --get in degree
        "select * from ( "
        "select tweets_user_id_str as source_user_id_str, retweet_user_id_str as indegree, '' as outdegree from twitter_retweets " +
        "where hashtag_id > 230 " +
        "union " +
        "select in_reply_to_user_id_str as source_user_id_str, reply_to_user_id_str as indegree, '' as outdegree from twitter_replies " +
        "where hashtag_id > 230 " +
        "and in_reply_to_user_id_str <> '' " +
        "union " +
        "select mention_user_id_str as source_user_id_str, tweet_user_id_str as indegree, '' as outdegree from twitter_mentions " +
        "where hashtag_id > 230 " +
        # --get out degree
        "union " +
        "select retweet_user_id_str as source_user_id_str, '' as indegree, tweets_user_id_str as outdegree from twitter_retweets " +
        "where hashtag_id > 230 " +
        "union " +
        "select reply_to_user_id_str as source_user_id_str, '' as indegree, in_reply_to_user_id_str as outdegree from twitter_replies " +
        "where hashtag_id > 230 " +
        "and in_reply_to_user_id_str <> '' " +
        "union " +
        "select tweet_user_id_str as source_user_id_str, '' as indegree, mention_user_id_str as outdegree from twitter_mentions " +
        "where hashtag_id > 230 "
        ")q "
        "order by source_user_id_str"
    )
    database.close()
    print(database.result)
    write_adj_list("data/adj_list", database.result)
    convert_dict_to_json("data/adj_list")


if __name__ == '__main__':
    main()



