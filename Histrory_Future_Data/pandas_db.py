# !/usr/bin/env python3
# -*- coding:utf-8 -*-
'''
created by Fangyang on Time:2018/10/28
'''
__author__ = 'Fangyang'


import pandas as pd
from pymongo import MongoClient
import json
import time
from sqlalchemy import create_engine
# import cStringIO


class PandasMySQL(object):

    def __init__(self):
        # self.db_info = {
        #     'user': 'root',
        #     'password': '123',
        #     'host': '192.168.3.155',
        #     'database': 'yldsj'  # 这里我们事先指定了数据库，后续操作只需要表即可
        # }

        self.db_info = {
            'user': 'root',
            'password': 'raspberry',
            'host': '127.0.0.1',
            'database': 'yldsj'  # 这里我们事先指定了数据库，后续操作只需要表即可
        }
        self.engine = create_engine(
            'mysql+pymysql://{user}:{password}@{host}/{database}?charset=utf8'.format(**self.db_info), encoding='utf-8')

    def read_db(self, table_name):

        # return pd.read_sql('SELECT * FROM `work-1`.work1_dep2', con=engine)
        print("Start to read [table:{0}] in [DB:{database}]".format(table_name, **self.db_info))
        start_time = time.time()
        df = pd.read_sql(table_name, con=self.engine)
        print('Read data from mysql cost : {:.2f}s'.format(time.time()-start_time))
        return df

    def write_df_to_db(self, my_df, table_name, if_exists='append', chunk_size=None, index=False):
        '''
        传入dataframe, 给出数据库中的表名string, 存入数据库
        fail, 如果表存在，啥也不做
        replace, 如果表存在，删了表，再建立一个新表，把数据插入
        append, 如果表存在，把数据插入，如果表不存在创建一个表！！
        '''
        print("Start to save df into database...")
        start_time = time.time()
        # 如果表存在, 就append数据到表中, 默认不将dataframe的index写入table
        my_df.to_sql(table_name, con=self.engine, if_exists=if_exists, chunksize=chunk_size, index=index)
        print("Save dataframe data to MySQL DB successfully. Cost {:.2f}s".format(time.time() - start_time))

    # def write_df_to_db_sp(self, df_a, table_name):
    #
    #     output = cStringIO.StringIO()
    #     # ignore the index
    #     df_a.to_csv(output, sep='\t', index=False, header=False)
    #     output.getvalue()
    #     # jump to start of stream
    #     output.seek(0)
    #
    #     connection = self.engine.raw_connection()  # engine 是 from sqlalchemy import create_engine
    #     cursor = connection.cursor()
    #     # null value become ''
    #     cursor.copy_from(output, table_name, null='')
    #     connection.commit()
    #     cursor.close()


class PandasMongoDB(object):

    def __init__(self):
        self.db_info = {
            'server': 'localhost',
            'mongodb_port': 27017
                        }
        self.client = MongoClient(self.db_info['server'], int(self.db_info['mongodb_port']))

    def read_db(self,
                   database_name='mydatabasename',
                   collection_name='mycollectionname',
                   query=(dict(), {'_id':0})):

        db = self.client[database_name]
        collection = db[collection_name]
        start_time = time.time()
        df = pd.DataFrame(list(collection.find(*query)))
        print('read data from mongo cost:{:.2f}s'.format(time.time()-start_time))
        return df

    def get_collections_list(self, db_name):
        return self.client[db_name].list_collection_names()

    def write_df_list_to_db(self,
                            my_df,
                            database_name='mydatabasename',
                            collection_name='mycollectionname',
                            chunksize=None,
                            timer=True):
        #"""
        #This function take a list and create a collection in MongoDB (you should
        #provide the database name, collection, port to connect to the remoete database,
        #server of the remote database, local port to tunnel to the other machine)
        #
        #---------------------------------------------------------------------------
        #Parameters / Input
        #    my_list: the list to send to MongoDB
        #    database_name:  database name
        #
        #    collection_name: collection name (to create)
        #    server: the server of where the MongoDB database is hosted
        #        Example: server = '132.434.63.86'
        #    this_machine_port: local machine port.
        #        For example: this_machine_port = '27017'
        #    remote_port: the port where the database is operating
        #        For example: remote_port = '27017'
        #    chunksize: The number of items of the list that will be send at the
        #        some time to the database. Default is None.
        # ----------------------------------------------------------------------------

        # To connect
        db = self.client[database_name]
        collection = db[collection_name]
        # To write
        collection.delete_many({})  # Destroy the collection
        #aux_df=aux_df.drop_duplicates(subset=None, keep='last') # To avoid repetitions

        if timer:
            start_time = time.time()

        my_list = my_df.to_dict('records')
        nrows = len(my_list)
        if nrows == 0:
            return

        if chunksize is None:
            chunksize = nrows
        elif chunksize == 0:
            raise ValueError('chunksize argument should be non-zero')

        chunks = int(nrows / chunksize) + 1

        for i in range(chunks):
            start_i = i * chunksize
            end_i = min((i + 1) * chunksize, nrows)
            if start_i >= end_i:
                break
            collection.insert_many(my_list[start_i:end_i])

        if timer:
            print('Complete importing data to DB:{} -- collection:{}. Time cost : {:.2f}s'.format(
                database_name, collection_name, time.time()-start_time))
        else:
            print('Complete importing data to DB:{} -- collection:{}.'.format(
                database_name, collection_name))
        return

    def write_df_json_to_db(self,
                            my_df,
                            database_name='mydatabasename',
                            collection_name='mycollectionname',
                            timer=True):
        '''
        三种写入数据的方式中最快, 快10多秒钟, 其他两种方式有chunksize的效果一般
        :param my_df: Dataframe
        :param database_name: str
        :param collection_name: str
        :param server: str
        :param mongodb_port: int/str
        :param timer: Boolean
        :return: None
        '''

        db = self.client[database_name]
        collection = db[collection_name]
        collection.delete_many({})  # Destroy the collection

        if timer:
            start_time = time.time()

        collection.insert(json.loads(my_df.to_json(orient='records')))

        if timer:
            print('Complete importing data to DB:{} -- collection:{}. Time cost : {:.2f}s'.format(
                database_name, collection_name, time.time()-start_time))
        else:
            print('Complete importing data to DB:{} -- collection:{}.'.format(
                database_name, collection_name))
        return


if __name__ == '__main__':
    pd_mongo = PandasMongoDB()
    df = pd.read_csv('./data/time_series_data.csv')
    # # pd_mongo.write_df_list_to_db(df, collection_name='from_df_list_nochunk')
    # # pd_mongo.write_df_list_to_db(df, collection_name='from_df_list_chunksize100', chunksize=5000)
    #
    # # 格式化处理下, 方便回测使用
    df.rename(columns={'date': 'datetime'}, inplace=True)
    # df['datetime'] = pd.to_datetime(df['datetime'], format='%Y-%m-%d')
    #  上面这里得返回, 要不没生效. 这句先不要, 存到mongo里面后是int64, 读出来之后还是int64, 得自己转下, 所以不要了, 还是str吧
    # print(df.dtypes)

    pd_mongo.write_df_json_to_db(df, collection_name='from_df_json')

    # Complete importing data to DB:mydatabasename -- collection:from_df_list_nochunk. Time cost : 46.07s
    # Complete importing data to DB:mydatabasename -- collection:from_df_list_chunksize100. Time cost : 45.45s
    # Complete importing data to DB:mydatabasename -- collection:from_df_json. Time cost : 35.21s

    #=================================================================================================
    # pd_mysql = PandasMySQL()
    # df = pd.read_csv('./data/DBA04.csv')
    # pd_mysql.write_df_to_db(df, 'DBA04_append_nochunk')
    # pd_mysql.write_df_to_db(df, 'DBA04_append_chunk2000', chunk_size=5000)
    # pd_mysql.write_df_to_db_sp(df, 'DBA04_sp')

    # 'Save dataframe data to MySQL DB successfully. Cost 96.19s'
    # 'Save dataframe data to MySQL DB successfully. Cost 79.72s'

    #=============================================================================================
    df_r1 = pd_mongo.read_db(collection_name='from_df_json')
    # df_r2 = pd_mysql.read_db(table_name='dba04_append_nochunk')
    print(df_r1['datetime'])
    # print(df_r2.head())

    # 'Read data from mongo cost : 12.95s'
    # 'Read data from mysql cost : 39.89s'