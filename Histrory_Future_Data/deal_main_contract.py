# !/usr/bin/env python3
# -*- coding:utf-8 -*-
'''
created by Fangyang on Time:2018/11/10
'''
__author__ = 'Fangyang'


import pandas as pd
from Histrory_Future_Data.pandas_db import PandasMongoDB


market_var = {'cffex': ['IF','IC','IH','T','TF','TS'],
'dce':['C','CS','A','B','M','Y','P','FB','BB','JD','L','V','PP','J','JM','I'],
'czce':['WH','PM','CF','SR','TA','OI','RI','MA','ME','FG','RS','RM','ZC','JR','LR','SF','SM','WT','TC','GN','RO','ER','SRX','SRY','WSX','WSY','CY','AP'],
'shfe':['CU','AL','ZN','PB','NI','SN','AU','AG','RB','WR','HC','FU','BU','RU']
}
markets = ['cffex', 'dce', 'czce', 'shfe']


def gen_main_con(contract_name=None, db_name=None, collection=None, pd_db_instance=None):
    # 合成主连合约 df2
    # db.getCollection('MarketData_Year_2018').find({code:{$regex:'^rb'}, datetime:'2018-01-02'})
    # db.getCollection('MarketData_Year_2018').find({datetime:{$gt:'2018-02-02'}}) 根据字符串查询时间

    assert contract_name is not None, "Please set contract_name..."
    assert db_name is not None, "Please set mongodb db_name..."
    assert collection is not None, "Please set mongodb collection..."
    assert pd_db_instance is not None, "Please set Pandas mongodb interface instance..."

    if db_name == 'CZCE':
        contract_name = contract_name.upper()
    elif db_name == 'DCE' or db_name == 'SHFE':
        contract_name = contract_name.lower()

    query = {'code': {'$regex': '^{}'.format(contract_name)}}
    # df = pd.DataFrame(list(collection.find(dict(query), {'_id': 0})))
    df = pd_db_instance.read_db(database_name=db_name,
                   collection_name=collection,
                   query=(query, {'_id':0}))
    # print(df.info())    # 测试datetime 类型
    # df = pd.to_datetime(df['datetime'], unit='ms')

    df1 = df.groupby(by='datetime')[['持仓量']].max()
    df2 = pd.merge(df, df1, on=['datetime', '持仓量'])

    return df2


if __name__ == '__main__':

    pd_mongo = PandasMongoDB()

    # db = 'SHFE'
    # collection = 'MarketData_Year_2018'
    db = 'DCE'
    collection = '2017'

    df = gen_main_con(contract_name='B', db_name=db, collection=collection, pd_db_instance=pd_mongo)
    print(df)

