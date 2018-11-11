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

    query = {'code': {'$regex': '^{}{{1}}?\d'.format(contract_name)}}  # ^开头, 字符{匹配前面字符几次}, ?匹配前面0/1次, \d数字
    print(query)
    # df = pd.DataFrame(list(collection.find(dict(query), {'_id': 0})))
    df = pd_db_instance.read_db(database_name=db_name,
                                collection_name=collection,
                                query=(query, {'_id':0}))
    # print(df.info())    # 测试datetime 类型
    # df = pd.to_datetime(df['datetime'], unit='ms')

    df1 = df.groupby(by='datetime')[['持仓量']].max()
    df2 = pd.merge(df, df1, on=['datetime', '持仓量'])

    return df2


def get_future_daily_collection_list_by_db(pd_db_instance, db=None, col_name_remove_key_str=['hold', 'option']):
    '''

    :param pd_db_instance:
    :param db: db name
    :param col_name_remove_key_str: 给出string, list, 在列表中的str, 如果包含在collection名字中, 就不包含在返回的结果中
    :return: collections list
    '''
    collections_list = pd_db_instance.get_collections_list(db)

    common_set = {}
    for remove_word in col_name_remove_key_str:
        temp_set = {x for x in collections_list if remove_word not in x}
        # print(temp_set)  # 执行这句就知道了, 取留下的set的交集, 即不含remove_key_str的关键词字符串成分

        if common_set:
            common_set = common_set & temp_set
        else:
            common_set = temp_set

    return list(common_set)


if __name__ == '__main__':

    pd_mongo = PandasMongoDB()

    # db = 'SHFE'
    # collection = 'MarketData_Year_2018'
    # db = 'CZCE'
    # collection = '2017'
    # db = 'DCE'

    db_daily_collection_dict = {'CZCE': [], 'DCE': [], 'SHFE': []}
    for key in db_daily_collection_dict.keys():
        db_daily_collection_dict[key] = get_future_daily_collection_list_by_db(pd_mongo,
                                                                               db=key,
                                                                               col_name_remove_key_str=['hold',
                                                                                                        'option'])
    # print(db_daily_collection_dict)

    # df = gen_main_con(contract_name='jm', db_name=db, collection=collection, pd_db_instance=pd_mongo)
    # print(df)

