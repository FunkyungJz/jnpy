# !/usr/bin/env python3
# -*- coding:utf-8 -*-
'''
created by Fangyang on Time:2018/11/4
'''
from Histrory_Future_Data.pandas_db import PandasMongoDB

__author__ = 'Fangyang'

import pandas as pd
import os


def read_SHFE_daily(file):
    '''

    :return: DataFrame
    '''
    df = pd.read_excel(file, skiprows=2, skipfooter=5)
    df.dropna(axis=1, how='all', inplace=True)  # 去掉最后一列空列
    df['合约'].fillna(method='ffill', inplace=True)

    print(df.dtypes)

    df.rename(columns={"日期": "datetime",
                       "合约": "code",
                       "开盘价": "open",
                       "最高价": "high",
                       "最低价": "low",
                       "收盘价": "close",
                       "成交量": "vol"}, inplace=True)

    # df['datetime'] = pd.to_datetime(df['datetime'], format='%Y%m%d', errors='ignore')
    df['datetime'] = df['datetime'].apply(str)
    df['datetime'] = df['datetime'].apply(lambda x: x[:4] + '-' + x[4:6] + '-' + x[6:])

    return df


if __name__ == '__main__':
    pd_mongo = PandasMongoDB()

    collection_name = 'daily_data'
    database_name = 'SHFE'

    path_str = os.path.dirname(os.path.abspath(__file__))
    path_str = path_str + '\\' + collection_name  # 生成 \\hold_data 路径
    print(path_str)
    files_list = os.listdir(path_str)
    print(files_list)

    #TODO 命名乱七八糟, 后续需要重新维护
    for file in files_list:
        if not '.' in file:
            print(file)
            path_str2 = os.path.join(path_str, file)
            print(path_str2)
            files_list2 = os.listdir(path_str2)

            for ff in files_list2:
                print(ff)
                ff_with_path = os.path.join(path_str2, ff)
                df = read_SHFE_daily(ff_with_path)
                pd_mongo.write_df_json_to_db(df, database_name=database_name, collection_name=file)  # 这里直接用文件名做表名


    # pd_mongo = PandasMongoDB()
    #
    # df = read_SHFE_daily(file)
    # pd_mongo.write_df_json_to_db(df, database_name='SHFE', collection_name='option_daily_bar_{}'.format(i))

