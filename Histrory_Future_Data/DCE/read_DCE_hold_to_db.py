# !/usr/bin/env python3
# -*- coding:utf-8 -*-
'''
created by Fangyang on Time:2018/11/4
'''
from Histrory_Future_Data.pandas_db import PandasMongoDB

__author__ = 'Fangyang'

import pandas as pd
import os


def read_DCE_hold(file, file_open_way):
    '''

    :return: DataFrame
    '''
    if file_open_way is 'csv':
        df = pd.read_csv(file, engine='python')  # 因为名字中含有中文, 所以这里用python作为engine
    elif file_open_way is 'excel':
        df = pd.read_excel(file)
    df.dropna(axis=1, how='all', inplace=True)  # 去掉最后一列空列
    # DCE_used_columns_list = ['合约', '日期', '前收盘价', '前结算价', '开盘价', '最高价',
    #            '最低价', '收盘价', '结算价', '涨跌1', '涨跌2', '成交量', '成交金额', '持仓量']
    # df = df[DCE_used_columns_list]
    # df['合约'].fillna(method='ffill', inplace=True)  # SHFE用这句

    print(df.dtypes)
    df.drop('var', axis=1, inplace=True)
    df.rename(columns={"symbol": "code", "date": "datetime"}, inplace=True)

    df['datetime'] = df['datetime'].apply(str)
    df['datetime'] = df['datetime'].apply(lambda x: x[:4] + '-' + x[4:6] + '-' + x[6:])
    return df


if __name__ == '__main__':
    pd_mongo = PandasMongoDB()

    path_str = os.path.dirname(os.path.abspath(__file__))
    collection_name = 'hold_data'
    path_str = path_str + '\\' + collection_name
    print(path_str)
    files_list = os.listdir(path_str)
    print(files_list)

    #TODO 命名乱七八糟, 后续需要重新维护 (这部分代码用于保存/daily_data/2010 ... 这种文件结构)
    # for file in files_list:
    #     if not '.' in file:
    #         print(file)
    #         path_str2 = os.path.join(path_str, file)
    #         # print(path_str2)
    #         files_list2 = os.listdir(path_str2)
    #
    #         df_total = pd.DataFrame()
    #         for ff in files_list2:
    #             if '.csv' in ff:
    #                 ff_with_path = os.path.join(path_str2, ff)
    #                 print(ff_with_path)
    #                 try:
    #                     df = read_DCE_daily(ff_with_path, file_open_way='csv')
    #                 except:
    #                     df = read_DCE_daily(ff_with_path, file_open_way='excel')
    #                 finally:
    #                     df_total = df_total.append(df, ignore_index=True)
    #
    #         pd_mongo.write_df_json_to_db(df_total, database_name='DCE', collection_name=file)

    df_total = pd.DataFrame()
    for ff in files_list:
        if '.csv' in ff:
            ff_with_path = os.path.join(path_str, ff)
            print(ff_with_path)
            try:
                df = read_DCE_hold(ff_with_path, file_open_way='csv')
            except:
                df = read_DCE_hold(ff_with_path, file_open_way='excel')
            finally:
                df_total = df_total.append(df, ignore_index=True)

    pd_mongo.write_df_json_to_db(df_total, database_name='DCE', collection_name=collection_name)


    # pd_mongo = PandasMongoDB()
    #
    # df = read_SHFE_daily(file)
    # pd_mongo.write_df_json_to_db(df, database_name='SHFE', collection_name='option_daily_bar_{}'.format(i))

