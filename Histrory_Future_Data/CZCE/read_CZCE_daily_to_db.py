# !/usr/bin/env python3
# -*- coding:utf-8 -*-
'''
created by Fangyang on Time:2018/11/4
'''

__author__ = 'Fangyang'

import traceback
import pandas as pd
import numpy as np
from Histrory_Future_Data.pandas_db import PandasMongoDB


# def del_whitespace(var):
#     """
#     convert the string number to a float
#     _ 去除空格
#     """
#     new_value = var.replace(' ', '')
#     return new_value
#
#
# def del_comma(var):
#     """
#     convert the string number to a float
#     _ 去除$
#     - 去除逗号，
#     - 转化为浮点数类型
#     """
#     new_value = var.replace(",", "")
#     return float(new_value)

def read_option_txt(file, encoding):
    '''
    通过pandas读取txt表为 DataFrame,
    列名和数据去空格处理, 价格数据原始为str, 去逗号转换为相应类型
    常用列名转英文
    返回DataFrame
    '''
    df = pd.read_table(file, sep="|", skiprows=1, encoding=encoding, low_memory=False)  # 这里有mix type问题, 所以设置成false
    df.dropna(axis=1, how='all', inplace=True)   # 去掉空列
    print(df.dtypes)

    float_number_value_column_list = ["昨结算", "今开盘", "最高价", "最低价", "今收盘",
                                      "今结算", '涨跌1', '涨跌2', '成交额(万元)', '交割结算价',
                                      'DELTA', '隐含波动率']
    int_number_value_column_list = ['成交量(手)', '空盘量', '增减量', '行权量']

    df.columns = [i.strip() for i in df.columns.values]  # 去掉列名中的空格
    df_columns = df.columns

    for j in df_columns:

        if df[j].values.dtype == np.int64:
            df[j] = df[j].astype("float64")
            continue
        elif df[j].values.dtype == np.float64:
            continue

        # apply 只适用于单列, 而且处理后要赋值保存
        df[j] = df[j].apply(lambda x: x.replace(" ", ''))  # 去掉value中的空格
        if j in float_number_value_column_list:
            df[j] = df[j].apply(lambda x: x.replace(",", "")).astype("float64")  # 数值型的data转为浮点型
        elif j in int_number_value_column_list:
            df[j] = df[j].apply(lambda x: x.replace(",", "")).astype("int64")   # 成交量转为int

    # 常用列名英文化
    if "品种代码" in df_columns:
        code_chinese = "品种代码"
    else:
        code_chinese = "品种月份"

    df.rename(columns={"交易日期": "datetime",
                       code_chinese: "code",
                       "今开盘": "open",
                       "最高价": "high",
                       "最低价": "low",
                       "今收盘": "close",
                       "成交量(手)": "vol"}, inplace=True)

    return df


def read_future_txt(file, encoding):
    '''
    通过pandas读取txt表为 DataFrame,
    列名和数据去空格处理, 价格数据原始为str, 去逗号转换为相应类型
    常用列名转英文
    返回DataFrame
    '''
    df = pd.read_table(file, sep="|", skiprows=1, encoding=encoding, low_memory=False)  # 这里有mix type问题, 所以设置成false
    df.dropna(axis=1, how='all', inplace=True)   # 去掉空列
    print(df.dtypes)

    float_number_value_column_list = ["昨结算", "今开盘", "最高价", "最低价", "今收盘",
                                      "今结算", '涨跌1', '涨跌2', '成交额(万元)', '交割结算价']
    int_number_value_column_list = ['成交量(手)', '空盘量', '增减量']

    df.columns = [i.strip() for i in df.columns.values]  # 去掉列名中的空格
    df_columns = df.columns

    for j in df_columns:

        if df[j].values.dtype == np.int64:
            df[j] = df[j].astype("float64")
            continue
        elif df[j].values.dtype == np.float64:
            continue

        # apply 只适用于单列, 而且处理后要赋值保存
        df[j] = df[j].apply(lambda x: x.replace(" ", ''))  # 去掉value中的空格
        if j in float_number_value_column_list:
            df[j] = df[j].apply(lambda x: x.replace(",", "")).astype("float64")  # 数值型的data转为浮点型
        elif j in int_number_value_column_list:
            df[j] = df[j].apply(lambda x: x.replace(",", "")).astype("int64")   # 成交量转为int

    # 常用列名英文化
    if "品种代码" in df_columns:
        code_chinese = "品种代码"
    else:
        code_chinese = "品种月份"

    df.rename(columns={"交易日期": "datetime",
                       code_chinese: "code",
                       "今开盘": "open",
                       "最高价": "high",
                       "最低价": "low",
                       "今收盘": "close",
                       "成交量(手)": "vol"}, inplace=True)

    return df


def save_CZCE_daily_to_db(database_name, type, pd_mongo):

    # save future/option into db
    for i in range(2010, 2018+1):
        encoding = 'utf-8' if i >= 2017 else 'GB2312'
        try:
            df = read_future_txt('./{}/{}.txt'.format(type, i), encoding=encoding)
            # df['datetime'] = df['datetime'].apply(lambda x: int(x.replace('-', '')))
            pd_mongo.write_df_json_to_db(df, database_name=database_name, collection_name='{}_{}'.format(type, i))
        except Exception:
            traceback.print_exc()


if __name__ == '__main__':
    pd_mongo = PandasMongoDB()
    database_name = 'CZCE'

    save_CZCE_daily_to_db(database_name, type='future', pd_mongo=pd_mongo)
    save_CZCE_daily_to_db(database_name, type='option', pd_mongo=pd_mongo)


    # # save option into db
    # for i in range(2017, 2018+1):
    #     encoding = 'utf-8' if i >= 2017 else 'GB2312'
    #     df = read_option_txt('./option/{}.txt'.format(i), encoding=encoding)
    #     pd_mongo.write_df_json_to_db(df, database_name='CZCE', collection_name='option_daily_bar_{}'.format(i))