# !/usr/bin/env python3
# -*- coding:utf-8 -*-
'''
created by Fangyang on Time:2018/10/25
'''
__author__ = 'Fangyang'


import pandas as pd

def gen_ts_columns(df, columns_list, after_time, before_time=0, step=1):
    '''
    df : 传入需要调整的dataframe
    columns_list : 传入需要调整的列的list
    after_time : 今天之后多少天, int类型
    before: 今天之前多少天, int类型
    返回 调整后的 dataframe
    '''
    df.sort_values(by='datetime', inplace=True)  # 把时间列进行降序排序, 远期数据在上
    for i in range(-after_time, before_time, step):
        for column in columns_list:
            new_column_name = '{}_t{:+}'.format(column, -i)
            df[new_column_name] = df[column].shift(i)

    df.dropna(inplace=True)
    return df

if __name__ == '__main__':
    df = pd.read_csv('./data/merged_RB.csv')
    columns = ['open', 'high', 'low', 'close']
    df = gen_ts_columns(df, columns, 3)
    df.to_csv('./data/time_series_data.csv', index=False)


