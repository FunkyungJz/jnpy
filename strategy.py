# !/usr/bin/env python3
# -*- coding:utf-8 -*-
'''
created by Fangyang on Time:2018/11/3
'''
__author__ = 'Fangyang'

# 编写一个简单的双均线策略
import numpy as np
from jnpy.trader.app.ctaStrategy.ctaTemplate import CtaTemplate
import pandas as pd
from Histrory_Future_Data.pandas_db import PandasMongoDB
from jnpy.time_series_transform import gen_ts_columns
from model.model import Regression

import copy
from datetime import datetime


class DoubleMaStrategy(CtaTemplate):
    '''双均线策略demo'''
    className = 'DoubleMaStrategy'
    author = 'fangyang'

    # 策略参数
    initDays = 250  # 初始化数据所用的天数

    # 策略变量
    barCount = 0
    closeArray = np.zeros(20)
    ma5 = 0
    ma20 = 0
    lastMa5 = 0
    lastMa20 = 0

    # 参数列表, 保存了参数的名称
    paramList = ['name', 'className', 'author', 'vtSymbol']
    # 变量列表, 保存了变量的名称
    varList = ['inited', 'trading', 'pos']

    def __init__(self, ctaEngine, setting):
        super(DoubleMaStrategy, self).__init__(ctaEngine, setting)
        self.closeArray = np.zeros(20)
        self.data_df = pd.DataFrame()  # 用于缓存即将送入model的df
        self.pd_mongo = PandasMongoDB()
        self.hold_df = pd.DataFrame()

        self.ll = 0
        self.ss = 0
        self.coe = 2


    def onInit(self):
        '''初始化策略'''
        self.writeCtaLog(u'双均线策略初始化')

        initData = self.loadBar(self.initDays)
        for bar in initData:
            self.onBar(bar)
        self.putEvent()

    def onStart(self):
        self.writeCtaLog('双均线策略启动')
        self.putEvent()

    def onStop(self):
        self.writeCtaLog('双均线策略停止')
        self.putEvent()

    def onTick(self, tick):
        pass

    def onBar(self, bar):
        print(bar.close, type(bar.close))
        print(bar.datetime, type(bar.datetime))
        if not isinstance(bar.datetime, str):
            bar.datetime = datetime.strftime(bar.datetime, '%Y-%m-%d')
        # print(bar.volume, type(bar.volume))  # 不存在
        # print(bar.vtSymbol, type(vtSymbol))  # 不存在
        # print(self.vtSymbol, type(self.vtSymbol))

        query = {'datetime': '{}'.format(bar.datetime),
                 'code': self.vtSymbol}
        hold_data_df = self.pd_mongo.read_db(database_name='SHFE', collection_name='hold_data',
                                             query=(query, {'_id': 0}))

        bar_df = self.change_bar_cls_to_df(bar)
        bar_df = pd.merge(hold_data_df, bar_df, on=['datetime', 'code'])
        # self.closeArray[0:19] = self.closeArray[1:20]
        # self.closeArray[-1] = bar.close
        self.data_df = self.data_df.append(bar_df, ignore_index=True)
        self.barCount += 1
        if self.barCount < self.initDays:
            return

        #TODO
        # result = put_to_model_to_train()
        # 获得 t+1天 o h l c , t+2 天 o h l c

        columns = ['open', 'high', 'low', 'close']
        data_df_for_model = copy.deepcopy(self.data_df)
        data_df_for_model = gen_ts_columns(data_df_for_model, columns, 10)
        # print(self.data_df.columns)
        day1, day2, actual_value = Regression(data_df_for_model,
                      ['open_t+9', 'close_t+9', 'high_t+9', 'low_t+9'],
                      ['open_t+10', 'close_t+10', 'high_t+10', 'low_t+10'])
        print(day1, day2, actual_value)

        self.ll = day2['close_t+10'] - day1['open_t+9']
        self.ss = day1['open_t+9'] - day2['close_t+10']
        if self.ll > (self.coe * self.ss):
            if self.pos == 0:
                self.buy(min(day1['low_t+9'], bar.close), 100)
            elif self.pos < 0:
                self.cover(max(day1['low_t+9'], bar.close), 100)
                self.buy(min(day1['low_t+9'], bar.close), 100)

        elif self.ss > (self.coe * self.ll):
            if self.pos == 0:
                self.short(max(day1['high_t+9'], bar.close), 100)
            elif self.pos > 0:
                self.sell(min(day1['high_t+9'], bar.close), 100)
                self.short(max(day1['high_t+9'], bar.close), 100)

        self.data_df = self.data_df.drop([0])
        self.putEvent()
        # self.ma5 = self.closeArray[15:20].mean()
        # self.ma20 = self.closeArray.mean()

        # crossOver = self.ma5 > self.ma20 and self.lastMa5 <= self.lastMa20
        # crossBelow = self.ma5 < self.ma20 and self.lastMa5 >= self.lastMa20

        # if crossOver:
        #     if self.pos == 0:
        #         self.buy(bar.close * 1.05, 100)
        #     elif self.pos < 0:
        #         self.cover(bar.close * 1.05, 100)
        #         self.buy(bar.close * 1.05, 100)
        # elif crossBelow:
        #     if self.pos == 0:
        #         self.short(bar.close * 0.95, 100)
        #     elif self.pos > 0:
        #         self.sell(bar.close * 0.95, 100)
        #         self.short(bar.close * 0.95, 100)


    def onOrder(self, order):
        pass

    def onTrade(self, trade):
        pass

    def onStopOrder(self, so):
        pass

    def change_bar_cls_to_df(self, bar):
        bar_dict = dict()
        for name, value in vars(bar).items():
            bar_dict[name] = [value]

        return pd.DataFrame(bar_dict)

if __name__ == '__main__':

    from jnpy.trader.app.ctaStrategy.ctaBacktesting import BacktestingEngine

    engine = BacktestingEngine()
    engine.setBacktestingMode(engine.BAR_MODE)
    engine.setStartDate('20140301', initDays=120)

    # 设置产品相关参数
    engine.setSlippage(0)
    engine.setRate(2 / 10000)
    engine.setSize(1)
    engine.setPriceTick(1)
    engine.setCapital(1000000)

    # 设置历史数据库
    vtSymbol = 'RB'  # 作为collection的名字
    engine.setDatabase('SHFE_main', vtSymbol)

    # 在引擎中创建策略对象
    engine.initStrategy(DoubleMaStrategy, {'vtSymbol': 'RB'})

    engine.runBacktesting()
    engine.showDailyResult()


