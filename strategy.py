# !/usr/bin/env python3
# -*- coding:utf-8 -*-
'''
created by Fangyang on Time:2018/11/3
'''
__author__ = 'Fangyang'

# 编写一个简单的双均线策略
import numpy as np
from jnpy.trader.app.ctaStrategy.ctaTemplate import CtaTemplate


class DoubleMaStrategy(CtaTemplate):
    '''双均线策略demo'''
    className = 'DoubleMaStrategy'
    author = 'fangyang'

    # 策略参数
    initDays = 25  # 初始化数据所用的天数

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
        self.closeArray[0:19] = self.closeArray[1:20]
        self.closeArray[-1] = bar.close

        self.barCount += 1
        if self.barCount < self.initDays:
            return

        self.ma5 = self.closeArray[15:20].mean()
        self.ma20 = self.closeArray.mean()

        crossOver = self.ma5 > self.ma20 and self.lastMa5 <= self.lastMa20
        crossBelow = self.ma5 < self.ma20 and self.lastMa5 >= self.lastMa20

        if crossOver:
            if self.pos == 0:
                self.buy(bar.close * 1.05, 100)
            elif self.pos < 0:
                self.cover(bar.close * 1.05, 100)
                self.buy(bar.close * 1.05, 100)
        elif crossBelow:
            if self.pos == 0:
                self.short(bar.close * 0.95, 100)
            elif self.pos > 0:
                self.sell(bar.close * 0.95, 100)
                self.short(bar.close * 0.95, 100)

        self.putEvent()

    def onOrder(self, order):
        pass

    def onTrade(self, trade):
        pass

    def onStopOrder(self, so):
        pass


if __name__ == '__main__':

    from jnpy.trader.app.ctaStrategy.ctaBacktesting import BacktestingEngine

    engine = BacktestingEngine()
    engine.setBacktestingMode(engine.BAR_MODE)
    engine.setStartDate('20131008', initDays=20)

    # 设置产品相关参数
    engine.setSlippage(0)
    engine.setRate(2 / 10000)
    engine.setSize(1)
    engine.setPriceTick(1)
    engine.setCapital(1000000)

    # 设置历史数据库
    vtSymbol = 'from_df_json'  # 作为collection的名字
    engine.setDatabase('mydatabasename', vtSymbol)

    # 在引擎中创建策略对象
    engine.initStrategy(DoubleMaStrategy, {})

    engine.runBacktesting()
    engine.showDailyResult()


