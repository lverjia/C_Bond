#集思录walkerdu分享2021-08-21。
#对原始代码中ConBonds['SecID']的获取进行了更新！即通过MktConsBondPremiumGet获取后并去重。2022-11-03
from datetime import datetime
import pandas as pd
start = '2021-01-01'                       # 回测起始时间
end = '2021-08-19'                         # 回测结束时间
benchmark = 'HS300'                        # 策略参考标准
freq = 'd'                                 # 策略类型，'d'表示日间策略使用日线回测，'m'表示日内策略使用分钟线回测
refresh_rate = (1, ['14:52'])              # 调仓频率，表示执行handle_data的时间间隔，若freq = 'd' 时间间隔的单位为交易日，取盘前数据，若freq = 'm' 时间间隔为分钟

def initialize(context):
    global MyPosition, HighValue, MyCash, Withdraw, HoldRank, HoldNum, initData
    MyPosition = {}    #持仓
    MyCash = 1000000   #现金
    HighValue = MyCash #最高市值
    Withdraw = 0       #最大回撤
    HoldRank = 10      #排名多少之后卖出
    HoldNum = 4        #持债支数

    ConBonds = DataAPI.BondGet(typeID="02020113",field=u"secID",pandas="1")
    ZhuanGu = DataAPI.BondConvStockItemGet(secID=ConBonds['secID'],field=u"secID,convEndtime,convStoptime",pandas="1")
    MeiRi = DataAPI.MktConsBondPerfGet(beginDate=start,endDate=end,secID=ConBonds['secID'],field=u"tradeDate,secID,closePriceBond,bondPremRatio",pandas="1")
    initData = pd.merge(MeiRi,ZhuanGu,on='secID')
    initData.fillna('2031-01-01',inplace = True)
    initData['Daoqi'] = 0

    for i in range(len(initData)):
        initData.iloc[i,6] = (datetime.strptime(initData.iloc[i,0],"%Y-%m-%d") - datetime.strptime(min(initData.iloc[i,5],initData.iloc[i,4]),"%Y-%m-%d")).days

def handle_data(context):    
    global MyCash, HighValue, Withdraw
    today_date = context.now.strftime('%Y-%m-%d')

    #每天重新计算双低排名
    data = initData.query('tradeDate=="' + today_date +'" and Daoqi < 0') #如果用不同的调仓频率，比如3天调仓，就把 < 0 改成 < -3
    data.set_index('secID',inplace=True)

    data['DoubleLow'] = data.closePriceBond + data.bondPremRatio
    data = data.sort_values(by="DoubleLow" , ascending=True)
    PosValue = MyCash
    
    #抛出不在持有排名HoldRank的
    for stock in MyPosition.keys():
        CurPrice = DataAPI.MktConsBondPerfGet(beginDate=today_date,endDate=today_date,secID=stock,field=u"closePriceBond",pandas="1").closePriceBond[0]
        PosValue += MyPosition[stock] * CurPrice * 10    #计算当前市值
        if stock not in data.index[:HoldRank]:
            MyCash += MyPosition[stock] * CurPrice * (1-0.0001)  # 卖出后回收现金，交易摩擦成本按万分之一
            del MyPosition[stock]
    if PosValue > HighValue:HighValue = PosValue         #更新最高市值
    if (HighValue - PosValue) / HighValue > Withdraw:Withdraw = (HighValue - PosValue) / HighValue #更新最大回撤
    
    #买入排在HoldRank内的，总持有数量HoldNum，通过现金和理论仓位对买入的仓位进行了平衡
    BuyMoney = min(MyCash / (HoldNum - len(MyPosition)), (MyCash / (HoldNum - len(MyPosition)) + PosValue / HoldNum) / 2)
    for i in range(HoldRank):
        if len(MyPosition) == HoldNum or len(data.index) < HoldNum:break
        if data.index[i] not in MyPosition.keys():
            MyPosition[data.index[i]] = int(BuyMoney / data['closePriceBond'][i] / 10)
            MyCash -= MyPosition[data.index[i]] * data['closePriceBond'][i] * 10
    print(today_date + ' , ' + str(HighValue) + ' , ' + str(PosValue) + ' , ' + str(Withdraw))
    print(MyPosition)