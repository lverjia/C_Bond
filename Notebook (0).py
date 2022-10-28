
# coding: utf-8

# In[ ]:

start = '2020-01-02'                       # 回测起始时间
end = '2020-11-13'                         # 回测结束时间
universe = DynamicUniverse('HS300')        # 证券池，支持股票、基金、期货、指数四种资产
benchmark = 'HS300'                        # 策略参考标准
freq = 'd'                                 # 策略类型，'d'表示日间策略使用日线回测，'m'表示日内策略使用分钟线回测
refresh_rate = (1, ['14:52'])              # 调仓频率，表示执行handle_data的时间间隔，若freq = 'd' 时间间隔的单位为交易日，取盘前数据，若freq = 'm' 时间间隔为分钟

def initialize(context):
    global MyPosition, HighValue, MyCash, Withdraw, HoldRank, HoldNum
    MyPosition = {}  #持仓
    MyCash = 100000  #现金
    HighValue = MyCash  #最高市值
    Withdraw = 0  #最大回撤
    HoldRank = 10  #排名多少之后卖出
    HoldNum = 4  #持债支数
    
def handle_data(context):    
    global MyCash, HighValue, Withdraw
    previous_date = context.previous_date.strftime('%Y%m%d')
    today_date = context.now.strftime('%Y%m%d')
    
    #每天重新计算双低排名
    ConBonds = DataAPI.SecIDGet(partyID=u"",ticker=u"",cnSpell=u"",assetClass=u"B",exchangeCD="XSHE,XSHG",listStatusCD="",field=u"secID",pandas="1")
    data = DataAPI.MktConsBondPerfGet(beginDate=today_date,endDate=today_date,secID=ConBonds['secID'],tickerBond=u"",tickerEqu=u"",field=u"secID,closePriceBond,bondPremRatio",pandas="1")
    data.set_index('secID',inplace=True)
    data['DoubleLow'] = data.closePriceBond + data.bondPremRatio
    data = data.sort_values(by="DoubleLow" , ascending=True)
    PosValue = MyCash
    
    #抛出不在持有排名HoldRank的
    for stock in MyPosition.keys():
        try:
            CurPrice = data.loc[stock]['closePriceBond']
        except:
            NoPrice = DataAPI.MktConsBondPerfGet(beginDate=previous_date,endDate=previous_date,secID=stock,tickerBond=u"",tickerEqu=u"",field=u"closePriceBond",pandas="1")
            CurPrice = NoPrice.closePriceBond[0]
        PosValue += MyPosition[stock] * CurPrice * 10 #计算当前市值
        if stock not in data.index[:HoldRank]:
            MyCash += MyPosition[stock] * CurPrice * 9.9 # 卖出后回收现金，交易摩擦成本按百分之一，10*0.99=9.9
            del MyPosition[stock]
    if PosValue > HighValue:HighValue = PosValue
    if (HighValue - PosValue) / HighValue > Withdraw:Withdraw = (HighValue - PosValue) / HighValue
    
    #买入排在HoldRank内的，总持有数量HoldNum
    for i in range(HoldRank):
        if len(MyPosition) == HoldNum or len(data.index) < HoldNum:break
        if data.index[i] not in MyPosition.keys():
            MyPosition[data.index[i]] = int(MyCash / (HoldNum - len(MyPosition)) / data['closePriceBond'][i] / 10) # 简单粗暴地资金均分买入
            MyCash -= MyPosition[data.index[i]] * data['closePriceBond'][i] * 10 # 买入时不再计算交易摩擦成本，直接扣减
    print(today_date + ': 最高市值 ' + str(HighValue) + ' , 当前市值 ' + str(PosValue) + ' , 最大回撤 ' + str(round(Withdraw*100,2)))
    print(MyPosition)

