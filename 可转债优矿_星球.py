#来自知识星球：量化分析与程序交易的文章“优矿回测可转债双低 代码分析”2021年07月27日
import datetime

start = '2018-01-01'                       # 回测起始时间
end = '2021-05-28'                         # 回测结束时间
benchmark = 'HS300'                        # 策略参考标准
freq = 'd'                                 # 策略类型，'d'表示日间策略使用日线回测，'m'表示日内策略使用分钟线回测            
refresh_rate = 5 # 调仓频率，表示执行handle_data的时间间隔，若freq = 'd' 时间间隔的单位为交易日，
hold_num = 10 # 持有转债的个数

    
def initialize(context):
    global MyPosition, HighValue, MyCash, Withdraw, HoldRank, HoldNum,Start_Cash
    MyPosition = {}  #持仓
    MyCash = 1000000  #现金
    Start_Cash= 1000000
    HighValue = MyCash  #最高市值
    Withdraw = 0  #最大回撤
    HoldRank = hold_num  #排名多少之后卖出
    HoldNum = hold_num  #持债支数
    
def bonds(beginDate=u"20170101",endDate=u"20201215",EB_ENABLE=False):
    code_set = set()
    df = DataAPI.MktConsBondPremiumGet(SecID=u"",tickerBond=u"",beginDate=beginDate,endDate=endDate,
                                       field=u"",pandas="1")
    cb_df = df.tickerBond.str.startswith(('12', '11'))
    df = df[cb_df]
    cb_df = df.tickerBond.str.startswith('117')
    df = df[~cb_df]
    if not EB_ENABLE:
        eb = df.secShortNameBond.str.match('\d\d.*?E[123B]')  # TODO 判断EB是否过滤
        df = df[~eb]

    ticker_list = []
    for _, row in df[['tickerBond', 'secShortNameBond', 'tickerEqu']].iterrows():
        if row['tickerBond'] not in code_set:
            ticker_list.append((row['tickerBond'], row['secShortNameBond'], row['tickerEqu']))
            code_set.add(row['tickerBond'])
    return list(code_set)

def get_last_price(code,start_date,last_data):
    bond_info_df =DataAPI.MktBonddGet(ticker=code,tradeDate=u"",beginDate=start_date,endDate=last_data,
                                      field=u"closePrice",pandas="1")
    if len(bond_info_df)>0:
        price=round(bond_info_df['closePrice'].iloc[-1],2)
    else:
        price=100 
    return price
        
def handle_data(context):    
    global MyPosition, HighValue, MyCash, Withdraw, HoldRank, HoldNum,Start_Cash
    
    today_date = context.now.strftime('%Y%m%d')
    
    #每天重新计算双低排名
    ticker_list=bonds(today_date,today_date)
    data = DataAPI.MktConsBondPerfGet(beginDate=today_date,endDate=today_date,secID='',tickerBond=ticker_list,
                                      tickerEqu=u"",field=u"",pandas="1")
    data['secID']=data['tickerBond']
    data.set_index('secID',inplace=True)
    data['DoubleLow'] = data['closePriceBond'] + data['bondPremRatio']
    data = data.sort_values(by="DoubleLow" , ascending=True)
    PosValue = MyCash
    
    #抛出不在持有排名HoldRank的
    for stock in MyPosition.keys():
        try:
            CurPrice = data.loc[stock]['closePriceBond']
        except:
            last_date = (context.now + datetime.timedelta(days=-7)).strftime('%Y%m%d')
            CurPrice=get_last_price(stock,last_date,today_date)

        PosValue += MyPosition[stock] * CurPrice * 10 #计算当前市值
        
        if stock not in data.index[:HoldRank]:
            MyCash += MyPosition[stock] * CurPrice * 10
            try:
                name=data.loc[stock]['secShortNameBond']
            except:
                name = stock
                
            log.info('{} 卖出{},{},价格：{}'.format(today_date,stock,name,CurPrice))
            
            del MyPosition[stock]
            
    if PosValue > HighValue:HighValue = PosValue
    if (HighValue - PosValue) / HighValue > Withdraw:Withdraw = (HighValue - PosValue) / HighValue
    
    #买入排在HoldRank内的，总持有数量HoldNum
    min_hold = min(HoldRank,len(data.index))
    for i in range(min_hold):
        if len(MyPosition) >= HoldNum:break
        if data.index[i] not in MyPosition.keys():
            name=data.loc[data.index[i]]['secShortNameBond']
            price=data.loc[data.index[i]]['closePriceBond']
            cb_ration=data.loc[data.index[i]]['bondPremRatio']
            log.info('{} 买入{}, {}, 价格{}, 溢价率{}'.format(today_date,data.index[i],name,price,cb_ration))
            MyPosition[data.index[i]] = int(MyCash / (HoldNum - len(MyPosition)) / data['closePriceBond'][i] / 10) # 简单粗暴地资金均分买入
            MyCash -= MyPosition[data.index[i]] * data['closePriceBond'][i] * 10 # 买入时不再计算交易摩擦成本，直接扣减
    
    ratio = (PosValue-Start_Cash)/Start_Cash*100
    log.info(today_date + ': 最高市值 ' + str(HighValue) + ' , 当前市值 ' + str(PosValue) + '收益率 ： '
             +str(ratio)+'% , 最大回撤 ' + str(round(Withdraw*100,2))+'%') 