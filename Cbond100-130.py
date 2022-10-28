import datetime  #共270行代码，当精简之。20220907
import numpy as np
import copy
import pandas as pd
MAX = np.inf
extra_num = 2

start = '2021-01-01' # 回测起始时间
end = '2021-08-26'   # 回测结束时间

# 参数设置
SELL_PRICE = 130
BUY_PRICE = 100
    
benchmark = 'HS300'  # 策略参考标准
freq = 'd'  
rotation_rate = 5    # 调仓频率
current_day =0       
hold_num = 10        # 持有转债的个数
history_profit=[]
daily_netvalue =[]
holding_list=[]
ratation_list=[]
EB_ENABLE=False
refresh_rate = 1 # 调仓频率，表示执行handle_data的时间间隔，若freq = 'd' 时间间隔的单位为交易日，取盘前数据，若freq = 'm' 时间间隔为分钟

def formator():
    print('='*20)
    print('\n')
    
def initialize(context):
    global MyPosition, HighValue, MyCash, Withdraw, HoldRank, HoldNum,Start_Cash
    MyPosition = {}  #持仓
    MyCash = 1000000  #现金
    Start_Cash= 1000000
    HighValue = MyCash  #最高市值
    Withdraw = 0  #最大回撤
    HoldRank = hold_num  #排名多少之后卖出
    HoldNum = hold_num  #持债支数
    
def get_bonds_list(beginDate=u"20170101", endDate=u"20201215"): #获取可转债代码、简称和正股代码。
    df = DataAPI.MktConsBondPremiumGet(SecID=u"",tickerBond=u"",beginDate=beginDate,endDate=endDate,field=u"",pandas="1")
    cb_df = df.tickerBond.str.startswith(('12', '11'))  #取代码12或11开头的。
    df = df[cb_df]
    cb_df = df.tickerBond.str.startswith('117')         #删除代码117开头的。
    df = df[~cb_df]
    if not EB_ENABLE:
        eb = df.secShortNameBond.str.match('\d\d.*?E[123B]')  # TODO 判断EB是否过滤
        df = df[~eb]
    ticker_list = []
    for _, row in df[['tickerBond', 'secShortNameBond', 'tickerEqu']].iterrows():
        ticker_list.append((row['tickerBond'], row['secShortNameBond'], row['tickerEqu']))
    return ticker_list

def get_last_price(code, start_date, last_data):
    bond_info_df = DataAPI.MktBonddGet(secID=u"", ticker=code,tradeDate=u"",
                                       beginDate=start_date,endDate=last_data,field=u"closePrice",pandas="1")
    if len(bond_info_df) > 0:
        price = round(bond_info_df['closePrice'].iloc[-1], 2)
    else:
        price = 100
    return price
 
def get_position_netvalue(today_date):
    global PosValue,MyCash,HighValue,Withdraw,Start_Cash,daily_netvalue,holding_list
    ticker_list = list(MyPosition.keys())
    if len(ticker_list)==0:
        print('{}没有持仓'.format(today_date))
        return 
    
    data = DataAPI.MktConsBondPerfGet(beginDate=today_date,endDate=today_date,secID='',tickerBond=ticker_list, tickerEqu=u"",
        field=u"tickerBond,closePriceBond,bondPremRatio,secShortNameBond,tickerEqu,remainSize,chgPct",pandas="1")    
    if len(data)==0:
        return
    
    data=data.set_index('tickerBond',drop=False)
    PosValue = MyCash
    for ticker in ticker_list:
        try:
            closePriceBond = data.loc[ticker,'closePriceBond']
        except Exception as e:
            # print(e)
            print('没有查询到价格,ticker {}'.format(ticker))
            today_parse = datetime.datetime.strptime(today_date,'%Y%m%d')
            start_date = (today_parse + datetime.timedelta(days=-14)).strftime('%Y%m%d')
            closePriceBond=get_last_price(ticker,start_date, today_date)
        bond_pos = MyPosition[ticker]*closePriceBond*10
        PosValue+=bond_pos
        
    if PosValue > HighValue: HighValue = PosValue
    if (HighValue - PosValue) / HighValue > Withdraw: Withdraw = (HighValue - PosValue) / HighValue
    ratio = round((PosValue - Start_Cash) / Start_Cash * 100,2)
    message = today_date + ': 最高市值 ' + str(HighValue) + ' , 当前市值 ' + str(PosValue) + '收益率 ： ' + str(ratio) + '% , 最大回撤 ' + str(round(Withdraw * 100, 2)) + '%'
    
    df=data[data['tickerBond'].isin(ticker_list)]
    df=df.reset_index(drop=True)
    df['tradeDate']=today_date
    holding_list.append(df)
    daily_netvalue.append({'日期':today_date,'当前市值':PosValue,'收益率':ratio,'最大回撤':round(Withdraw * 100, 2)})
    # ==================================================================================================
    
def handle_data(context):    
    global MyPosition, HighValue, MyCash, Withdraw, HoldRank, HoldNum, Start_Cash, threshold,history_profit,current_day,holding_list,daily_netvalue,ratation_list
    previous_date = context.previous_date.strftime('%Y%m%d')
    today_date = context.now.strftime('%Y%m%d')

    if current_day%rotation_rate!=0:
        get_position_netvalue(today_date)
        current_day = current_day + 1
        return
    
    bonds_list = get_bonds_list(beginDate=today_date, endDate=today_date)
    if len(bonds_list) == 0:
        log.info('没有符合条件的转债')
        return

    ticker_dict = {}
    for ticker, name, ticker_zg in bonds_list:
        tmp_dict = {}
        ticker_dict[ticker] = {'name': name, 'zg': ticker_zg}
    ticker_list = list(ticker_dict.keys())   
    data = DataAPI.MktConsBondPerfGet(beginDate=today_date,endDate=today_date,
        secID='',tickerBond=ticker_list, tickerEqu=u"",
        field=u"tickerBond,closePriceBond,bondPremRatio,secShortNameBond,tickerEqu,remainSize,chgPct",pandas="1")
    
    data['secID'] = data['tickerBond']
    data.set_index('secID', inplace=True)

    data['doublelow'] = data['closePriceBond'] + data['bondPremRatio']
    data
    condition = "closePriceBond"     #条件为：按价格排序
    data = data.sort_values(by=condition , ascending=True)

    temp_df=data[:HoldNum + extra_num].copy()
    temp_df['tradeDate']=today_date
    ratation_list.append(temp_df)
    PosValue = MyCash
    
    # if HoldNum > len(data):
    #     HoldNum = len(data)

    last_bond_list = temp_df[:HoldNum]['tickerBond'].tolist()

    for stock in MyPosition.keys():
        try:
            CurPrice = data.loc[stock]['closePriceBond']
        except Exception as e:
            NoPrice = DataAPI.MktConsBondPerfGet(beginDate=previous_date,endDate=previous_date,
                                                 secID=stock,tickerBond=u"",tickerEqu=u"",
                                                 field=u"closePriceBond",pandas="1")
            if len(NoPrice) == 0:
                last_date = (context.now + datetime.timedelta(days=-7)).strftime('%Y%m%d')
                CurPrice = get_last_price(stock, last_date, today_date)
            else:
                CurPrice = NoPrice.closePriceBond[0]

        PosValue += MyPosition[stock] * CurPrice * 10  # 计算当前市值
        if CurPrice>SELL_PRICE:
        # if stock not in last_bond_list:
            MyCash += MyPosition[stock] * CurPrice * 10
            online = True         
            try:
                name_ = data.loc[stock]['secShortNameBond']
                chgPct = data.loc[stock]['chgPct']*100
            except:
                name_ = stock
                online =False            
            if online:
                cb_ration = data.loc[stock]['bondPremRatio']
            else:
                cb_ration=''
                chgPct=''
            message ='{} 卖出{}, {}, 价格 {}, 溢价率 {}, 当日涨幅{}'.format(today_date, stock, name_, CurPrice,
                                                                                cb_ration,chgPct)
            log.info(message)         
            history_profit.append(message)            
            del MyPosition[stock]
    
    if PosValue > HighValue: HighValue = PosValue
    if (HighValue - PosValue) / HighValue > Withdraw: Withdraw = (HighValue - PosValue) / HighValue
    less_than_price = data[data['closePriceBond']<BUY_PRICE]
    min_hold = min(HoldRank,len(less_than_price))
    
    for i in range(min_hold):# 买入排在HoldRank内的，总持有数量HoldNum
        if len(MyPosition) == HoldNum:break       
        price = data.loc[data.index[i]]['closePriceBond']
        if data.index[i] not in MyPosition.keys() and price<=BUY_PRICE:
            name = data.loc[data.index[i]]['secShortNameBond']
            cb_ration = data.loc[data.index[i]]['bondPremRatio']
            chgPct = data.loc[data.index[i]]['chgPct']*100
            message ='{} 买入{}, {}, 价格 {}, 溢价率 {}, 当日涨幅{}'.format(today_date, data.index[i], name, price,cb_ration,chgPct)
            log.info(message)
            history_profit.append(message)
            MyPosition[data.index[i]] = int(MyCash / (HoldNum - len(MyPosition)) / data['closePriceBond'][i] / 10)  
            MyCash -= MyPosition[data.index[i]] * data['closePriceBond'][i] * 10 

    ratio = round((PosValue - Start_Cash) / Start_Cash * 100,2)
    daily_netvalue.append({'日期':today_date,'当前市值':PosValue,'收益率':ratio,    '最大回撤':round(Withdraw * 100, 2)})
    log.info(today_date + ': 最高市值 ' + str(HighValue) + ' , 当前市值 ' + str(PosValue) + '收益率 ： '
             + str(ratio) + '% , 最大回撤 ' + str(round(Withdraw * 100, 2)) + '%')
    ticker_list = list(MyPosition.keys())
    
    df=data[data['tickerBond'].isin(ticker_list)]
    df=df.reset_index()
    df=df[['tickerBond','closePriceBond','bondPremRatio','secShortNameBond','tickerEqu','remainSize']]
    df['tradeDate']=today_date
    
    holding_list.append(df)
    current_day = current_day + 1
    formator()