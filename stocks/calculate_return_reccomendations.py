import pandas as pd
import os
import pandas_datareader.data as web
import pandas_datareader as pdr
from datetime import datetime
import matplotlib
import yfinance as yf
from datetime import timedelta
import numpy as np
yf.pdr_override()
ntickers = pd.read_csv("~/coding/mingquant/stocks/nasdaq_ticker_info.txt", sep="\t")
top_tickers = pd.read_csv("~/coding/mingquant/stocks/nasdaq_volume_leaders.csv")
midcap_tickers = pd.read_csv("~/coding/mingquant/stocks/mid_cap_volume_leaders.csv")
allsymbols = [i for i in set([x for x in midcap_tickers.Symbol] + [y for y in top_tickers.Symbol])]


def get_normalize_close(ticker, start, end):
    df = yf.download(ticker, auto_adjust = True, start = start, end = end)
    df[ticker] = df['Close']/df['Close'][0]
    return df[[ticker]]

def get_return(symbol, start, end):
    df = yf.download(symbol, auto_adjust = True, start = start, end = end, threads=False, progress=False)
    returnval = (df['Close'][-1]-df['Close'][0])/df['Close'][0]
    return returnval

def add_days(date, ndays=5):
    return str(pd.to_datetime(date) + pd.tseries.offsets.BusinessDay(ndays)).split(" ")[0]

def get_ratings_returns(symbol, start, end, ndays):  
    up_days = get_up_ratings(symbol, start, end)
    returns = [get_return(symbol, day, add_days(day, ndays)) for day in up_days]
    print("stock: " + symbol + " return: " + str(get_combined_return(returns)))
    return returns

def get_combined_return(allreturns):
    return np.mean([np.mean(x) for x in [[i for i in j[1] if str(i) != 'nan'] for j in allreturns]])

def open_close_time(teststart):
    openstart, closestart = pd.to_datetime(str(teststart).split(" ")[0] + ' 13:30:00'), pd.to_datetime(str(teststart).split(" ")[0] + ' 20:00:00')
    if pd.to_datetime(teststart) > pd.to_datetime(openstart) and pd.to_datetime(teststart) < pd.to_datetime(closestart):
        return "midday"
    elif pd.to_datetime(teststart) > pd.to_datetime(closestart):
        return 'aftermarket'
    else:
        return 'premarket'

def next_date(date):
    return pd.to_datetime(date) + pd.tseries.offsets.BusinessDay(1)

def get_adj_date(release_type, date):
    if release_type == 'midday' or release_type == 'aftermarket':
        return next_date(date)
    else:
        return date
    
def get_up_ratings(symbol, start, end):
    dft = pd.DataFrame(index=pd.bdate_range(start, end))
    ticker = yf.Ticker(symbol)
    try:
        openstart = str(start) + ' 13:30:00'
        closestart = str(start) + ' 20:00:00'
        recs = ticker.recommendations
        recs.loc[(recs.index >= start) & (recs.index <= end)]
        recs['date'] = recs.copy().index.astype('str')
        recs['timing'] = recs.apply(lambda x: open_close_time(x['date']), axis=1)
        recs['adj_date'] = recs.apply(lambda x: get_adj_date(x['timing'], x['date']), axis=1)
        return [str(i).split(' ')[0] for i in recs[recs['Action'] == "up"].adj_date]
    except:
        return None
    
def get_returns_recs(symbols, startdate, enddate, ndays):
    allreturns = []
    for symbol in symbols:
        if get_up_ratings(symbol, startdate, enddate):
            try:
                print("running for: " + str(symbol))
                returns = get_ratings_returns(symbol, startdate, enddate, ndays)
                allreturns.append([symbol, returns])
            except:
                print("failed for: " + str(symbol))
    return allreturns

# startdate = sys.argsv[1]
# enddate = sys.argsv[2]
# ndays = sys.argsv[3]

startdate = '2019-01-01'
enddate = '2019-07-30'
ndays = 5
symbols = allsymbols[:]
starttime = datetime.now()
allreturns = get_returns_recs(symbols, startdate, enddate, ndays)
combined_return = get_combined_return(allreturns)
print("combined return: " + str(combined_return))
print("all return length: " + str(len(allreturns)))
outdf = pd.DataFrame.from_dict({'symbol': [i[0] for i in allreturns], 'returns': [i[1] for i in allreturns]}, orient='columns')
outdf.to_csv("out_allreturns_may25.txt", sep="\t")
endtime = datetime.now()
print("this took: " + str(endtime-starttime))