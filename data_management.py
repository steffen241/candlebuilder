import shutil
import datetime as dt
import urllib.request
import numpy as np
import pandas as pd
import requests
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.parquet as pq
import databento as db

month_list = ['01','02','03','04','05','06','07','08','09','10','11','12']

mo_schema = pa.schema([
            pa.field('ts', pa.float64()),
            pa.field('side', pa.int8()),
            pa.field('price', pa.float32()),
            pa.field('num_trades', pa.int16()),
            pa.field('size', pa.float32())])

fn = 'SUIUSDT-aggTrades-2024-01.parquet'

start_date = '2024-04-01 0:00'
end_date = '2024-04-23 1:00'


def create_databento(symbol,files,out_fn):
    i = 0
    for d in files:
        print (d)
        tmp = db.from_dbn(d).to_ndarray()
        ts = tmp['ts_event']/1e6
        quantity = tmp['size']
        price = tmp['price']/1e9
        num_trades = np.ones(len(tmp))
        side = np.zeros(len(tmp))
        tmp_side = tmp['side'].astype('str')
        side[tmp_side=='A'] = -1
        side[tmp_side=='B'] = 1
        tmp2 = pa.Table.from_arrays([ts,side,price,num_trades,quantity], schema=mo_schema)
        if i==0:
            table = tmp2
        else:
            table = pa.concat_tables([table,tmp2])
        i = i+1
    pq.write_table(table,out_fn,compression='brotli')

def load_aggTrades(symbol,fn_base,start_date, end_date):
    month_list = ['01','02','03','04','05','06','07','08','09','10','11','12']
    start_dt = dt.datetime.strptime(start_date, '%Y-%m-%d %H:%M').replace(tzinfo=dt.timezone.utc)
    start_ts = start_dt.timestamp()*1e3
    end_dt = dt.datetime.strptime(end_date, '%Y-%m-%d %H:%M').replace(tzinfo=dt.timezone.utc)
    end_ts = end_dt.timestamp()*1e3

    fn_base = symbol+fn_base+str(start_dt.year)+'-'
    fn = fn_base+month_list[start_dt.month-1]+'.parquet'

    print ('Loading: ',fn)
    data = pq.read_table(fn,filters=[('ts','>=',start_ts),('ts','<',end_ts)])
    # do we need to load more months?
    if (start_dt.month != end_dt.month):
        print ('Loading more months:',month_list[start_dt.month:end_dt.month])
        for m in month_list[start_dt.month:end_dt.month]:
            fn = fn_base+m+'.parquet'
            print ('Loading: ',fn)
            tmp_data = pq.read_table(fn,filters=[('ts','>=',start_ts),('ts','<',end_ts)])
            data = pa.concat_tables([data,tmp_data])
    return data

def download_aggTrades(symbol):
    month_list = ['01','02','03','04','05','06','07','08']

    for month in month_list:
        fn = symbol+'-aggTrades-2024-'+month+'.zip'
        url = 'https://data.binance.vision/data/futures/um/monthly/aggTrades/'+symbol+'/'+fn
        print ('Download: '+url)
        urllib.request.urlretrieve(url,fn)
        shutil.unpack_archive(fn)
        print ('Unpacked')

        #fn = 'Z:\\storage\\binance\\SUIUSDT-aggtrades-2024-'+month+'.csv'
        fn = symbol+'-aggTrades-2024-'+month+'.csv'
        tmp = csv.read_csv(fn)
        num_trades = tmp['last_trade_id'].to_numpy()-tmp['first_trade_id'].to_numpy()+1
        quantity = tmp['quantity'].to_numpy()
        ts = tmp['transact_time'].to_numpy()
        price = tmp['price'].to_numpy()
        side = np.zeros(len(tmp['price']))
        side[tmp['is_buyer_maker'].to_numpy()==False]=1
        side[tmp['is_buyer_maker'].to_numpy()==True]=-1

        print ('create parquet from '+fn)
        tmp2 = pa.Table.from_arrays([ts,side,price,num_trades,quantity], schema=mo_schema)
        out_fn=symbol+'-aggtrades-2024-'+month+'.parquet'
        pq.write_table(tmp2,out_fn,compression='brotli')
