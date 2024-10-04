import matplotlib.pyplot as m
import plotly.graph_objects as go

import pandas as pd
import numpy as np
import talib as ta
import time as time

from Backtester import *
from Candleseries import *
from Orderflow import *
from helper import *
from data_management import *

#%%
# load data, now pretty fast and nice :)
# data_raw = load_aggTrades('REEFUSDT', '-aggTrades-', '2024-03-31 22:00', '2024-04-01 21:00')
data_raw = load_aggTrades('ES', '-', '2024-03-31 22:00', '2024-04-01 21:00')
data_raw = np.array(data_raw)

#%%
orderflow_window = 100
cs_m1 = Candleseries(ctype='time',interval=1)
cs_t40 = Candleseries(ctype='time',interval=0.1666)
of = Orderflow()

mtf_mapping = []
import time

start = time.time()
for i in range(0,len(data_raw)):
    new_tick = of.add_tick(data_raw[i])
    if new_tick:
        of.calc_tape_velocity(orderflow_window)
        # print (of.vel_t[of.st_idx_t])
    state_m1 = cs_m1.add_tick(data_raw[i],of)
    state_t40 = cs_t40.add_tick(data_raw[i],of)

    if state_t40 == 'closed':
        mtf_mapping.append((cs_t40.st_idx, cs_m1.st_idx-1))
        # print (cs_t40.vel_t_ohlc[:,cs_t40.st_idx])
    if state_m1 == 'closed':
        if cs_m1.st_idx > 5:
            orderflow_window = np.int_(np.mean(cs_m1.num_trades[-5+cs_m1.st_idx+1:cs_m1.st_idx+1]))
            # print (orderflow_window)
# we need to update the storage to get the last candle because it is not closed
cs_m1.update_storage()
cs_t40.update_storage()
end = time.time()
print(end - start)

#%%
num_trades = []
vol = []
atr = []
for i in range(0,len(cs_m1.clist)):
    num_trades.append(cs_m1.clist[i].num_trades)
    vol.append(cs_m1.clist[i].vol)
    atr.append(cs_m1.clist[i].h-cs_m1.clist[i].l)

#%%
m1 = cs_t40.ohlc[:,:cs_t40.st_idx+1]
m1_vel_t = cs_t40.vel_b_ohlc[:,:cs_t40.st_idx+1]
m1_vel_delta = cs_t40.vel_delta_ohlc[:,:cs_t40.st_idx+1]
m1_close = cs_m1.ohlc[3,np.array(mtf_mapping)[:,1]]
m1_ema21 = (ta.EMA(cs_m1.ohlc[3,:cs_m1.st_idx+1],21))
mtf_m1_ema21 = m1_ema21[np.array(mtf_mapping)[:,1]]
m1_ema9 = (ta.EMA(cs_m1.ohlc[3,:cs_m1.st_idx+1],9))
mtf_m1_ema9 = m1_ema9[np.array(mtf_mapping)[:,1]]

v500_tvcd = cs_t40.tvcd_ohlc[:,:cs_t40.st_idx+1]

fig = ps.make_subplots(rows=3,cols=1,shared_xaxes=True)
fig.add_trace(go.Candlestick(open=m1[0,:],high=m1[1,:],low=m1[2,:],close=m1[3,:],name='price M1'),row=1,col=1)
fig.add_trace(go.Candlestick(open=m1_vel_t[0,:],high=m1_vel_t[1,:],low=m1_vel_t[2,:],close=m1_vel_t[3,:],name='velocity'),row=2,col=1)
# fig.add_trace(go.Candlestick(open=m1_vel_t[0,:],high=m1_vel_t[1,:],low=m1_vel_t[2,:],close=m1_vel_t[3,:],name='velocity sell'),row=2,col=1)

fig.add_trace(go.Candlestick(open=m1_vel_delta[0,:],high=m1_vel_delta[1,:],low=m1_vel_delta[2,:],close=m1_vel_delta[3,:],name='delta velocity'),row=3,col=1)

#fig.add_trace(go.Candlestick(open=v500_tvcd[0,:],high=v500_tvcd[1,:],low=v500_tvcd[2,:],close=v500_tvcd[3,:],name='up/downtick cumdelta'),row=2,col=1)
# fig.add_trace(go.Scatter(y=pd.DataFrame(m1_vel_delta[3,:]).rolling(40).quantile(0.9).values[:,0],mode='lines',name='90% percentile'),row=3,col=1)
# fig.add_trace(go.Scatter(y=pd.DataFrame(m1_vel_delta[3,:]).rolling(40).quantile(0.1).values[:,0],mode='lines',name='90% percentile'),row=3,col=1)
# fig.add_trace(go.Scatter(y=m1_close,mode='lines',name='M1 close'),row=1,col=1)
fig.add_trace(go.Scatter(y=mtf_m1_ema21,mode='lines',name='M1 EMA 21'),row=1,col=1)
fig.add_trace(go.Scatter(y=mtf_m1_ema9,mode='lines',name='M1 EMA 9'),row=1,col=1)

fig.update_xaxes(rangeslider_visible=False)
fig.update_traces(increasing_fillcolor='cyan', selector=dict(type='candlestick'))
fig.update_traces(increasing_line_color='blue', selector=dict(type='candlestick'))
fig.update_traces(decreasing_fillcolor='orange', selector=dict(type='candlestick'))
fig.update_traces(decreasing_line_color='orange', selector=dict(type='candlestick'))

#%%

def export_to_vbt(data):
    e_idx = data.st_idx+1
    # data_index = data.time[:e_idx]
    data_index = np.arange(0,e_idx)
    vbt_dat = pd.DataFrame()
    vbt_dat.index = data_index #pd.to_datetime(data.time[:e_idx].astype('datetime64[ms]'))
    vbt_dat['open'] = data.ohlc[0,:e_idx]
    vbt_dat['high'] = data.ohlc[1,:e_idx]
    vbt_dat['low'] = data.ohlc[2,:e_idx]
    vbt_dat['close'] = data.ohlc[3,:e_idx]
    vbt_dat['tvcd open'] = data.tvcd_ohlc[0,:e_idx]
    vbt_dat['tvcd high'] = data.tvcd_ohlc[1,:e_idx]
    vbt_dat['tvcd low'] = data.tvcd_ohlc[2,:e_idx]
    vbt_dat['tvcd close'] = data.tvcd_ohlc[3,:e_idx]
    vbt_dat['veld open'] = data.vel_delta_ohlc[0,:e_idx]
    vbt_dat['veld high'] = data.vel_delta_ohlc[1,:e_idx]
    vbt_dat['veld low'] = data.vel_delta_ohlc[2,:e_idx]
    vbt_dat['veld close'] = data.vel_delta_ohlc[3,:e_idx]
    vbt_dat['vel open'] = data.vel_t_ohlc[0,:e_idx]
    vbt_dat['vel high'] = data.vel_t_ohlc[1,:e_idx]
    vbt_dat['vel low'] = data.vel_t_ohlc[2,:e_idx]
    vbt_dat['vel close'] = data.vel_t_ohlc[3,:e_idx]    
    vbt_dat['delta open'] = data.delta_hlc[0,:e_idx]
    vbt_dat['delta high'] = data.delta_hlc[1,:e_idx]
    vbt_dat['delta low'] = data.delta_hlc[2,:e_idx]
    vbt_dat['delta close'] = data.delta_hlc[3,:e_idx]
    vbt_dat['tvdelta open'] = data.tvdelta_hlc[0,:e_idx]
    vbt_dat['tvdelta high'] = data.tvdelta_hlc[1,:e_idx]
    vbt_dat['tvdelta low'] = data.tvdelta_hlc[2,:e_idx]
    vbt_dat['tvdelta close'] = data.tvdelta_hlc[3,:e_idx]  
    return vbt_dat
    
data_15s = export_to_vbt(cs_t40)
data_1m = export_to_vbt(cs_m1)