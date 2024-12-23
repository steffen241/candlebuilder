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
from event_queue import *

#%%
# load data, now pretty fast and nice :)
data_raw = load_aggTrades('REEFUSDT', '-aggtrades-', '2024-04-03 21:00', '2024-04-04 21:00')
# data_raw = load_aggTrades('ES', '-','2024-04-02 22:00', '2024-04-03 21:00') # '2024-03-31 22:00', '2024-04-01 21:00')
data_raw = np.array(data_raw)

#%%
orderflow_window = 100
of = Orderflow()

g = (data_raw[i,:] for i in range(0,len(data_raw)))#: #119000,119800))# len(data_raw)))
cs_m1 = Candleseries(name='m1',ctype='time',interval=1)
cs_s10 = Candleseries(name='s10',ctype='time',interval=0.16666)
def get_data_point():
    try:
        d.append(('TICK',next(g)))
        data_available = True
    except StopIteration:
        data_available = False
    return data_available

def tick_handler(data):
    # print(datetime.datetime.fromtimestamp(data[0]/1e3))
    new_tick = of.add_tick(data)
    of.calc_tape_velocity(orderflow_window)
    state_m1 = cs_m1.add_tick(data,of)
    state_s10 = cs_s10.add_tick(data,of)

def on_bar_handler(data):
    if data == 'm1':
        pass
        #print(cs_m1.name,len(cs_m1.clist),cs_m1.clist[-2].state, datetime.datetime.fromtimestamp(cs_m1.clist[-2].time/1e3))
    if data == 's10':
        pass
        #print(cs_s10.name,len(cs_s10.clist),cs_s10.clist[-2].state, datetime.datetime.fromtimestamp(cs_s10.clist[-2].time/1e3))

from time import perf_counter
start = perf_counter()
# i = 0
while True:
    data_available = get_data_point()
    if data_available:
        while len(d) > 0:
            e = d.popleft()
            if e[0] == 'TICK':
                tick_handler(e[1])
            elif e[0] == 'ON_BAR':
                on_bar_handler(e[1])
            else:
                break
    else:
        break
cs_m1.update_storage()
cs_s10.update_storage()

end = perf_counter()
print(end - start)

#%%
import plotly.subplots as ps
import plotly.graph_objects as go
import plotly.io as pio
pio.renderers.default = "browser"

m1 = cs_s10.ohlc[:,:cs_s10.st_idx+1] #cs_m1.ohlc[:,:cs_m1.st_idx+1]

v500_tvcd = cs_s10.tvcd_ohlc[:,:cs_s10.st_idx+1]

fig = ps.make_subplots(rows=2,cols=1,shared_xaxes=True)
fig.add_trace(go.Candlestick(open=m1[0,:],high=m1[1,:],low=m1[2,:],close=m1[3,:],name='price M1'),row=1,col=1)
# fig.add_trace(go.Candlestick(open=m1_vel_t[0,:],high=m1_vel_t[1,:],low=m1_vel_t[2,:],close=m1_vel_t[3,:],name='velocity'),row=2,col=1)
# fig.add_trace(go.Candlestick(open=m1_vel_t[0,:],high=m1_vel_t[1,:],low=m1_vel_t[2,:],close=m1_vel_t[3,:],name='velocity sell'),row=2,col=1)

# fig.add_trace(go.Candlestick(open=m1_vel_delta[0,:],high=m1_vel_delta[1,:],low=m1_vel_delta[2,:],close=m1_vel_delta[3,:],name='delta velocity'),row=3,col=1)

fig.add_trace(go.Candlestick(open=v500_tvcd[0,:],high=v500_tvcd[1,:],low=v500_tvcd[2,:],close=v500_tvcd[3,:],name='up/downtick cumdelta'),row=2,col=1)
# fig.add_trace(go.Scatter(y=pd.DataFrame(m1_vel_delta[3,:]).rolling(40).quantile(0.9).values[:,0],mode='lines',name='90% percentile'),row=3,col=1)
# fig.add_trace(go.Scatter(y=pd.DataFrame(m1_vel_delta[3,:]).rolling(40).quantile(0.1).values[:,0],mode='lines',name='90% percentile'),row=3,col=1)
# fig.add_trace(go.Scatter(y=m1_close,mode='lines',name='M1 close'),row=1,col=1)
# fig.add_trace(go.Scatter(y=mtf_m1_ema21,mode='lines',name='M1 EMA 21'),row=1,col=1)
# fig.add_trace(go.Scatter(y=mtf_m1_ema9,mode='lines',name='M1 EMA 9'),row=1,col=1)

fig.update_xaxes(rangeslider_visible=False)
fig.update_traces(increasing_fillcolor='cyan', selector=dict(type='candlestick'))
fig.update_traces(increasing_line_color='blue', selector=dict(type='candlestick'))
fig.update_traces(decreasing_fillcolor='orange', selector=dict(type='candlestick'))
fig.update_traces(decreasing_line_color='orange', selector=dict(type='candlestick'))