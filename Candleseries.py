# -*- coding: utf-8 -*-
"""
Created on Fri Jul 19 10:21:36 2024

@author: steff
"""
import numpy as np

class Candle:
    def __init__(self):
        self.o = 0
        self.h = 0
        self.l = 1e9
        self.c = 0
        self.delta_h = 0
        self.delta_l = 0
        self.delta = 0
        self.cum_delta_o = 0
        self.cum_delta_h = 0
        self.cum_delta_l = 0
        self.cum_delta_c = 0
        self.state = 'open'
        self.v = {}
        self.vol = 0
        self.delta = 0
        self.num_trades = 0
        self.time = 0
        self.time_mod = 0
        self.tick_dir = 'neutral'
        self.tv_delta = 0
        self.tv_delta_h = 0
        self.tv_delta_l = 0
        self.cum_tv_delta_o = 0
        self.cum_tv_delta_h = 0
        self.cum_tv_delta_l = 0
        self.cum_tv_delta_c = 0
        self.vel_t = 0
        self.vel_t_o = 0
        self.vel_t_h = 0
        self.vel_t_l = 1e10
        self.vel_b = 0
        self.vel_b_o = 0
        self.vel_b_h = 0
        self.vel_b_l = 1e10
        self.vel_s = 0
        self.vel_s_o = 0
        self.vel_s_h = 0
        self.vel_s_l = 1e10
        self.vel_delta = 0
        self.vel_delta_o = 0
        self.vel_delta_h = -1e10
        self.vel_delta_l = 1e10
    
    def start_candle(self,o,interval):
        self.o = o[2]
        self.h = o[2]
        self.l = o[2]
        self.c = o[2]
        self.num_trades = o[3]
        self.vol = o[4]
        self.state = 'running'
        self.time = o[0]
        self.time_mod = o[0]/1e3/interval//60
        
    def update_delta(self,o):
        # handle conventional delta
        # we change from 'A'/'B' to -1/1 for sell/buy
        if o[1] == -1:
            self.delta = self.delta-o[4]
        if o[1] == 1:
            self.delta = self.delta+o[4]
        if self.delta > self.delta_h:
            self.delta_h = self.delta
        if self.delta < self.delta_l:
            self.delta_l = self.delta
        # calculate up/downtick delta:
        # https://www.sierrachart.com/index.php?page=doc/StudiesReference.php&ID=323
        if self.tick_dir == 'down':
            self.tv_delta = self.tv_delta-o[4]
        elif self.tick_dir == 'up':
            self.tv_delta = self.tv_delta+o[4]
        elif self.tick_dir == 'neutral':
            self.tv_delta = self.delta
        if self.tv_delta > self.tv_delta_h:
            self.tv_delta_h = self.tv_delta
        if self.tv_delta < self.tv_delta_l:
            self.tv_delta_l = self.tv_delta
            
    def update_velocity(self,of):
        self.vel_t = of.vel_t[of.st_idx_t]
        if self.vel_t > self.vel_t_h:
            self.vel_t_h = self.vel_t
        if self.vel_t < self.vel_t_l:
            self.vel_t_l = self.vel_t
        self.vel_b = of.vel_b[of.st_idx_b]
        if self.vel_b > self.vel_b_h:
            self.vel_b_h = self.vel_b
        if self.vel_b < self.vel_b_l:
            self.vel_b_l = self.vel_b
        self.vel_s = of.vel_s[of.st_idx_s]
        if self.vel_s > self.vel_s_h:
            self.vel_s_h = self.vel_s
        if self.vel_s < self.vel_s_l:
            self.vel_s_l = self.vel_s
        self.vel_delta = of.vel_delta[of.st_idx_t]
        if self.vel_delta > self.vel_delta_h:
            self.vel_delta_h = self.vel_delta
        if self.vel_delta < self.vel_delta_l:
            self.vel_delta_l = self.vel_delta
        if self.vel_t_o == 0:
            self.vel_t_o = self.vel_t            
        if self.vel_b_o == 0:
            self.vel_b_o = self.vel_b
        if self.vel_s_o == 0:
            self.vel_s_o = self.vel_s
        if self.vel_delta_o == 0:
            self.vel_delta_o = self.vel_delta
            
class Candleseries:
    def __init__(self,ctype,interval):
        self.storage_size = 10000
        self.st_idx = 0
        self.clist = []
        self.ctype = ctype
        self.interval = interval
        self.clist.append(Candle())
        # we introduce some lists which we convert to arrays for convenient value access
        self.time = np.zeros(self.storage_size)
        self.ohlc = np.zeros((4,self.storage_size))
        self.cd_ohlc = np.zeros((4,self.storage_size))
        self.tvcd_ohlc = np.zeros((4,self.storage_size))
        self.delta_hlc = np.zeros((4,self.storage_size))
        self.tvdelta_hlc = np.zeros((4,self.storage_size))
        self.vel_t_ohlc = np.zeros((4,self.storage_size))
        self.vel_s_ohlc = np.zeros((4,self.storage_size))
        self.vel_b_ohlc = np.zeros((4,self.storage_size))
        self.vel_delta_ohlc = np.zeros((4,self.storage_size))
        self.vol = np.zeros(self.storage_size)
        self.num_trades = np.zeros(self.storage_size)
        
        # self.ohlc = 0 #pd.DataFrame()
        # self.cumdelta_ohlc = 0
        # self.tv_cumdelta_ohlc = 0
        # self.ohlc_l = []
        # self.cumdelta_ohlc_l = []
        # self.tv_cumdelta_ohlc_l = []
        
    def update_cumdelta(self):
        c = self.clist[-1]
        if len(self.clist) == 1:
            c.cum_delta_h = c.delta_h
            c.cum_delta_l = c.delta_l
            c.cum_delta_c = c.delta
        else:
            c_p = self.clist[-2]
            c.cum_delta_o = c_p.cum_delta_c
            c.cum_delta_h = c.cum_delta_o+c.delta_h
            c.cum_delta_l = c.cum_delta_o+c.delta_l
            c.cum_delta_c = c.cum_delta_o+c.delta
        if len(self.clist) == 1:
            c.cum_tv_delta_h = c.delta_h
            c.cum_tv_delta_l = c.delta_l
            c.cum_tv_delta_c = c.delta
        else:
            c_p = self.clist[-2]
            c.cum_tv_delta_o = c_p.cum_tv_delta_c
            c.cum_tv_delta_h = c.cum_tv_delta_o+c.tv_delta_h
            c.cum_tv_delta_l = c.cum_tv_delta_o+c.tv_delta_l
            c.cum_tv_delta_c = c.cum_tv_delta_o+c.tv_delta
    
    def update_storage(self):
        idx = self.st_idx
        c = self.clist[-1]
        self.time[idx] = c.time
        self.ohlc[:,idx] = c.o,c.h,c.l,c.c
        self.vol[idx] = c.vol
        self.num_trades[idx] = c.num_trades
        self.cd_ohlc[:,idx] = c.cum_delta_o,c.cum_delta_h,c.cum_delta_l,c.cum_delta_c
        self.tvcd_ohlc[:,idx] = c.cum_tv_delta_o,c.cum_tv_delta_h,c.cum_tv_delta_l,c.cum_tv_delta_c
        self.delta_hlc[:,idx] = 0,c.delta_h,c.delta_l,c.delta
        self.tvdelta_hlc[:,idx] = 0,c.tv_delta_h,c.tv_delta_l,c.tv_delta
        self.vel_t_ohlc[:,idx] = c.vel_t_o,c.vel_t_h,c.vel_t_l,c.vel_t
        self.vel_b_ohlc[:,idx] = c.vel_b_o,c.vel_b_h,c.vel_b_l,c.vel_b
        self.vel_s_ohlc[:,idx] = c.vel_s_o,c.vel_s_h,c.vel_s_l,c.vel_s
        self.vel_delta_ohlc[:,idx] = c.vel_delta_o,c.vel_delta_h,c.vel_delta_l,c.vel_delta
    
    def add_tick(self,o,of):
        c = self.clist[-1]
        # tick direction
        if c.c < o[2]:
            c.tick_dir = 'up'
        elif c.c > o[2]:
            c.tick_dir = 'down'
        if c.state == 'open':
            c.start_candle(o,self.interval)
        if c.state == 'closed':
            c_new = Candle()
            c_new.start_candle(o,self.interval)
            self.st_idx = self.st_idx+1
            self.clist.append(c_new)
            c = self.clist[-1]
        elif c.state == 'running':
            if o[2] > c.h:
                c.h = o[2]
            if o[2] < c.l:
                c.l = o[2] 
            c.c = o[2]
            c.num_trades = c.num_trades+o[3]
            c.vol = c.vol+o[4]
            if self.ctype == 'tick' and c.num_trades > self.interval:
                c.state = 'closed'
            if self.ctype == 'volume' and c.vol > self.interval:
                c.state = 'closed'
            if self.ctype == 'time' and c.time_mod < o[0]/1e3/self.interval//60:
                c.state = 'closed'
                # print (self.ctype,c.time_mod,o[0]/1e3/self.interval//60,c.state)
        self.clist[-1].update_delta(o)
        self.update_cumdelta()
        self.clist[-1].update_velocity(of)
        if c.state == 'closed':
            self.update_storage()
        return c.state