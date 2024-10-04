# -*- coding: utf-8 -*-
"""
Created on Fri Jul 19 10:26:45 2024

@author: steff
"""
import numpy as np
import talib as ta
# from numba import njit

# implement three-candle defense based on candle close
def cdl_defense(ohlc):
    cr_long = False
    if len(ohlc) >= 4:
        l = ohlc[-4,3]
        cr_long = ohlc[-3,2] <= l and ohlc[-3,3] >= l and ohlc[-2,2] <= l and ohlc[-2,3] >= l and ohlc[-1,2] <= l and ohlc[-1,3] >= l
    if cr_long == True:
        return 1
    else:
        return 0

# @njit
def rolling_percentile(a,w_len,perc):
    p = np.percentile(rolling_window(a,w_len),perc)
    return p

# @njit
def rolling_window(a,w_len):
    st_idx = len(a)
    if st_idx > w_len:
        return a[st_idx-w_len:st_idx+1]
    else:
        return a[:st_idx+1]

def vel_exh_acc(vel,p3):
    exh = 0
    if len(vel[0,:])>2:
        candle_1_direction = (vel[0,-2]-vel[3,-2])<0
        candle_2_direction = (vel[0,-1]-vel[3,-1])<0
        if vel[1,-2]>p3 and candle_1_direction==True:
            if candle_1_direction == True and candle_2_direction == False:
                exh = 1
    return exh

def cumdelta_filter(cd,cd_m,cd_p10,cd_p90):
    fil = 0
    a = cd_m+(cd_p90-cd_m)/3
    # cd_o = cd[0]
    # cd_c = cd[3]
    if cd[3]>cd_m and cd[3]<cd_p90 and cd[3]>cd[0]:
        fil = 1
    elif cd[3]>cd_p90 and cd[3]<cd[0]:
        fil = 2
    if cd[3]<cd_m and cd[3]>cd_p10:
        fil = -1
    elif cd[2]<cd_p10 and cd[3]>cd[0]:
        fil = -2
    return fil
        
def candle_positive(ohlc):
    res = False
    if ohlc[3]>ohlc[0]:
        res = True
    return res

def numpy_ewma_vectorized_v2(data, window):
    alpha = 2 /(window + 1.0)
    alpha_rev = 1-alpha
    n = data.shape[0]

    pows = alpha_rev**(np.arange(n+1))

    scale_arr = 1/pows[:-1]
    offset = data[0]*pows[1:]
    pw0 = alpha*alpha_rev**(n-1)

    mult = data*pw0*scale_arr
    cumsums = mult.cumsum()
    out = offset + cumsums*scale_arr[::-1]
    return out

def optimizer_permutations_4(a,b,c,d):
    perm = []
    for i in a:
        for j in b:
            for k in c:
                for l in d:
                    perm.append((i,j,k,l))
    return perm

def get_basic_bands(med_price, atr, multiplier):
    matr = multiplier * atr
    upper = med_price + matr
    lower = med_price - matr
    return upper, lower

def get_final_bands_nb(close, upper, lower):
    trend = np.full(close.shape, np.nan)
    dir_ = np.full(close.shape, 1)
    long = np.full(close.shape, np.nan)
    short = np.full(close.shape, np.nan)

    for i in range(1, close.shape[0]):
        if close[i] > upper[i - 1]:
            dir_[i] = 1
        elif close[i] < lower[i - 1]:
            dir_[i] = -1
        else:
            dir_[i] = dir_[i - 1]
            if dir_[i] > 0 and lower[i] < lower[i - 1]:
                lower[i] = lower[i - 1]
            if dir_[i] < 0 and upper[i] > upper[i - 1]:
                upper[i] = upper[i - 1]

        if dir_[i] > 0:
            trend[i] = long[i] = lower[i]
        else:
            trend[i] = short[i] = upper[i]

    return trend, dir_, long, short

def faster_supertrend_talib(high, low, close, period=7, multiplier=3):
    avg_price = ta.MEDPRICE(high, low)
    atr = ta.ATR(high, low, close, period)
    upper, lower = get_basic_bands(avg_price, atr, multiplier)
    return get_final_bands_nb(close, upper, lower)

class Signal:
    def __init__(self):
        self.storage_size = 10000000
        self.st_idx = -1
        self.v = np.zeros(self.storage_size)
        
    def add_val(self,v):
        self.st_idx = self.st_idx+1
        self.v[self.st_idx] = v
        
    def get(self):
        return self.v[:self.st_idx+1]