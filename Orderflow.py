# -*- coding: utf-8 -*-
"""
Created on Fri Jul 19 10:25:59 2024

@author: steff
"""
import numpy as np
from helper import *
from numba import njit
from numba import int32, float32, float64    # import the types
from numba.experimental import jitclass

spec = [
    ('storage_size', int32),        # a simple scalar field
    ('st_idx_t', int32),
    ('st_idx_b', int32),
    ('st_idx_s', int32),
    ('m_tot', float64[:,:]),          # an array field
    ('m_buys', float64[:,:]),          # an array field
    ('m_sells', float64[:,:]),          # an array field
    ('vel_t', float64[:]),          # an array field
    ('vel_b', float64[:]),          # an array field
    ('vel_s', float64[:]),          # an array field
    ('vel_delta', float64[:]),          # an array field
    ('last_vel_calc', float64),
    ('last_ask_trade', float64),
    ('last_bid_trade', float64),
    ('last_trade', float64),
    ('prev_trade', float64),
    ('prev_bid_trade', float64),
    ('prev_ask_trade', float64),
]

# @jitclass(spec)
class Orderflow:
    def __init__(self):
        self.storage_size = 10000000
        self.st_idx_t = -1
        self.st_idx_b = -1
        self.st_idx_s = -1
        # time, volume
        self.m_tot = np.zeros((2,self.storage_size))
        self.m_buys = np.zeros((2,self.storage_size))
        self.m_sells = np.zeros((2,self.storage_size))
        self.vel_t = np.zeros(self.storage_size)
        self.vel_b = np.zeros(self.storage_size)
        self.vel_s = np.zeros(self.storage_size)
        self.vel_delta = np.zeros(self.storage_size)
        self.last_vel_calc = 0
        self.last_ask_trade = 0
        self.last_bid_trade = 0
        self.last_trade = 0
        self.prev_trade = 0
        self.prev_bid_trade = 0
        self.prev_ask_trade = 0

    def add_tick(self,o):
        if o[0]-self.m_tot[0,self.st_idx_t] > 0.1:
            self.prev_trade = self.last_trade
            self.st_idx_t = self.st_idx_t+1
            self.m_tot[:,self.st_idx_t] = o[0],o[4]
            if o[1] == 1:
                self.prev_ask_trade = self.last_ask_trade
                self.st_idx_b = self.st_idx_b+1
                self.m_buys[:,self.st_idx_b] = o[0],o[4]
                self.last_ask_trade = o[2]
            if o[1] == -1:
                self.prev_bid_trade = self.last_bid_trade
                self.st_idx_s = self.st_idx_s+1
                self.m_sells[:,self.st_idx_s] = o[0],o[4]
                self.last_bid_trade = o[2]
            self.last_trade = o[2]
            return True
        else:
            return False
        
    def calc_tape_velocity(self,fil_len):
        self.last_vel_calc = self.m_tot[0,self.st_idx_t]
        # diff_t = np.diff(self.m_tot[0,:self.st_idx_t+1])
        # diff_b = np.diff(self.m_buys[0,:self.st_idx_b+1])
        # diff_s = np.diff(self.m_sells[0,:self.st_idx_s+1])
        if fil_len > self.st_idx_t:
            diff_t = np.diff(self.m_tot[0,:self.st_idx_t+1])
            self.vel_t[self.st_idx_t] = np.nanmean(diff_t[:self.st_idx_t+1])
        else:
            diff_t = np.diff(self.m_tot[0,self.st_idx_t-fil_len-1:self.st_idx_t+1])
            self.vel_t[self.st_idx_t] = np.mean(diff_t)
        if fil_len > self.st_idx_b:
            diff_b = np.diff(self.m_buys[0,:self.st_idx_b+1])
            self.vel_b[self.st_idx_b] = np.nanmean(diff_b[:self.st_idx_b+1])
        else:
            diff_b = np.diff(self.m_buys[0,self.st_idx_b-fil_len-1:self.st_idx_b+1])
            self.vel_b[self.st_idx_b] = np.mean(diff_b)
        if fil_len > self.st_idx_s:        
            diff_s= np.diff(self.m_sells[0,:self.st_idx_s+1])
            self.vel_s[self.st_idx_s] = np.nanmean(diff_s[:self.st_idx_s+1])
        else:
            diff_s = np.diff(self.m_sells[0,self.st_idx_s-fil_len-1:self.st_idx_s+1])            
            self.vel_s[self.st_idx_s] = np.mean(diff_s)
            # self.vel_t[self.st_idx_t] = np.mean(diff_t[self.st_idx_t-fil_len:self.st_idx_t+1])
            # self.vel_b[self.st_idx_b] = np.mean(diff_b[self.st_idx_b-fil_len:self.st_idx_b+1])
            # self.vel_s[self.st_idx_s] = np.mean(diff_s[self.st_idx_s-fil_len:self.st_idx_s+1])
        self.vel_delta[self.st_idx_t] = 1/self.vel_b[self.st_idx_b]-1/self.vel_s[self.st_idx_s]

@njit
def diff(a):
    return a[1:] - a[:-1]