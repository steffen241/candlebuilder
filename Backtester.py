# -*- coding: utf-8 -*-
"""
Created on Wed Jul 31 13:25:00 2024

@author: steff
"""

class Trade():
    def __init__(self,o,order_type,size,side,entry_limit_price,sl_price):
        self.open = True
        self.entry_filled = False
        self.entry = 0
        self.exit = 0
        self.sl_order_id = 0
        self.entry_order_id = 0
        self.track_pnl = [(0,0)]
        self.side = side
        self.execution_note = 0 # could be anything, I use it now to just track the number the trade was entered
        if order_type == 'market':
            o.create_market_order(size,side)
        if order_type == 'limit':
            o.create_limit_order(entry_limit_price,size,side)
        self.entry_order_id = o.active_orders[-1][0]
        if sl_price > 0:
            if side == 'buy':
                o.create_stop_order(sl_price,size,'sell')
            if side == 'sell':
                o.create_stop_order(sl_price,size,'buy')
            self.sl_order_id = o.active_orders[-1][0]

class Orders():
    def __init__(self,symbol):
        self.symbol = symbol
        self.active_orders = []
        self.filled_orders = []
        self.order_id = 1000
        
    def create_market_order(self,qty,side):
        self.order_id = self.order_id+1
        if side == 'buy':
            self.active_orders.append((self.order_id,'market','buy',qty))
        if side == 'sell':
            self.active_orders.append((self.order_id,'market','sell',qty))

    def create_limit_order(self,price,qty,side):
        self.order_id = self.order_id+1
        if side == 'buy':
            self.active_orders.append((self.order_id,'limit','buy',price,qty))
        if side == 'sell':
            self.active_orders.append((self.order_id,'limit','sell',price,qty))

    def create_stop_order(self,price,qty,side):
        self.order_id = self.order_id+1
        if side == 'buy':
            self.active_orders.append((self.order_id,'stop','buy',price,qty))
        if side == 'sell':
            self.active_orders.append((self.order_id,'stop','sell',price,qty))

    def cancel_order(self,o_id):
        for i in range(0,len(self.active_orders)):
            if o_id == self.active_orders[i][0]:
                self.active_orders.remove(self.active_orders[i])
                break

class Backtestengine():
    def __init__(self):
        self.t_list = []
        self.order_id = 20000
    
    def fill_order(self,o,o_id,tick):
        for i in range(0,len(o.active_orders)):
            if o_id == o.active_orders[i][0]:
                self.order_id = self.order_id+1
                if o.active_orders[i][1] == 'limit':
                    f_order = (o.active_orders[i][0],'limit',o.active_orders[i][2],o.active_orders[i][3],o.active_orders[i][4],tick[0])
                if o.active_orders[i][1] == 'market':
                    f_order = (o.active_orders[i][0],'market',o.active_orders[i][2],tick[2],o.active_orders[i][3],tick[0])
                if o.active_orders[i][1] == 'stop':
                    f_order = (o.active_orders[i][0],'stop',o.active_orders[i][2],tick[2],o.active_orders[i][3],tick[0])
                o.active_orders.remove(o.active_orders[i])
                o.filled_orders.append(f_order)
                break
            
    def ontick(self,of,o,tick):
        # update trade pnl before we do something else
        for t in self.t_list:
            if t.open == True and t.entry_filled:
                if t.side == 'buy':
                    if t.track_pnl[-1][1] != of.last_trade-t.entry: #if (of.prev_bid_trade != of.last_bid_trade):# or (of.prev_ask_trade != of.last_ask_trade): #of.prev_trade > of.last_bid_trade):
                        t.track_pnl.append((tick[0],of.last_trade-t.entry))
                if t.side == 'sell':
                    if t.track_pnl[-1][1] != t.entry-of.last_trade: #if (of.prev_bid_trade != of.last_bid_trade):# or (of.prev_ask_trade != of.last_ask_trade): #of.prev_trade > of.last_bid_trade):
                        t.track_pnl.append((tick[0],t.entry-of.last_trade))

        # let's fill open orders
        l_ao = len(o.active_orders)
        event = [0,0]
        if len(o.active_orders) > 0:
            for i in range(0,l_ao):
                # check if we need to trigger an order
                # market orders will trigger with the next trade in order direction
                # limits will get filled with first tick: room for improvement
                # we don't care about fill size right now
                if o.active_orders[i][1] == 'limit' and o.active_orders[i][2] == 'buy' and \
                    o.active_orders[i][3] >= tick[2] and tick[1] == 'A':
                        event = [1,o.active_orders[i][0]]
                        self.fill_order(o, o.active_orders[i][0], tick)
                        break
                if o.active_orders[i][1] == 'limit' and o.active_orders[i][2] == 'sell' and \
                    o.active_orders[i][3] <= tick[2] and tick[1] == 'B':
                        event = [1,o.active_orders[i][0]]
                        self.fill_order(o, o.active_orders[i][0], tick)
                        break
                if o.active_orders[i][1] == 'market' and o.active_orders[i][2] == 'buy' and  tick[1] == 'B':
                        event = [1,o.active_orders[i][0]]
                        self.fill_order(o, o.active_orders[i][0], tick)
                        break
                if o.active_orders[i][1] == 'market' and o.active_orders[i][2] == 'sell' and  tick[1] == 'A':
                        event = [1,o.active_orders[i][0]]
                        self.fill_order(o, o.active_orders[i][0], tick)
                        break
                if o.active_orders[i][1] == 'stop' and o.active_orders[i][2] == 'buy' and  tick[1] == 'B' and \
                    o.active_orders[i][3] <= tick[2]:
                        event = [1,o.active_orders[i][0]]
                        self.fill_order(o, o.active_orders[i][0], tick)    
                        break
                if o.active_orders[i][1] == 'stop' and o.active_orders[i][2] == 'sell' and  tick[1] == 'A' and \
                    o.active_orders[i][3] >= tick[2]:
                        event = [1,o.active_orders[i][0]]
                        self.fill_order(o, o.active_orders[i][0], tick)
                        break                
    #    def emulate_paper_traders(self,trade_list):
        # order filled/closed with sl
        if event[0] == 1:
            first_trade = 0
            for t in self.t_list:
                if t.open == True and t.side != o.filled_orders[-1][2]:
                    first_trade = t
                    t.open = False
                    t.exit = o.filled_orders[-1][3]
                    o.cancel_order(t.sl_order_id)
                    break
            for t in self.t_list:
                if t.open == True and t.entry_order_id == event[1]:
                    t.entry_filled = True
                    t.entry = o.filled_orders[-1][3]
                    break
                if t.open == True and t.sl_order_id == event[1]:
                    t.open = False
                    t.exit = o.filled_orders[-1][3]            
        return event
    
    def print_trades(self):
        for t in self.t_list:
            print ('Candle#', t.execution_note, 'Entry', t.entry, 'Exit', t.exit, 'PnL' ,t.track_pnl[-1][1])
            
    def trade_analytics(self):
        for t in self.t_list:
            a_pnl = np.array(t.track_pnl)
            print (np.min(a_pnl[:,0]))