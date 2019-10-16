# coding: utf-8

# In[7]:

# import general packages
import os
import math
import pandas as pd
import datetime as dt
import operator

from collections import OrderedDict

# import internal packages
import nipun.holdings as hld
import nipun_task.mpb.mpb_data  as mpd

import axioma
from axioma import workspace
from axioma.costmodel import CostModel, CostStructure

import axioma
from axioma.account import Account
from axioma.analytics import Analytics, AnalyticMethod
from axioma.assetset import AssetSet
from axioma.asset import Asset, AssetType
from axioma.costmodel import CostModel, CostStructure
from axioma.group import Group, Benchmark, Unit
from axioma.riskmodel import RiskModel
from axioma.strategy import Strategy, Objective, Target, Scope, ObjectiveTermType
from axioma.workspace_element import ElementType
from axioma.rebalancing import Rebalancing
from axioma.metagroup import Metagroup

print('ok')

# In[9]:

test_date = dt.datetime(2019, 10,10)
date_path = str(test_date.year) + '/' + str(test_date.month).zfill(2) + '/' + str(test_date.date()).replace('-',
                                                                                                            '') + '/'
TEST_PATH = '/home/jzhang/shared/axioma_api_wsp/' + date_path
PRODUCTION_PATH = '/opt/production/' + date_path
PRODUCTION_ws_path = TEST_PATH + 'axioma_master/production_master_main_only.wsp'
PRODUCTION_ws_path_withdate = TEST_PATH + 'axioma_master/production_master_main_only_%s.wsp' % str(
    test_date.date()).replace('-', '')
OUTPUT_ws_path_withdate = TEST_PATH + 'axioma_master'
print('ok')


# In[3]:

def clean_ws():
    workspaceNames = workspace.get_available_workspace_names()
    for workspaceX in workspaceNames:
        print('Workspace Destroyed: ' + workspaceX)
        ws_name = workspace.get(workspaceX)
        ws_name.destroy()


def del_cost_model(wsp, cost_model_name):
    cm = wsp.get_cost_model(cost_model_name)
    cm.destroy()


def get_adj_sell_costs(last_brkpoint, new_brkpoint, rate, sell_costs):
    # print last_brkpoint
    breakpoint_slopes = []
    sorted_int_sell_cost_keys = sorted([int(k) for k in sell_costs.keys()])
    loop_flag = True
    while loop_flag:
        itg_cost = 0
        for k in sorted_int_sell_cost_keys:
            if k > last_brkpoint:
                break
            else:
                itg_cost = sell_costs[str(k)]

        if last_brkpoint > sorted_int_sell_cost_keys[-1]:
            brkpoint = last_brkpoint
        else:
            brkpoint = min(float(k), last_brkpoint)
        breakpoint_slopes.append((brkpoint, rate + itg_cost))
        if k > new_brkpoint or (last_brkpoint >= sorted_int_sell_cost_keys[-1]):
            loop_flag = False
        else:
            last_brkpoint = k
    return breakpoint_slopes


def get_adj_buy_costs(last_brkpoint, new_brkpoint, charge_rate, buy_costs):
    breakpoint_slopes = []
    sorted_int_buy_cost_keys = sorted([int(float(k)) for k in buy_costs.keys()])
    loop_flag = True
    while loop_flag:
        itg_cost = 0
        for k in sorted_int_buy_cost_keys:
            if k > last_brkpoint:
                break
            else:
                itg_cost = buy_costs[str(k)]
        if last_brkpoint > sorted_int_buy_cost_keys[-1]:
            brkpoint = last_brkpoint
        else:
            brkpoint = min(float(k), last_brkpoint)
        breakpoint_slopes.append((brkpoint, itg_cost - charge_rate))
        if k > new_brkpoint or (last_brkpoint >= sorted_int_buy_cost_keys[-1]):
            loop_flag = False
        else:
            last_brkpoint = k
    return breakpoint_slopes


def adj_buy_cost(cost_structure, buy_costs, charges, asset, pbs, total_hld):
    asset_charge = charges[charges.index == asset].reset_index()
    charge_dict = {pb: asset_charge['%s_charged' % pb][0] for pb in pbs if
                   not math.isnan(asset_charge['%s_charged' % pb][0])}
    sorted_charges = sorted(charge_dict.items(), key=operator.itemgetter(1), reverse=True)
    # print sorted_charges
    asset_hld = total_hld[total_hld['barrid'] == asset].reset_index()
    asset_price = prices.get(asset)
    holdings_dict = {pb: abs(asset_hld['%s_shares' % pb][0] * asset_price) for pb in pbs}
    # print holdings_dict

    buy_costs = {str(int((float(k)))): buy_costs[k] for k in sorted(buy_costs.keys())}
    all_buy_slopes = []
    last_brkpoint = 0
    for brk, charge_rate in sorted_charges:
        hld = holdings_dict.get(brk, 0)
        if hld == 0:
            continue
        new_brkpoint = hld + last_brkpoint
        if new_brkpoint == last_brkpoint:
            break
        break_slopes = get_adj_buy_costs(last_brkpoint, new_brkpoint, charge_rate, buy_costs)
        all_buy_slopes.append(break_slopes)
        last_brkpoint = new_brkpoint
    # print all_buy_slopes
    # is first buy slope negative?
    linear_buy = 0
    adjusted_slopes = []
    if all_buy_slopes[0][0][1] < 0:
        linear_buy = all_buy_slopes[0][0][1]
        for all_slps in all_buy_slopes:
            for slp in all_slps:
                adjusted_slopes.append((slp[0], slp[1] + (-1 * linear_buy)))
    else:
        for all_slps in all_buy_slopes:
            for slp in all_slps:
                adjusted_slopes.append(slp)
    # print adjusted_slopes
    for slp in adjusted_slopes:
        # print slp
        cost_structure.add_buy_slope(slp[0], slp[1])
    return linear_buy


def adj_short_cost(cost_structure, sell_costs, avail, asset, pbs):
    asset_avail = avail[avail['barrid'] == asset][rate_cols + avail_cols + dollavail_cols].reset_index()
    # print asset_avail
    rate_dict = {pb: asset_avail['%s_rate' % pb][0] for pb in pbs if not math.isnan(asset_avail['%s_rate' % pb][0])}
    # print rate_dict
    sorted_rates = sorted(rate_dict.items(), key=operator.itemgetter(1))
    # print sorted_rates
    davail_dict = {pb: asset_avail['%s_dollavail' % pb][0] for pb in pbs}
    # print davail_dict
    # deal with abnormal data: availble shares=0, short rate<>0
    for pb in pbs:
        if davail_dict.get(pb) == 0:
            if pb in rate_dict.keys():
                del rate_dict[pb]
    sorted_rates = sorted(rate_dict.items(), key=operator.itemgetter(1))
    # print sorted_rates
    sell_costs = {str(int((float(k)))): sell_costs[k] for k in sorted(sell_costs.keys())}
    # print sell_costs
    last_brkpoint = 0
    for brk, rate in sorted_rates:
        # print brk
        # print rate
        if math.isnan(rate) or rate == 0:
            continue
        new_brkpoint = davail_dict.get(brk) + last_brkpoint
        break_slopes = get_adj_sell_costs(last_brkpoint, new_brkpoint, rate, sell_costs)
        # print break_slopes
        for bk_slp in break_slopes:
            # print bk_slp
            # print bk_slp[0], bk_slp[1]
            cost_structure.add_sell_slope(bk_slp[0], bk_slp[1])
        last_brkpoint = new_brkpoint

    return cost_structure


# # 1. Load Workspace

# In[4]:

clean_ws()
ws_name = 'production_master_main_only_' + str(test_date.date()).replace('-', '')
server_ws_name = 'production_master_main_only_COSTTEST_' + str(test_date.date()).replace('-', '')

ws_path = TEST_PATH + 'axioma_master/%s.wsp' % ws_name

# prepare_workspace(ws_path,ws_path_linux)
file_path = 'file:' + TEST_PATH + 'axioma_master/%s.wsp' % ws_name
ws = workspace.load_from_file(server_ws_name, file_path)

# In[5]:

workspaceNames = workspace.get_available_workspace_names()
print workspaceNames

# In[6]:

# get initial strategy name
ini_strategy_name = ws.get_strategy_names()
print 'current strategy name'
print ini_strategy_name
# load initial stratey
ini_strategy = ws.get_strategy(ini_strategy_name[0])
# get initial objectives
ini_objective = ini_strategy.get_objectives()[0]
# print initial objective terms
print "initial objective terms"
print ini_objective.get_terms()
# drop borrow cost from objective
drop_objective_terms = ini_strategy.get_objective_term('BORROWCOST')
drop_objective_terms.destroy()
print "current objective terms"
print ini_objective.get_terms()

# # 2. Get new cost model
# ### update itg_cost model with available files, change transactioncost in objective function with new nipun cost model
# ### add linear buy cost into objective function to adjust buy cover cost back to negative

# In[7]:

# get available files
d = test_date
pbs = ['baml', 'gs', 'ubs', 'msdw']
avail_cols = [pb + '_avail' for pb in pbs]
dollavail_cols = [pb + '_dollavail' for pb in pbs]
rate_cols = [pb + '_rate' for pb in pbs]
prices = mpd.get_meta(d, as_dict=True)['usd_price']
price_df = pd.DataFrame(prices.items(), columns=['barrid', 'usd_price'])
# currently, the code is getting availability from database
avail = mpd.load_raw_availability(pbs, prices, mode='normal', min_dollar_avail=0, pdate=d)
avail = pd.merge(avail, price_df, left_index=True, right_on='barrid', how='left')
avail['total_avail'] = avail[avail_cols].sum(axis=1)
for pb in pbs:
    avail['%s_dollavail' % pb] = avail['%s_avail' % pb] * avail['usd_price']
# charged rates
charges = mpd.load_charges(d, 1000).abs()
# adjust by scaling
# if the user would like to have different weights on BorrowCost, Please set OBJECTIVE_ADJUSTMENT
scaling_factor = float(1.0 / 12.0)
TACF_ADJUSTMENT = 2.0
for pb in pbs:
    avail[pb + '_rate'] = avail[pb + '_rate'] * scaling_factor * TACF_ADJUSTMENT
    charges[pb + '_charged'] = charges[pb + '_charged'] * scaling_factor * TACF_ADJUSTMENT

# In[8]:

# get initial cost model
itg = ws.get_cost_model('ITGACE')
all_itg_assets = itg.get_assets()
# get current holdings
'''
as our holding table is updated several times every day. If we would like to have the initial holdings on historical date
we need to load the holding from 1-day before
'''
total_hld = hld.get_strategy_holdings(date=(d - dt.timedelta(days=1)), strategy_id=1000)
curr_hld = total_hld[['barrid', 'shares_held_total']].set_index('barrid')
#curr_hld = {k: v['shares_held_total'] for (k, v) in curr_hld.to_dict(orient='index').items()}
# curr_hld = {k: v['shares_held_total'] for (k, v) in curr_hld.to_dict().items()}
curr_hld = curr_hld.to_dict()

# In[9]:

# create new cost model
# del_cost_model(ws, 'nipun_tcost1')
new_cost_model = CostModel(ws, 'nipun_tcost1')
all_itg_assets = itg.get_assets()
# all_itg_assets=['MALBRF1']
# for each asset
linear_buys = {}
i_count = 0.00
for asset in all_itg_assets:
    print asset
    if i_count % 100 == 0:
        print i_count, '/', len(all_itg_assets), 'completed'
    buy_costs = itg.get_buy_slopes_for_asset(asset)
    sell_costs = itg.get_sell_slopes_for_asset(asset)
    holding_init = curr_hld.get(asset, 0)
    linear_buy = 0

    # print asset, holding_init
    # create cost structure
    cost_structure = CostStructure(new_cost_model, '%s_cost_struct1' % asset)
    cost_structure.include_asset(asset)

    # initial long - no need to modify
    if holding_init > 0:
        for k in sorted(buy_costs.keys()):
            cost_structure.add_buy_slope(float(k), buy_costs[k])
        for k in sorted(sell_costs.keys()):
            cost_structure.add_sell_slope(float(k), sell_costs[k])

    # no position, modify short curve
    if holding_init == 0:
        if len(avail[avail.barrid == asset]) > 0 and avail[avail.barrid == asset]['total_avail'].iloc[0] > 0:

            adj_short_cost(cost_structure, sell_costs, avail, asset, pbs)
        else:
            # no availability,just use regular tcost curve
            for k in sorted(sell_costs.keys()):
                cost_structure.add_sell_slope(float(k), sell_costs[k])

        # buys are the same
        for k in sorted(buy_costs.keys()):
            cost_structure.add_buy_slope(float(k), buy_costs[k])

    # short, modify both short and cover costs
    if holding_init < 0:
        # modify shorts
        if len(avail[avail.barrid == asset]) > 0 and avail[avail.barrid == asset]['total_avail'].iloc[0] > 0:
            adj_short_cost(cost_structure, sell_costs, avail, asset, pbs)
        else:
            # no availability,just use regular tcost curve
            for k in sorted(sell_costs.keys()):
                cost_structure.add_sell_slope(float(k), sell_costs[k])
        # modify buy costs
        # notes
        '''
        if the charged rate is missing,there will be an error message. The user could ask production support team to check
        the charge rate table
        '''
        try:
            linear_buy = adj_buy_cost(cost_structure, buy_costs, charges, asset, pbs, total_hld)
        except:
            print "Check %s Charge Rate Table" % asset
            print "Current Manual Assign -1 to this value"
            linear_buy = -1

    linear_buys[asset] = linear_buy

    i_count = i_count + 1

# In[10]:

print 'current risk model names in workspace'
print ws.get_cost_model_names()

# In[11]:

# set default transactioncost
ws.set_defaults(cost_model='nipun_tcost1')

# In[12]:

# add linear buy adjustment slope into Axioma
# built buy cost linear adjustment group
buy_cost_linear_adj_group = Group(ws, 'BUY_Cost_Linear_Adj_group', values=linear_buys, unit=Unit.Currency)
# add this adjustment group to current strategy objective terms
buy_cost_linear_adj_Term = axioma.strategy.create_linear_buy_term(ini_strategy, 'BUY_Cost_Linear_Adj',
                                                                  group=buy_cost_linear_adj_group)

# In[13]:

# get objective terms from current strategy
# 'expectedreturn'
Objective_expectedreturn = ini_strategy.get_objective_term('expectedreturn')
# 'transactioncost'
Objective_transactioncost = ini_strategy.get_objective_term('transactioncost')
# 'Variance'
Objective_Variance = ini_strategy.get_objective_term('Variance')
# 'trade_timing_buy_cost'
Objective_trade_timing_buy_cost = ini_strategy.get_objective_term('trade_timing_buy_cost')
# 'trade_timing_sell_cost'
Objective_trade_timing_sell_cost = ini_strategy.get_objective_term('trade_timing_sell_cost')
# 'BUY_Cost_Linear_Adj'
Objective_BUY_Cost_Linear_Adj = ini_strategy.get_objective_term('BUY_Cost_Linear_Adj')
# bulit an empty OrderedDict and add the objective terms to the OrderedDict
terms = OrderedDict()
terms[Objective_expectedreturn] = 1
terms[Objective_transactioncost] = -0.5
terms[Objective_Variance] = -3
terms[Objective_trade_timing_buy_cost] = -0.5
terms[Objective_trade_timing_sell_cost] = -0.5
terms[buy_cost_linear_adj_Term] = -0.5

# In[14]:

# destroy current objective function,'MVO_Production'.Rebuild another Objective Function with the defined Ordereddict
'''
Currently, in Axioma Python API, we cannot edit the existing Objective Function directly. The only way to add/delete/edit objective
terms and Objective terms weights is constructing a new one
Axioma Help Desk said they may have this function available in the future versions
'''
ini_strategy.get_objective('MVO_Production').destroy()
Objective(ini_strategy, 'MVO_Production', Target.Maximize, terms=terms, active=True)

# In[15]:

# re-assign the objective function to ini_objective
ini_objective = ini_strategy.get_objective('MVO_Production')
print "current objective terms"
print ini_objective.get_terms()

# In[16]:

ws.set_defaults(cost_model='nipun_tcost1')

# In[17]:

# write the update workspace back to linux server machine
ws.write(url="file:" + OUTPUT_ws_path_withdate)
# change the folder permission
cmd = 'chmod -R 777 ' + TEST_PATH
os.system(cmd)

# In[18]:

clean_ws()

# In[ ]:



