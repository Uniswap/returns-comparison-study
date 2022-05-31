import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import requests
import time
import math

from tqdm import tqdm
import sqlite3
import statsmodels.api as sm
import pickle as pkl
from scipy.stats.mstats import winsorize

def tgt_pools(n):
    ### pull only the top n pools
    q = """
        query MyQuery {
        pools(orderBy: volumeUSD, orderDirection: desc, first: %s) {
            id
        }
        }
    """ % (n)
    prod_url = "uniswap/uniswap-v3"
    alt_url = "ianlapham/v3-minimal/graphql"

    request = requests.post(
        f"https://api.thegraph.com/subgraphs/name/{prod_url}",
        json={"query": q},
    )

    v3_top_pools = request.json()
    v3_top_pools = v3_top_pools["data"]["pools"]
    v3_pools = [entry["id"] for entry in v3_top_pools]
    return v3_pools

def paper_metrics(uni):
    largest_v3 = uni[uni['largestV3'] == 1].copy()
    stable_pools = largest_v3.loc[largest_v3['stable_stable'].notna(), 'v3pool'].unique()

    print(largest_v3['v3pool'].unique().shape)

    bps100 = largest_v3[largest_v3['fee'] == 10000].copy()
    bps30 = largest_v3[largest_v3['fee'] == 3000].copy()
    stable = largest_v3[largest_v3.index.isin(stable_pools)]

    print(f'V2: {largest_v3["v2Ret"].mean() * 10000} - {np.percentile(largest_v3["v2Ret"], [25, 50, 75]) * 10000}')
    print(f'V3: {largest_v3["stableV3"].mean() * 10000} - {np.percentile(largest_v3["stableV3"], [25, 50, 75]) * 10000}')
    print(f'V3 100: {bps100["stableV3"].mean() * 10000} - {np.percentile(bps100["stableV3"], [25, 50, 75]) * 10000}')
    print(f'V3 30: {bps30["stableV3"].mean() * 10000} - {np.percentile(bps30["stableV3"], [25, 50, 75]) * 10000}')

    winsor_v3_1 = winsorize(largest_v3['stableV3'], limits=[0.01, 0.01]).mean() * 10000
    winsor_v3_5 = winsorize(largest_v3['stableV3'], limits=[0.05, 0.05]).mean() * 10000

    winsor_v2_1 = winsorize(largest_v3['stableV3'], limits=[0.01, 0.01]).mean() * 10000
    winsor_v2_5 = winsorize(largest_v3['stableV3'], limits=[0.05, 0.05]).mean() * 10000

    winsor_bps100_1 = winsorize(bps100['stableV3'], limits=[0.01, 0.01]).mean() * 10000
    winsor_bps100_5 = winsorize(bps100['stableV3'], limits=[0.05, 0.05]).mean() * 10000

    print("\n----------\n")

    print(f'Windsorization v3: 1% {winsor_v3_1:.1f} - 5% {winsor_v3_5:.1f}')
    print(f'Windsorization v2: 1% {winsor_v2_1:.1f} - 5% {winsor_v2_5:.1f}')
    print(f'Windsorization v3 100 bps: 1% {winsor_bps100_1:.1f} - 5% {winsor_bps100_5:.1f}')

    grouped_returns = (
                        largest_v3[['v3pool', 'v2Ret', 'v3Ret', 'stableV3', 'tvlUSD_v3']].dropna()
                        .groupby("v3pool")
                        .mean()
                        ).copy()

    grouped_returns = (
                        grouped_returns
                        .join(
                            largest_v3[['v3pool', 'token0_sym', 'token1_sym', 'fee', 'token0', 'token1', 'top_Npools']]
                            .set_index("v3pool")
                            )
                        .drop_duplicates()
                        )

    grouped_returns['label'] = grouped_returns['token0_sym'] + "-" + grouped_returns['token1_sym']
    grouped_returns['label_default'] = grouped_returns['token0_sym'] + "-" + grouped_returns['token1_sym']
    grouped_returns.loc[~((grouped_returns['token0'].isin(token_list)) & 
                        (grouped_returns['token1'].isin(token_list))), 'label'] = ''

    labeled_values = grouped_returns.loc[grouped_returns['top_Npools'] == 1, 'label']
    print(f"Labeled pools: {labeled_values[labeled_values != ''].shape}")

    grouped_returns['v3/v2_ret'] = (grouped_returns['stableV3'] - grouped_returns['v2Ret'])

    grouped_returns.to_csv("grouped_returns.csv")


    bps1 = grouped_returns[grouped_returns['fee'] == 100]
    bps5 = grouped_returns[grouped_returns['fee'] == 500]
    bps30 = grouped_returns[grouped_returns['fee'] == 3000]
    bps100 = grouped_returns[grouped_returns['fee'] == 10000]
    stable = grouped_returns[grouped_returns.index.isin(stable_pools)]

    bps1_diff = np.percentile(bps1['v3/v2_ret'], [25, 50, 75]) * 10000
    bps5_diff = np.percentile(bps5['v3/v2_ret'], [25, 50, 75]) * 10000
    bps30_diff = np.percentile(bps30['v3/v2_ret'], [25, 50, 75]) * 10000
    bps100_diff = np.percentile(bps100['v3/v2_ret'], [25, 50, 75]) * 10000
    stable_diff = np.percentile(stable['v3/v2_ret'], [25, 50, 75]) * 10000
    full_diff = np.percentile(grouped_returns['v3/v2_ret'], [25, 50, 75]) * 10000

    print("\n----------\n")
    print(f"Full diff: {full_diff}")
    print(f"1 bps diff: {bps1_diff}")
    print(f"5 bps diff: {bps5_diff}")
    print(f"30 bps diff: {bps30_diff}")
    print(f"100 bps diff: {bps100_diff}")
    print(f"Stable diff: {stable_diff}")

    ######
    grouped_returns = (
                        largest_v3[['v3pool', 'v2Ret', 'stableV3', 'v3_md', 'v2_md']].dropna()
                        .groupby("v3pool")
                        .mean()
                        ).copy()

    grouped_returns = (
                        grouped_returns
                        .join(
                            largest_v3[['v3pool', 'token0_sym', 'token1_sym', 'fee', 'token0', 'token1', 'top_Npools']]
                            .set_index("v3pool")
                            )
                        .drop_duplicates()
                        )

    grouped_returns['label'] = grouped_returns['token0_sym'] + "-" + grouped_returns['token1_sym']
    grouped_returns.loc[~((grouped_returns['token0'].isin(token_list)) & 
                        (grouped_returns['token1'].isin(token_list))), 'label'] = ''

    grouped_returns['v3/v2_md'] = (grouped_returns['v3_md'] / grouped_returns['v2_md'])
    grouped_returns.to_csv("marketdepth_v2v3.csv")
    md_pairwise = np.percentile(grouped_returns['v3/v2_md'], [25, 50, 75])

    print("\n----------\n")
    print(f"MD: {md_pairwise}")

# pull in the relevant data sources
# sometimes i place data in other directories instead of the current one, so
# i assign path variables to that other directory
# pull in the relevant data sources
path = os.getcwd()
conn = sqlite3.connect(f"{path}/sql/returns.db")
cursor = conn.cursor()

# variables to look at
tvl_limit = 1000
drop_pools = False
zero_rets = True
repull_pairs = False
paper_print = True

n = 500

# Read the marketdepth, blocks to datetime, and returns
md = pd.read_csv(f'{path}/data/marketdepth.csv')
md['date'] = pd.to_datetime(md['date']).dt.tz_localize(None)

blocks = pd.read_csv(f"{path}/data/block_to_dt.csv")
blocks["dt"] = blocks['dt'].apply(lambda x: pd.to_datetime(x).tz_localize(None))
v3 = pd.read_sql("SELECT * from v3_ret", conn).rename(columns = {"feeRet": "v3Ret", 
                                                                 "tvlUSD": 'tvlUSD_v3'}).drop_duplicates()
v2 = pd.read_sql("SELECT * from v2_ret", conn).rename(columns = {"feeRet": "v2Ret",
                                                                 "tvlUSD": 'tvlUSD_v2'}).drop_duplicates()
# v3 factory for token0 token1 and fee tier
factory_v3 = pd.read_csv(f"{path}/data/factory_v3.csv")
factory_v2 = pd.read_csv(f"{path}/data/factory_v2.csv")

# erc20 for token names
erc20 = pd.read_csv(f"{path}/data/erc20.csv")
erc20['contract_address'] = erc20['contract_address'].apply(lambda x: str(x).replace('\\', '0'))

## v2 md calculations
v2_raw = pd.read_sql("SELECT * from v2", conn)
v2_raw['dt'] = pd.to_datetime(v2_raw['dt'])

v2_md = v2_raw[['dt', 'pool']]

print("Done reading data")

v3_pools = tgt_pools(n)

# pull token_list 
req = requests.get("https://tokens.uniswap.org/")
tokens = req.json()
token_list = [row['address'].lower() for row in tokens['tokens']]

## join the v2 and v3 data
# if drop pools -> drop all pools that even touch the tvl limit
# if zero_rets -> zero out returns if the tvl is below the limit
if drop_pools:
    dropped_v3 = len(v3[v3['tvlUSD_v3'].astype(float) < tvl_limit]['v3pool'].unique())
    
    # set arithmetic because its efficient and nice
    v3_pools = set(v3['v3pool']) - set(v3[v3['tvlUSD_v3'].astype(float) < tvl_limit]['v3pool'].unique())
    v3_pools = [*v3_pools]

    dropped_v2 = len(v2[v2['tvlUSD_v2'].astype(float) < tvl_limit]['v2pool'].unique())
    v2_pools = set(v2['v2pool']) - set(v2[v2['tvlUSD_v2'].astype(float) < tvl_limit]['v2pool'].unique())
    v2_pools = [*v2_pools]
    print(f'Dropped V3 Pools: {dropped_v3} and Dropped V2 Pools: {dropped_v2}')

    v2 = v2[v2['v2pool'].isin(v2_pools)].copy()
    v3 = v3[v3['v3pool'].isin(v3_pools)].copy()
elif zero_rets:
    print(f"Zero'd out V3 Pools {v3.loc[v3['tvlUSD_v3'].astype(float) < tvl_limit].count().iloc[0] / v3.count().iloc[0]}")
    print(f"Zero'd out V2 Pools {v2.loc[v2['tvlUSD_v2'].astype(float) < tvl_limit].count().iloc[0] / v2.count().iloc[0]}")
    
    v3.loc[v3['tvlUSD_v3'].astype(float) < tvl_limit, 'v3Ret'] = 0
    v2.loc[v2['tvlUSD_v2'].astype(float) < tvl_limit, 'v2Ret'] = 0
else:
    raise ValueError

# broken pools - something happens with the rebase of these pools
# some also dont trade anymore so its like the token got stopped

v3 = v3[~v3['v3pool'].isin(['0x7e3a3a525d9d265d11d1d1db3cad678746b47d09', '0xad6d2f2cb7bf2c55c7493fd650d3a66a4c72c483',
                         '0x6a8c06aeef13aab2cdd51d41e41641630c41f5ff', '0x0fddb7063f2db3c6b5b00b33758cdbd51ed2cc6f',
                           '0x1becf1ac50f31c3441181563f9d350ddf72a2bfa'])].copy()

# merge v3 and v2
# we want this to be an inner join
# we want all the others to be an outer join
uni = v2.merge(v3, left_on = ['block_t0', 'block_t1', 'v3pool'], right_on = ['block_t0', 'block_t1', 'v3pool']).copy()

uni = uni[~uni['v2pool_x'].isna()].copy()

assert uni[(uni['v2pool_x'] != uni['v2pool_y'])].empty, "Different v2 references"

uni['block_t0'] = uni['block_t0'].astype(float)
uni['block_t1'] = uni['block_t1'].astype(float)

uni['spr_v2-v3'] = uni['v2Ret'] - uni['v3Ret']
uni['v2Ret'] = uni['v2Ret'].apply(lambda x: 0 if math.isclose(x, 0, abs_tol = 1e-7) else x)

uni['tvlSpr'] = uni['tvlUSD_v2'].astype(float) / uni['tvlUSD_v3'].astype(float)
uni = uni[uni['v3pool'].isin(v3_pools)].copy()

uni = uni.set_index("block_t0")
uni.index = uni.index.astype(int)
uni = uni.join(blocks.set_index("block"))
uni = uni.reset_index().rename(columns = {"index": "block_t0"})
length_before = uni.shape[0]

## join v3 factory
uni = pd.merge(uni, factory_v3[['token0', 'token1', 'fee', 'pool']], 
         left_on = ['v3pool'], right_on = ['pool'], how = 'left')

## join erc20 token names
uni = pd.merge(uni, erc20[['contract_address', 'symbol']].rename(columns = {"symbol": "token0_sym"}), 
                 left_on = ['token0'], 
                 right_on = ['contract_address'], how = 'left').drop("contract_address", axis = 1)

uni = pd.merge(uni, erc20[['contract_address', 'symbol']].rename(columns = {"symbol": "token1_sym"}), 
                 left_on = ['token1'], 
                 right_on = ['contract_address'], how = 'left').drop("contract_address", axis = 1)

## sanity checks
assert length_before == uni.shape[0], 'Merging change the lengths of the values'
uni['tvlUSD_v2'] = uni['tvlUSD_v2'].astype(float)
uni['tvlUSD_v3'] = uni['tvlUSD_v3'].astype(float)
uni['largestV3'] = 0


stable = (
            pd.read_csv("passive_df.csv")
            .dropna()
            .drop("Unnamed: 0", axis = 1)
        )

stable['dt'] = pd.to_datetime(stable['dt'])

uni['stableV3'] = uni['v3Ret']

uni = pd.merge(uni, stable[['block_t0', 'v3Pool', 'feeRet', 'curTick', 'min_tick', 'max_tick']], 
         left_on = ['block_t0', 'v3pool'], 
         right_on = ['block_t0', 'v3Pool'], how = 'left').copy().rename(columns = {"feeRet": "stable_stable"})

uni.loc[uni["stable_stable"].notna(), 'stableV3'] = uni.loc[uni["stable_stable"].notna(), 'stable_stable']

uni_before_trunc = uni.copy()
uni = uni[uni['date'] >= pd.to_datetime("09-16-2021")].reset_index(drop = True).copy()

largest_tvl = (uni[['v2pool_x', 'v3pool', 'tvlUSD_v3']]
               .groupby(['v2pool_x', 'v3pool'])
               .mean()
               .reset_index()
              )

# if you groupby v2pool_x and select max, it gives you the highest valued hex string
# which is not what you want
largest_tvl = largest_tvl.loc[largest_tvl.groupby("v2pool_x")['tvlUSD_v3'].idxmax()].copy()

uni.loc[uni['v3pool'].isin([*largest_tvl['v3pool']]), 'largestV3'] = 1

uni_before_trunc.loc[uni_before_trunc['v3pool'].isin([*largest_tvl['v3pool']]), 'largestV3'] = 1

# rename columns
uni = uni.drop(['date', 'v2pool'], axis = 1).rename(columns = {"v2pool_x": "v2pool",
                                                    "marketdepth": "v3_md",
                                                    "px": "token0_usd_px",
                                                    "dt_x": "date"})

# top pools # top 21 gives 10 labels
label_n = 10
top_pools = uni.groupby("v3pool")['tvlUSD_v3'].mean().sort_values(ascending = False).iloc[0:label_n].index
uni['top_Npools'] = 0

uni.loc[uni['v3pool'].isin(top_pools), 'top_Npools'] = 1

print(f"Tracked stable-stable pools: {[*uni.loc[uni['stable_stable'].notna(), 'v3pool'].unique()]}")

# clean up the columns
uni = uni[['block_t0', 'block_t1', 'date', 'v2Ret', 'v3pool', 'v3Ret',
                    'v2pool', 'stableV3', 'largestV3']].copy()

uni = uni.rename(columns = {"stableV3": "nonrebalancingV3"})

largest_v3 = uni[uni['largestV3'] == 1].copy()
print(uni.loc[uni['top_Npools'] == 1, 'v3pool'].unique().shape)

# save the entire df
uni.to_csv("returns_df.csv")

if paper_print:
    paper_metrics(uni)