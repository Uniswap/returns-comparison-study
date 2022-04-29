import pandas as pd
import numpy as np
import os

import requests
import pandas as pd
import time

import math

import sqlite3

import statsmodels.api as sm


# pull in the relevant data sources
# sometimes i place data in other directories instead of the current one, so
# i assign path variables to that other directory
path = os.getcwd()

conn = sqlite3.connect(f"{path}/sql/returns.db")
cursor = conn.cursor()

# variables to look at
tvl_limit = 1000
drop_pools = False
zero_rets = True
n = 250
v2_md_depth = .02

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

# erc20 for token names
erc20 = pd.read_csv(f"{path}/data/erc20.csv")
erc20['contract_address'] = erc20['contract_address'].apply(lambda x: str(x).replace('\\', '0'))

## usd volume from dune
usd_v3 = pd.read_csv(f'{path}/data/usd_v3.csv')
usd_v2 = pd.read_csv(f'{path}/data/usd_v2.csv')
usd_v3['dt'] = usd_v3['dt'].apply(lambda x: pd.to_datetime(x).tz_localize(None))
usd_v2['dt'] = usd_v2['dt'].apply(lambda x: pd.to_datetime(x).tz_localize(None))

usd_v3['exchange_contract_address'] = usd_v3['exchange_contract_address'].apply(lambda x: x.replace('\\', '0'))
usd_v2['exchange_contract_address'] = usd_v2['exchange_contract_address'].apply(lambda x: x.replace('\\', '0'))

usd_v3 = usd_v3.rename(columns = {"usd_volume": "v3_volume",
                                 "exchange_contract_address": "v3pool"})[['dt', 'v3pool', 'v3_volume']]

usd_v2 = usd_v2.rename(columns = {"usd_volume": "v2_volume",
                                 "exchange_contract_address": "v2pool"})[['dt', 'v2pool', 'v2_volume']]

## v2 md calculations
v2_raw = pd.read_sql("SELECT * from v2", conn)
v2_raw['dt'] = pd.to_datetime(v2_raw['dt'])

v2_raw['md_+2'] = abs(v2_raw['reserve0'].astype(float) * (np.sqrt(1 + v2_md_depth) - 1))
v2_raw['md_-2'] = abs(v2_raw['reserve0'].astype(float) * (np.sqrt(1 - v2_md_depth) - 1))

v2_raw['v2_md'] = v2_raw['md_+2'] + v2_raw['md_-2']

v2_md = v2_raw[['dt', 'pool', 'v2_md']]

print("Done reading data")

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
    f"https://api.thegraph.com/subgraphs/name/{prod_url}" "",
    json={"query": q},
)

v3_top_pools = request.json()
v3_top_pools = v3_top_pools["data"]["pools"]
v3_pools = [entry["id"] for entry in v3_top_pools]

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
uni = v2.merge(v3, left_on = ['block_t0', 'block_t1', 'v3pool'], right_on = ['block_t0', 'block_t1', 'v3pool'])
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
length_before = uni.shape[0]

# join usd volume
uni = pd.merge(uni, usd_v3, left_on = ['dt', 'v3pool'], right_on = ['dt', 'v3pool'], how = 'left')
uni = pd.merge(uni, usd_v2, left_on = ['dt', 'v2pool_x'], right_on = ['dt', 'v2pool'], how = 'left')

## join v3 md dataframe
conc = md[md['pct'].isin([-.02, .02])].copy()
conc = conc.reset_index().groupby(['date', 'address']).sum().reset_index().copy()[['date', 'address', 'marketdepth']]
uni = pd.merge(uni, conc, left_on = ['dt', 'v3pool'], right_on = ['date', 'address'], how = 'left').copy()

## join v2 md
# there will be duplicates from multiple fee tiers, but we can just drop duplicates
uni = pd.merge(uni, v2_md, left_on = ['dt', 'v2pool'], right_on = ['dt', 'pool'], how = 'left').drop_duplicates()

## sanity checks
assert length_before == uni.shape[0], 'Merging change the lengths of the values'

uni.to_csv(f"{path}/data/returns_joined_df.csv")
