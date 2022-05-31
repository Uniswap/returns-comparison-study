import pandas as pd
import requests
import numpy as np
import os
import pickle
import time 

from tqdm.contrib.concurrent import process_map
from scipy.stats.mstats import winsorize

def subgraph_call(arr):
    v3_pool, dt, block_num = arr
    q = """
            query MyQuery {
                  pool(
                    id: "%s"
                    block: {number: %s}
                  ) {
                    id
                    liquidity
                    feeGrowthGlobal1X128
                    feeGrowthGlobal0X128
                    token0Price
                    sqrtPrice
                    feeTier
                    token1 {
                      name
                      decimals
                      id
                    }
                    token0 {
                      name
                      decimals
                      id
                    }
                    tick
                    totalValueLockedUSDUntracked
                    totalValueLockedUSD
                  }
                }
            """ % (
            v3_pool,
            block_num,
        )

    request = requests.post(
        "https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3" "",
        json={"query": q},
    )

    if request.status_code == 200:
        subgraph = request.json()
    else:
        print("Failed with reason ")
        return None

    if subgraph == None:
        print("Subgraph malformed")
        return None

    # subgraph = request.json()
    subgraph = subgraph["data"]["pool"]

    if not subgraph:
        print("Subgraph not alive")
        return None

    if subgraph["liquidity"] == "0":
        print("Empty Liquidity")
        return None
    
    return (subgraph['id'],
            subgraph['feeGrowthGlobal0X128'],
            subgraph['feeGrowthGlobal1X128'],
            subgraph['tick'],
            subgraph['token0']['decimals'],
            subgraph['token1']['decimals'],
            subgraph['sqrtPrice'],
            subgraph['liquidity'],
            subgraph['feeTier'],
            block_num,
            v3_pool,
            dt)

if __name__ == "__main__":
    stable_stable = ["0x5777d92f208679db4b9778590fa3cab3ac9e2168",
                "0xc63b0708e2f7e69cb8a1df0e1389a98c35a76d52",
                "0x3416cf6c708da44db2624d63ea0aaef7113527c6",
                "0x6c6bc977e13df9b0de53b251522280bb72383700",
                "0x97e7d56a0408570ba1a7852de36350f7713906ec",
                "0x10581399a549dbfffdbd9b070a0ba2f9f61620d2",
                "0x00cef0386ed94d738c8f8a74e8bfd0376926d24c",
                "0x7858e59e0c01ea06df3af3d20ac7b0003275d4bf",
                "0xa9ffb27d36901f87f1d0f20773f7072e38c5bfba",
                "0xdf50fbde8180c8785842c8e316ebe06f542d3443",
                "0x1c5c60bef00c820274d4938a5e6d04b124d4910b",
                "0xbb2e5c2ff298fd96e166f90c8abacaf714df14f8",
                "0x39529e96c28807655b5856b3d342c6225111770e",
                "0x8c54aa2a32a779e6f6fbea568ad85a19e0109c26",
                "0x5180545835bd68810fb7e11c7160bb7ea4ae8744",
                "0xb65fc555b0e970b16329a48e45cfa14fce9a36a4",
                "0x6f48eca74b38d2936b02ab603ff4e36a6c0e3a77",
                "0xd5ad5ec825cac700d7deafe3102dc2b6da6d195d",
                "0x4e0924d3a751be199c426d52fb1f2337fa96f736",
                "0x16980c16811bde2b3358c1ce4341541a4c772ec9",
                "0xd340b57aacdd10f96fc1cf10e15921936f41e29c"]

    columns = ['id', 'feeGrowthGlobal0X128', 'feeGrowthGlobal1X128',
        'curTick', 'token0Dec', 'token1Dec', "sqrtPrice", "liquidity", "feeTier", "block_num", 
        'v3Pool', 'dt']


    path = os.getcwd()

    blocks = pd.read_csv(f"{path}/data/block_to_dt.csv")
    blocks["dt"] = pd.to_datetime(blocks["dt"]).dt.strftime("%m-%d-%y")

    for v3_pool in stable_stable:
        print(v3_pool)

        factory_v3 = pd.read_csv(f"{path}/data/factory_v3.csv")
        factory_entry = factory_v3.loc[factory_v3['pool'] == v3_pool, 'block_number']
        if factory_entry.empty:
            continue

        starting_block = factory_entry.item()

        future_blocks = blocks[blocks["block"] > starting_block]

        unpacked_blocks = [[*arr] for arr in future_blocks.values]
        block = [blck[1] for blck in unpacked_blocks]

        arr = [[v3_pool] + blck for blck in unpacked_blocks]

        ret = process_map(subgraph_call, arr, max_workers=20)

        with open(f"dump/{v3_pool}.pkl", 'wb') as f:
                pickle.dump(ret, f)


    dt = {}

    for _file in os.listdir(f"{path}/dump"):
        if _file == "failed" or _file == '.DS_Store':
            continue
        pool = _file.split(".")[0]
        
        with open(f"{path}/dump/{_file}", 'rb') as f:
            data = pickle.load(f)
        
        dt[pool] = data

    stable_stable_df = pd.DataFrame()

    stable_stable_df = pd.DataFrame()

    windosor_lvl = .001
    for pool in dt.keys():
        output = [blck for blck in dt[pool] if blck is not None]
        v3_pool = pd.DataFrame(output, columns = columns)
        v3_pool = v3_pool.sort_values(by="block_num").copy()

        v3_pool['underBelow'] = 0
        v3_pool['aboveAbove'] = 0

        v3_pool['v3Pool'] = pool
        v3_pool["pctInput"] = 0.0001
        v3_pool["posLiq"] = v3_pool["liquidity"].astype(float) * v3_pool["pctInput"]
        v3_pool["adjSqrtPrice"] = v3_pool["sqrtPrice"].astype(float) / (2**96)
        v3_pool['curTick'] = v3_pool['curTick'].astype(int)
        
        # windosorize the edges at 0.005 - clip out 
        wind = winsorize(v3_pool['curTick'], limits=[windosor_lvl, windosor_lvl])
        perc_min_tick, perc_max_tick = wind.min(), wind.max()
        min_tick, max_tick = int(perc_min_tick) - 1, int(perc_max_tick) + 1

        # push out the range by 1 tick-spacing so we dont have to worry about within tick calculations
        # this is more realistic anyways since we want to ensure 99% is 
        if int(v3_pool['feeTier'].iloc[0]) == 500:
            tick_adj = 10
        else:
            tick_adj = 1
            
        v3_pool['min_tick'] = min_tick - tick_adj
        v3_pool['max_tick'] = max_tick + tick_adj
        
        
        v3_pool['price0_cur'] = 1.0001 ** v3_pool['curTick']
        v3_pool['price0_a'] = 1.0001 ** max_tick
        v3_pool['price0_b'] = 1.0001 ** min_tick

        # use whitepaper equations to calculate underlying
        v3_pool['token0_underlying'] = (
                                v3_pool['posLiq'] * 
                                ((np.sqrt(v3_pool['price0_a'].astype(float)) - v3_pool['adjSqrtPrice']) / 
                                (np.sqrt(v3_pool['price0_a'].astype(float)) * v3_pool['adjSqrtPrice'])) /
                                    (10 ** v3_pool['token0Dec'].astype(int))
                                )

        v3_pool['token1_underlying'] = (
                            v3_pool["posLiq"] * 
                            (v3_pool['adjSqrtPrice'] - np.sqrt(v3_pool['price0_b'].astype(float))) 
                            / (10 ** v3_pool['token1Dec'].astype(int))
                            )
        
        v3_pool["feeGrowthGlobal0X128_t1"] = v3_pool["feeGrowthGlobal0X128"].astype(float)
        v3_pool["feeGrowthGlobal1X128_t1"] = v3_pool["feeGrowthGlobal1X128"].astype(float)
        v3_pool['feeGrowthGlobal0X128_t0'] = v3_pool['feeGrowthGlobal0X128_t1'].shift(1)
        v3_pool['feeGrowthGlobal1X128_t0'] = v3_pool['feeGrowthGlobal1X128_t1'].shift(1)

        v3_pool['fees_token0'] = ((v3_pool["feeGrowthGlobal0X128_t1"] - v3_pool['feeGrowthGlobal0X128_t0']) / (2**128) * 
                                    v3_pool['posLiq'] / 
                                    (10 ** v3_pool['token0Dec'].astype(int)))

        v3_pool['fees_token1'] = ((v3_pool["feeGrowthGlobal1X128_t1"] - v3_pool['feeGrowthGlobal1X128_t0']) / (2**128) * 
                                    v3_pool['posLiq'] / 
                                    (10 ** v3_pool['token1Dec'].astype(int)))

        v3_pool['feeRet'] = ((v3_pool['fees_token0'] + v3_pool['fees_token1']) / 
                            (v3_pool['token0_underlying'] + v3_pool['token1_underlying']))

        v3_pool['block_t1'] = v3_pool['block_num']
        v3_pool['block_t0'] = v3_pool['block_t1'].shift(1)

        # discount the fee return after clipping the edges
        v3_pool['feeRet'] = (v3_pool['feeRet'] * (1 - (windosor_lvl * 2)))
        v3_pool = v3_pool[['dt', 'block_t0', 'block_t1', 'feeRet', 'v3Pool', 
                        'curTick', 'min_tick', 'max_tick', 'token0Dec', 'token1Dec']]

        if stable_stable_df.empty:
            stable_stable_df = v3_pool.copy()
        else:
            stable_stable_df = pd.concat([stable_stable_df, v3_pool])

        stable_stable_df.to_csv("passive_df.csv")
            