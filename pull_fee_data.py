import pandas as pd
import numpy as np
import os

import requests
import pandas as pd
import time

from tqdm import tqdm
from tqdm.contrib.concurrent import process_map

from web3 import Web3

import sqlite3

path = os.getcwd()

if not os.path.exists(f"{path}/sql"):
    os.mkdir(f"{path}/sql")

conn = sqlite3.connect(f"{path}/sql/returns.db")
cursor = conn.cursor()


def v3_call(input_data):
    dt, block_num, v3_pool, v2_pool = input_data
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
                ticks(where: {id: "%s#-887220"}) {
                  id
                  feeGrowthOutside0X128
                  feeGrowthOutside1X128
                  price1
                  price0
                }
                token0Price
                sqrtPrice
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
        v3_pool,
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

    # subgraph = request.json()
    subgraph = subgraph["data"]["pool"]

    if not subgraph:
        return None

    if subgraph["liquidity"] == "0":
        return None

    if len(subgraph["ticks"]) == 0:
        tick = {}
        tick["feeGrowthOutside0X128"] = -1
        tick["feeGrowthOutside1X128"] = -1
        tick["price0"] = -1
        tick["price1"] = -1

    else:
        tick = subgraph["ticks"][0]

    v3 = (
        dt,
        block_num,
        v3_pool,
        subgraph["liquidity"],
        subgraph["feeGrowthGlobal0X128"],
        subgraph["feeGrowthGlobal1X128"],
        tick["feeGrowthOutside0X128"],
        tick["feeGrowthOutside1X128"],
        tick["price0"],
        tick["price1"],
        subgraph["token0Price"],
        subgraph["sqrtPrice"],
        subgraph["token0"]["id"],
        subgraph["token1"]["id"],
        subgraph["token0"]["decimals"],
        subgraph["token1"]["decimals"],
        subgraph["tick"],
        subgraph['totalValueLockedUSDUntracked'],
        subgraph['totalValueLockedUSD'],
        v2_pool,
    )

    time.sleep(0.25)

    return v3


def v2_call(input_data):
    dt, block_num, v2_pool, v3_pool, diff_order = input_data
    q = """
                    query MyQuery {
                    pair (
                        id: "%s"
                        block: {number: %s}
                    ) {
                        reserve0
                        reserve1
                        token0 {
                        decimals
                        id
                        name
                        }
                        token1 {
                        decimals
                        id
                        name
                        }
                        totalSupply
                        reserveUSD
                    }
                    }
                """ % (
        v2_pool,
        block_num,
    )

    request = requests.post(
        "https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2" "",
        json={"query": q},
    )

    if request.status_code == 200:
        subgraph = request.json()
    else:
        print(f"Failed with reason {request.status_code}")
        return None

    subgraph = subgraph["data"]["pair"]

    if not subgraph:
        return None

    v2 = (
        dt,
        block_num,
        v2_pool,
        subgraph["reserve0"],
        subgraph["reserve1"],
        subgraph["totalSupply"],
        subgraph["token0"]["id"],
        subgraph["token1"]["id"],
        subgraph["token0"]["decimals"],
        subgraph["token1"]["decimals"],
        subgraph['reserveUSD'],
        diff_order,
        v3_pool,
    )
    time.sleep(0.25)

    return v2


if __name__ == "__main__":
    top250 = True
    rerun = False
    if rerun:
        print("Dropping table")

        cursor.execute("DROP TABLE IF EXISTS v2")
        cursor.execute("DROP TABLE IF EXISTS v3")

        cursor.execute(
            """CREATE TABLE IF NOT EXISTS v2 (dt text, block real, pool text, reserve0 text, reserve1 text,
                        totalSupply text, token0 text, token1 text, token0Dec text, token1Dec text, tvlUSD text,
                        orderDiff real, v3Pool text)"""
        )

        cursor.execute(
            """CREATE TABLE IF NOT EXISTS v3 (dt text, block real, pool text, liquidity text, feeGG0 text, 
                                        feeGG1 text, feeGO0 text, feeGO1 text, tickPrice0 text, tickPrice1 text, 
                                        token0Price text, sqrtPrice text, token0ID text, token1ID text, token0Dec text, 
                                        token1Dec text, tick text, tvlUSDUntracked text, tvlUSD text, v2Pool text)"""
        )

        conn.commit()

    # read in the blocks we care about
    blocks = pd.read_csv(f"{path}/data/block_to_dt.csv")
    blocks["dt"] = pd.to_datetime(blocks["dt"]).dt.strftime("%m-%d-%y")

    if top250:
        q = """
                query MyQuery {
                pools(orderBy: volumeUSD, orderDirection: desc, first: 1000) {
                    id
                }
                }
        """
        request = requests.post(
            "https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3" "",
            json={"query": q},
        )

        v3_top_pools = request.json()
        v3_top_pools = v3_top_pools["data"]["pools"]
        v3_pools = [entry["id"] for entry in v3_top_pools]

    else:
        # v3 pool
        v3_pools = [
            "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8",  # usdceth 30bps
            "0xea4ba4ce14fdd287f380b55419b1c5b6c3f22ab6",  # competh 30 bps
            "0xac4b3dacb91461209ae9d41ec517c2b9cb1b7daf",
        ]  # apeeth 30 bps

    factory_v2 = pd.read_csv(f"{path}/data/factory_v2.csv")
    factory_v3 = pd.read_csv(f"{path}/data/factory_v3.csv")

    # get rid of the already finished pools - we do v2 first, so we should at most have duplicates of v2
    # probably should drop duplicates
    _df = pd.read_sql("SELECT * from v3", conn)
    finished_pools = [*_df["pool"].unique()]
    before = len(v3_pools)
    v3_pools = [pool for pool in v3_pools if pool not in finished_pools]
    after = len(v3_pools)

    print(f"Removed {before - after} pools")

    for v3_pool in v3_pools:
        diff_order = 0
        v3_info = factory_v3[factory_v3["pool"] == v3_pool]
        if len(v3_info) == 0:
            print(f'Cannot find info in factory on v3_pool: {v3_pool}')
            continue

        v3_token0, v3_token1, starting_block = v3_info[
            ["token0", "token1", "block_number"]
        ].values[0]

        # find the pool with token0 and token1 in both slots, but not neccesarly in that order
        pool = factory_v2[
            (factory_v2["token0"] == v3_token0) | (factory_v2["token1"] == v3_token0)
        ]
        pool = pool[(pool["token0"] == v3_token1) | (pool["token1"] == v3_token1)]

        if pool.empty:
            print(f"Cannot find corrosponding pool for {v3_pool}")
            continue

        v2_pool = pool["pair"].item()

        if pool["token0"].item() == v3_token0:
            diff_order = 0
        else:
            diff_order = 1

        future_blocks = blocks[blocks["block"] > starting_block]

        v2_data = []
        unpacked_blocks = [[*arr] for arr in future_blocks.values]
        unpacked_blocks = [
            block + [v2_pool, v3_pool, diff_order] for block in unpacked_blocks
        ]

        v2_data = process_map(v2_call, unpacked_blocks, max_workers=20)
        v2_data = [data for data in v2_data if data]

        cursor.executemany(
            "INSERT INTO v2 values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", v2_data
        )
        conn.commit()

        v3_data = []
        unpacked_blocks = [[*arr] for arr in future_blocks.values]
        unpacked_blocks = [block + [v3_pool, v2_pool] for block in unpacked_blocks]
        v3_data = process_map(v3_call, unpacked_blocks, max_workers=20)

        # process out the None returns
        v3_data = [data for data in v3_data if data]

        cursor.executemany(
            "INSERT INTO v3 values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            v3_data,
        )
        conn.commit()
