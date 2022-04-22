import pandas as pd
import numpy as np
import os

import requests
import pandas as pd
import time

from tqdm import tqdm

from web3 import Web3

import sqlite3

path = os.getcwd()
conn = sqlite3.connect(f"{path}/sql/returns.db")
cursor = conn.cursor()

rerun = True

if rerun:
    print("Dropped table")
    cursor.execute("""DROP TABLE IF EXISTS v3_ret""")
    cursor.execute("""DROP TABLE IF EXISTS v2_ret""")

    cursor.execute(
        """CREATE TABLE IF NOT EXISTS v3_ret 
                        (block_t0 text, block_t1 text, feeRet real,
                        v3pool text, v2pool text, tvlUSD text)"""
    )

    cursor.execute(
        """CREATE TABLE IF NOT EXISTS v2_ret 
                        (block_t0 text, block_t1 text, feeRet real,
                        v2pool text, v3pool text, tvlUSD text)"""
    )

    conn.commit()

v2 = pd.read_sql("SELECT * from v2", conn).drop_duplicates().sort_values(by = "block")
v3 = pd.read_sql("SELECT * from v3", conn).drop_duplicates().sort_values(by = "block")

tickPrice0 = (
    "0.000000000000000000000000000000000000002954278418582885262890650958806081"
)
tickPrice1 = "338492131855223783697272027725930700000"

for pool in v3["pool"].unique():
    # the sort should be unneded but i dont want any race conditions
    v3_pool = v3[v3["pool"] == pool].sort_values(by="block").copy()

    v3_pool["tickPrice0"] = v3_pool["tickPrice0"].replace("-1", tickPrice0)
    v3_pool["tickPrice1"] = v3_pool["tickPrice1"].replace("-1", tickPrice1)

    # if the tick has not been initialized, we find the first value after it has been
    # however, if it is never initialized we just never adjust it.
    # because feeGO0 is constant, this is fine
    if not v3_pool[v3_pool['feeGO0'] != '-1'].empty:
        v3_pool["feeGO0"] = v3_pool[v3_pool['feeGO0'] != '-1']['feeGO0'].iloc[0]
        v3_pool["feeGO1"] = v3_pool[v3_pool['feeGO1'] != '-1']['feeGO1'].iloc[0]

    ## change the datatype and adj units
    v3_pool["pctInput"] = 0.00001
    v3_pool["posLiq"] = v3_pool["liquidity"].astype(float) * v3_pool["pctInput"]
    v3_pool["adjSqrtPrice"] = v3_pool["sqrtPrice"].astype(float) / (2**96)

    ### calc the needed fee values
    v3_pool["fr1_0"] = (
        v3_pool["feeGG0"].astype(float) - v3_pool["feeGO0"].astype(float)
    ) / (2**128)
    v3_pool["fr0_0"] = v3_pool["fr1_0"].shift(1)

    v3_pool["fr1_1"] = (
        v3_pool["feeGG1"].astype(float) - v3_pool["feeGO1"].astype(float)
    ) / (2**128)
    v3_pool["fr0_1"] = v3_pool["fr1_1"].shift(1)

    # calc the fees themselves from time 0 to time 1
    v3_pool["fees0"] = (
        (v3_pool["fr1_0"] - v3_pool["fr0_0"])
        * (v3_pool["posLiq"])
        / 10 ** v3_pool["token0Dec"].astype(int)
    )
    v3_pool["fees1"] = (
        (v3_pool["fr1_1"] - v3_pool["fr0_1"])
        * (v3_pool["posLiq"])
        / 10 ** v3_pool["token1Dec"].astype(int)
    )

    # calculate the spot values of our earned liquidity
    v3_pool["posLiq0"] = (
        v3_pool["posLiq"]
        * (np.sqrt(v3_pool["tickPrice1"].astype(float)) - v3_pool["adjSqrtPrice"])
        / (np.sqrt(v3_pool["tickPrice1"].astype(float)) * v3_pool["adjSqrtPrice"])
        / (10 ** v3_pool["token0Dec"].astype(int))
    )

    v3_pool["posLiq1"] = (
        v3_pool["posLiq"]
        * (v3_pool["adjSqrtPrice"] - np.sqrt(v3_pool["tickPrice0"].astype(float)))
        / (10 ** v3_pool["token1Dec"].astype(int))
    )
    # this is the position cost
    v3_pool["posValue"] = v3_pool["posLiq0"] + v3_pool["posLiq1"] * v3_pool[
        "token0Price"
    ].astype(float)
    v3_pool["fees"] = v3_pool["fees0"] + v3_pool["fees1"] * v3_pool[
        "token0Price"
    ].astype(float)

    v3_pool["feeRet_t1-t0"] = v3_pool["fees"] / v3_pool["posValue"]
    v3_pool["block_t0"] = v3_pool["block"].shift(1)

    tgts = ["block_t0", "block", "feeRet_t1-t0", "pool", "v2Pool", 'tvlUSD']
    data = v3_pool[tgts].rename(columns={"v2Pool": "otherVer"})
    data = data.dropna()
    unpacked = [[*arr] for arr in data.values]

    cursor.executemany("INSERT INTO v3_ret values (?, ?, ?, ?, ?, ?)", unpacked)
    conn.commit()

for pool in v2["pool"].unique():
    v2_pool = v2[v2["pool"] == pool].copy()

    v2_pool["tok1/tok0_t1"] = v2_pool["reserve0"].astype(float) / v2_pool[
        "reserve1"
    ].astype(float)
    v2_pool["tok1/tok0_t0"] = v2_pool["tok1/tok0_t1"].shift(1)
    v2_pool["totalSupply"] = v2_pool["totalSupply"].astype(float)
    v2_pool["totalSupply_t0"] = v2_pool["totalSupply"].shift(1)
    v2_pool["pctSupplied"] = 0.001

    v2_pool["reserve0"] = v2_pool["reserve0"].astype(float)
    v2_pool["reserve0_t0"] = v2_pool["reserve0"].shift(1)

    v2_pool["reserve1"] = v2_pool["reserve1"].astype(float)
    v2_pool["reserve1_t0"] = v2_pool["reserve1"].shift(1)

    v2_pool["block_t0"] = v2_pool["block"].shift(1)

    v2_pool["tok0_t0"] = v2_pool["pctSupplied"] * v2_pool["reserve0_t0"]
    v2_pool["tok1_t0"] = v2_pool["pctSupplied"] * v2_pool["reserve1_t0"]

    v2_pool["k_t0"] = v2_pool["reserve0_t0"] * v2_pool["reserve1_t0"]
    v2_pool["x_t0"] = (
        np.sqrt(v2_pool["k_t0"] * v2_pool["tok1/tok0_t0"]) * v2_pool["pctSupplied"]
    )
    v2_pool["y_t0"] = (
        np.sqrt(v2_pool["k_t0"] / v2_pool["tok1/tok0_t0"]) * v2_pool["pctSupplied"]
    )
    v2_pool["x_t1"] = (
        np.sqrt(v2_pool["k_t0"] * v2_pool["tok1/tok0_t1"]) * v2_pool["pctSupplied"]
    )
    v2_pool["y_t1"] = (
        np.sqrt(v2_pool["k_t0"] / v2_pool["tok1/tok0_t1"]) * v2_pool["pctSupplied"]
    )

    v2_pool["synPort_t0"] = v2_pool["x_t0"] + v2_pool["y_t0"] * v2_pool["tok1/tok0_t0"]
    v2_pool["synPort_t1"] = v2_pool["x_t1"] + v2_pool["y_t1"] * v2_pool["tok1/tok0_t1"]

    v2_pool["v2Port_t0"] = (v2_pool["pctSupplied"] * v2_pool["reserve0_t0"]) + (
        v2_pool["pctSupplied"] * v2_pool["reserve1_t0"] * v2_pool["tok1/tok0_t0"]
    )

    v2_pool["hodlPort_t1-t0"] = (v2_pool["pctSupplied"] * v2_pool["reserve0_t0"]) + (
        v2_pool["pctSupplied"] * v2_pool["reserve1_t0"] * v2_pool["tok1/tok0_t1"]
    )

    v2_pool["v2Port_t1"] = (
        ((v2_pool["pctSupplied"] * v2_pool["totalSupply_t0"]) / v2_pool["totalSupply"])
        * v2_pool["reserve0"]
    ) + (
        ((v2_pool["pctSupplied"] * v2_pool["totalSupply_t0"]) / v2_pool["totalSupply"])
        * v2_pool["reserve1"]
        * v2_pool["tok1/tok0_t1"]
    )

    #     px_ratio = v2_pool['tok1/tok0_t1'] / v2_pool['tok1/tok0_t0']
    #     divergence_loss = 2 * np.sqrt(px_ratio) / (1 + px_ratio) - 1
    #     v2_pool['imp_loss'] = divergence_loss * v2_pool['hodlPort_t1-t0']
    #     v2_pool['feeRet'] = v2_pool['v2Port_t1'] - (v2_pool['imp_loss'] + v2_pool['hodlPort_t1-t0'])

    # these are equivalent
    v2_pool["feeRet"] = v2_pool["v2Port_t1"] - v2_pool["synPort_t1"]

    v2_pool["pctRet"] = v2_pool["feeRet"] / v2_pool["v2Port_t0"]

    tgts = ["block_t0", "block", "pctRet", "pool", "v3Pool", 'tvlUSD']
    data = v2_pool[tgts].rename(columns={"v3Pool": "otherVer"}).dropna()
    unpacked = [[*arr] for arr in data.values]

    cursor.executemany("INSERT INTO v2_ret values (?, ?, ?, ?, ?, ?)", unpacked)
    conn.commit()

# dump data for sql upload
pd.read_sql("SELECT * from v3_ret", conn).to_csv(
    f"{path}/sql/v3_returns.csv", index=False, header=False
)
pd.read_sql("SELECT * from v2_ret", conn).to_csv(
    f"{path}/sql/v2_returns.csv", index=False, header=False
)
