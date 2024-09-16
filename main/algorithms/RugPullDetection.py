import utils.Utils as ut
from tqdm import tqdm
import os
import pandas as pd
from utils import Constant
from utils.Settings import Setting
from utils.Path import Path
from api import EtherscanAPI, BSCscanAPI
from data_collection.EventCollector import ContractEventCollector
import numpy as np
from web3 import Web3

path = Path()
setting = Setting()

key_index = 0

explorer_api = {
    "univ2": {"explorer": EtherscanAPI, "keys": setting.ETHERSCAN_API_KEYS},
    "panv2": {"explorer": BSCscanAPI, "keys": setting.BSCSCAN_API_KEYS},
}


def get_balance_of_weth_before_sell_rug(mints, burns, swaps, weth_position, max_swap_idx):
    total_mint_amount, total_burn_amount, total_swap_in, total_swap_out = 0, 0, 0, 0
    swaps_df = pd.DataFrame(swaps)
    max_swap_time_stamp = swaps_df["timeStamp"].tolist()[max_swap_idx]
    for mint in mints:
        if int(mint["timeStamp"]) <= max_swap_time_stamp:
            total_mint_amount += mint[f"amount{weth_position}"] / 10 ** 18
    for burn in burns:
        if int(burn["timeStamp"]) <= max_swap_time_stamp:
            total_burn_amount += burn[f"amount{weth_position}"] / 10 ** 18
    swap_out_amounts = swaps_df[f"amount{weth_position}Out"].astype(float) / 10 ** 18
    swap_in_amounts = swaps_df[f"amount{weth_position}In"].astype(float) / 10 ** 18
    swap_out_amounts = swap_out_amounts[:max_swap_idx]
    swap_in_amounts = swap_in_amounts[:max_swap_idx]
    total_swap_in = np.sum(swap_in_amounts)
    total_swap_out = np.sum(swap_out_amounts)
    return total_mint_amount + total_swap_in - total_burn_amount - total_swap_out


def is_mint_transfer(transfer):
    return transfer["from"] == Constant.ZERO and transfer["to"] != Constant.ZERO


def is_burn_transfer(transfer):
    return transfer["from"] != Constant.ZERO and transfer["to"] in Constant.BURN_ADDRESSES


def is_simple_rug_pull(transfers):
    mints = []
    burns = []
    scammers = set()
    for transfer in transfers:
        if is_mint_transfer(transfer):
            mints.append(transfer)
            scammers.add(transfer["to"])
        if is_burn_transfer(transfer):
            burns.append(transfer)
            scammers.add(transfer["from"])
    if len(mints) != 1 or len(burns) != 1:
        return False, []
    trading_period = burns[0]["timeStamp"] - mints[0]["timeStamp"]
    if trading_period > Constant.ONE_DAY_TIMESTAMP or trading_period < 0:
        return False, []
    minted_liq_tokens = float(mints[0]["value"]) / 10 ** Constant.POOL_DECIMALS
    burned_liq_tokens = float(burns[0]["value"]) / 10 ** Constant.POOL_DECIMALS
    if (burned_liq_tokens / minted_liq_tokens) >= 0.99:
        return True, scammers
    return False, []


def is_sell_rug_pull(transfers, mints, burns, swaps, weth_position):
    # expect 1 mint no burn and multiple swaps
    if len(swaps) == 0 or len(mints) != 1 or len(burns) > 0:
        return False, []
    swaps_df = pd.DataFrame(swaps)
    # find max swap
    hv_swap_amounts = swaps_df[f"amount{weth_position}Out"].astype(float) / 10 ** 18
    max_swap_amount = np.max(hv_swap_amounts)
    max_swap_idx = np.argmax(hv_swap_amounts)
    # rp period is one day
    trading_period = swaps[max_swap_idx]["timeStamp"] - mints[0]["timeStamp"]
    if trading_period > Constant.ONE_DAY_TIMESTAMP or trading_period < 0:
        return False, []

    # calculate WETH balance before rugging
    weth_balance = get_balance_of_weth_before_sell_rug(mints, burns, swaps, weth_position, max_swap_idx)

    # check if 99% of WETH be rugged by swap
    if max_swap_amount >= 0.99 * weth_balance:
        # extract scammers from first mint and max swap
        scammers = {swaps_df["to"].tolist()[max_swap_idx], swaps_df["sender"].tolist()[max_swap_idx]}
        for transfer in transfers:
            if is_mint_transfer(transfer):
                mints.append(transfer)
                scammers.add(transfer["to"])
            # if is_burn_transfer(transfer):
            #     burns.append(transfer)
            #     scammers.add(transfer["from"])
        return True, scammers
    return False, []

def is_1d_rug_pull(transfers, mints, burns, swaps, weth_position):
    if len(mints) < 1:
        return False, []
    result, scammers = is_simple_rug_pull(transfers)
    if result:
        return 1, scammers
    result, scammers = is_sell_rug_pull(transfers, mints, burns, swaps, weth_position)
    if result:
        return 2, scammers
    return 0, []


def is_1d_token(scam_token, dex='univ2'):
    contract_event_collector = ContractEventCollector()
    event_path = eval('path.{}_token_events_path'.format(dex))
    transfers = contract_event_collector.get_event(scam_token, "Transfer", event_path, dex)
    token_life_time = 0
    ts = np.array([e["timeStamp"] for e in transfers])
    if len(ts) > 0:
        first_event_ts = np.min(ts)
        last_event_ts = np.max(ts)
        token_life_time = last_event_ts - first_event_ts
    return token_life_time <= Constant.ONE_DAY_TIMESTAMP


def rug_pull_detection(job, dex='univ2'):
    out_pool_label_path = os.path.join(eval('path.{}_processed_path'.format(dex)), "1_pair_pool_labels.csv")
    out_scammer_path = os.path.join(eval('path.{}_processed_path'.format(dex)), "1_pair_scammers.csv")
    processed_pools = []
    if os.path.exists(out_pool_label_path):
        processed_pools = pd.read_csv(out_pool_label_path)["pool"].values
    event_path = eval('path.{}_pool_events_path'.format(dex))
    contract_event_collector = ContractEventCollector()

    pool_path = os.path.join(eval('path.{}_pool_path'.format(dex)), "pool_addresses.csv")
    pool_addresses = pd.read_csv(pool_path)["pool"].values

    pool_infos = pd.read_csv(os.path.join(eval('path.{}_pool_path'.format(dex)), "pool_info.csv"), low_memory=False)
    pool_infos.drop_duplicates(inplace=True)

    pool_creations = pd.read_csv(os.path.join(eval('path.{}_pool_path'.format(dex)), "pool_creation_info.csv"))
    pool_creations.drop_duplicates(inplace=True)

    token_infos = pd.read_csv(os.path.join(eval('path.{}_token_path'.format(dex)), "token_info.csv"), low_memory=False)
    token_infos.drop_duplicates(inplace=True)

    token_creations = pd.read_csv(os.path.join(eval('path.{}_token_path'.format(dex)), "token_creation_info.csv"))
    token_creations.drop_duplicates(inplace=True)

    chunks = ut.partitioning(0, len(pool_addresses), 20000)
    print("NUM CHUNKS", len(chunks), "JOB", job)
    chunk = chunks[job]
    chunk_addresses = pool_addresses[chunk["from"]:(chunk["to"] + 1)]
    pool_labels = []
    for pool_address in tqdm(chunk_addresses):
        if pool_address in processed_pools:
            continue
        scam_token = None
        pool_info = pool_infos[pool_infos["pool"] == pool_address]
        token0 = pool_info["token0"].values[0]
        token1 = pool_info["token1"].values[0]
        weth_position = 0 if token0 == Constant.WETH else 1
        pool_label, num_pool_swap, num_pool_mint, num_pool_burn, num_pool_transfer = 0, 0, 0, 0, 0,
        pool_transfers = contract_event_collector.get_event(pool_address, "Transfer", event_path, dex)
        pool_swaps = contract_event_collector.get_event(pool_address, "Swap", event_path, dex)
        pool_burns = contract_event_collector.get_event(pool_address, "Burn", event_path, dex)
        pool_mints = contract_event_collector.get_event(pool_address, "Mint", event_path, dex)
        num_pool_burn = len(pool_burns)
        num_pool_transfer = len(pool_transfers)
        num_pool_swap = len(pool_swaps)
        num_pool_mint = len(pool_mints)
        pool_creation = pool_creations[(pool_creations["contractAddress"] == pool_address) | (pool_creations["contractAddress"] == pool_address.lower())]
        pool_creator = pool_creation["contractCreator"].values[0]
        # ts_pools = np.array([e["timeStamp"] for e in pool_transfers + pool_swaps + pool_burns + pool_mints])
        #  pool_life_time = 0
        # if len(ts_pools) > 1:
        #     first_event_ts = np.min(ts_pools)
        #     last_event_ts = np.max(ts_pools)
        #     pool_life_time = last_event_ts - first_event_ts

        # only consider WETH-paired pool
        if token0 == Constant.WETH or token1 == Constant.WETH:
            low_value_token = eval(f"token{1 - weth_position}")
            num_pairs = pool_infos[(pool_infos["token0"].str.lower() == low_value_token.lower()) | (pool_infos["token1"].str.lower() == low_value_token.lower())]["pool"].count()
            # check of token live 1 day and has 1 pair only to avoid the case of migration.(https://etherscan.io/address/0xa86B8938ed9017693c5883e1b20741b8f735Bf2b#tokentxns)
            # 1 day token will miss the case of https://etherscan.io/address/0x8927E6432a75F98C664863500537afB7970936d9#events
            # if num_pairs == 1 and is_1d_token(low_value_token, dex):
            if num_pairs == 1:
                check_result, sell_scammers = is_1d_rug_pull(pool_transfers, pool_mints, pool_burns, pool_swaps, weth_position)
                if check_result != 0:
                    scam_token = low_value_token
                    # check if scam token is 1 day token
                    pool_label = check_result
                    token_creation = token_creations[(token_creations["contractAddress"] == scam_token) | (token_creations["contractAddress"] == scam_token.lower())]
                    token_creator = token_creation["contractCreator"].values[0]
                    scammers = [pool_creator, token_creator]
                    if sell_scammers is not None:
                        scammers.extend(sell_scammers)
                    if len(scammers) > 0:
                        scammers = set([str(Web3.to_checksum_address(s)) for s in scammers])
                        scammers = scammers - set(Constant.SPECIAL_ADDRESS)
                        scammer_dict = [{"pool": pool_address, "scammer": s} for s in scammers]
                        ut.save_or_append_if_exist(scammer_dict, out_scammer_path)

        pool_labels.append({"pool": pool_address,
                            "creator": pool_creator,
                            "is_rp": pool_label,
                            "token0": token0,
                            "token1": token1,
                            "scam_token": scam_token,
                            "swap": num_pool_swap,
                            "mint": num_pool_mint,
                            "burn": num_pool_burn,
                            "transfer": num_pool_transfer,
                            # "life_time": pool_life_time
                            })
        if len(pool_labels) >= 10:
            print(pool_labels)
            ut.save_or_append_if_exist(pool_labels, out_pool_label_path)
            pool_labels = []
    if len(pool_labels) > 0:
        ut.save_or_append_if_exist(pool_labels, out_pool_label_path)

def filter_1day_token_rp(dex='univ2'):
    contract_event_collector = ContractEventCollector()
    event_path = eval('path.{}_token_events_path'.format(dex))
    pool_label_path = os.path.join(eval('path.{}_processed_path'.format(dex)), "pool_labels.csv")
    scammer_path = os.path.join(eval('path.{}_processed_path'.format(dex)), "scammers.csv")
    out_1_d_rp_path = os.path.join(eval('path.{}_processed_path'.format(dex)), "1d_rp.csv")
    out_1_d_rp_scammer_path = os.path.join(eval('path.{}_processed_path'.format(dex)), "1d_rp_scammers.csv")
    scammers = pd.read_csv(scammer_path)
    pool_labels = pd.read_csv(pool_label_path)
    pool_labels.fillna("", inplace=True)
    rp_pools = pool_labels[pool_labels["is_rp"] != '0']
    rp_pools["pool"] = rp_pools["pool"].str.lower()
    data = []
    one_d_rp_addresses = []
    for idx, row in tqdm(rp_pools.iterrows()):
        scam_token = row["scam_token"]
        pool = row["pool"]
        if scam_token == "":
            continue
        transfers = contract_event_collector.get_event(scam_token, "Transfer", event_path, dex)
        token_life_time = 0
        ts = np.array([e["timeStamp"] for e in transfers])
        if len(ts) > 0:
            first_event_ts = np.min(ts)
            last_event_ts = np.max(ts)
            token_life_time = last_event_ts - first_event_ts
        if token_life_time <= Constant.ONE_DAY_TIMESTAMP:
            one_d_rp_addresses.append(pool.lower())
            record = row.to_dict()
            record["token_life_time"] = token_life_time
            record["token_transfer"] = len(transfers)
            print(record)
            data.append(record)
    ut.save_overwrite_if_exist(data, out_1_d_rp_path)
    one_d_rp_scammer = scammers[scammers["pool"].str.lower().isin(one_d_rp_addresses)]
    one_d_rp_scammer.to_csv(out_1_d_rp_scammer_path, index=False)

def debug_1_day_rp(pool_address, dex='univ2'):
    pool_infos = pd.read_csv(os.path.join(eval('path.{}_pool_path'.format(dex)), "pool_info.csv"), low_memory=False)
    pool_infos.drop_duplicates(inplace=True)
    pool_info = pool_infos[pool_infos["pool"] == pool_address]
    token0 = pool_info["token0"].values[0]
    token1 = pool_info["token1"].values[0]
    event_path = eval('path.{}_pool_events_path'.format(dex))
    contract_event_collector = ContractEventCollector()
    if token0 != Constant.WETH and token1 != Constant.WETH:
        print("BOTH TOKENS ARE NOT WETH")
        return
    pool_transfers = contract_event_collector.get_event(pool_address, "Transfer", event_path, dex)
    pool_swaps = contract_event_collector.get_event(pool_address, "Swap", event_path, dex)
    pool_burns = contract_event_collector.get_event(pool_address, "Burn", event_path, dex)
    pool_mints = contract_event_collector.get_event(pool_address, "Mint", event_path, dex)
    ts_pools = np.array([e["timeStamp"] for e in pool_transfers + pool_swaps + pool_burns + pool_mints])
    pool_life_time = 0
    if len(ts_pools) > 1:
        first_event_ts = np.min(ts_pools)
        last_event_ts = np.max(ts_pools)
        pool_life_time = last_event_ts - first_event_ts
    if pool_life_time > Constant.ONE_DAY_TIMESTAMP:
        print("POOL LIVE LONGER THAN ONE DAY")
        return
    weth_position = 0 if token0 == Constant.WETH else 1
    pool_label, sell_scammers = is_1d_rug_pull(pool_transfers, pool_mints, pool_burns, pool_swaps, weth_position)

    print("Is rug pull:", pool_label)


if __name__ == '__main__':
    rug_pull_detection(17)
    # filter_1day_token_rp()
