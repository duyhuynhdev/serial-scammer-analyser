import sys
import os
sys.path.append(os.path.join(os.path.dirname(sys.path[0])))

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

path = Path()
setting = Setting()

key_index = 0

explorer_api = {
    "univ2": {"explorer": EtherscanAPI, "keys": setting.ETHERSCAN_API_KEYS},
    "panv2": {"explorer": BSCscanAPI, "keys": setting.BSCSCAN_API_KEYS},
}

contract_event_collector = ContractEventCollector()


def get_balance_of_weth_before_sell_rug(mints, burns, swaps, weth_position, max_swap_idx):
    total_mint_amount, total_burn_amount, total_swap_in, total_swap_out = 0, 0, 0, 0
    swaps_df = pd.DataFrame(swaps)
    max_swap_time_stamp = swaps_df["timeStamp"].tolist()[max_swap_idx]
    for mint in mints:
        if int(mint["timeStamp"]) <= max_swap_time_stamp:
            total_mint_amount += mint[f"amount{weth_position}"] / 10 ** Constant.WETH_BNB_DECIMALS
    for burn in burns:
        if int(burn["timeStamp"]) <= max_swap_time_stamp:
            total_burn_amount += burn[f"amount{weth_position}"] / 10 ** Constant.WETH_BNB_DECIMALS
    swap_out_amounts = swaps_df[f"amount{weth_position}Out"].astype(float) / 10 ** Constant.WETH_BNB_DECIMALS
    swap_in_amounts = swaps_df[f"amount{weth_position}In"].astype(float) / 10 ** Constant.WETH_BNB_DECIMALS
    swap_out_amounts = swap_out_amounts[:max_swap_idx]
    swap_in_amounts = swap_in_amounts[:max_swap_idx]
    total_swap_in = np.sum(swap_in_amounts)
    total_swap_out = np.sum(swap_out_amounts)
    return total_mint_amount + total_swap_in - total_burn_amount - total_swap_out


def is_mint_transfer(transfer):
    return transfer["sender"].lower() == Constant.ZERO and transfer["to"].lower() != Constant.ZERO


def is_burn_transfer(transfer):
    return transfer["sender"].lower() != Constant.ZERO and transfer["to"].lower() in Constant.BURN_ADDRESSES


def is_simple_rug_pull(transfers):
    mints = []
    burns = []
    for transfer in transfers:
        if is_mint_transfer(transfer):
            mints.append(transfer)
        if is_burn_transfer(transfer):
            burns.append(transfer)
    if len(mints) != 1 or len(burns) != 1:
        return False, []
    trading_period = burns[0]["timeStamp"] - mints[0]["timeStamp"]
    if trading_period > Constant.ONE_DAY_TIMESTAMP or trading_period < 0:
        return False, []
    minted_liq_tokens = float(mints[0]["amount"]) / 10 ** Constant.POOL_DECIMALS
    burned_liq_tokens = float(burns[0]["amount"]) / 10 ** Constant.POOL_DECIMALS
    if (burned_liq_tokens / minted_liq_tokens) >= 0.99:
        scammers = {mints[0]["to"], burns[0]["sender"]}
        return True, scammers
    return False, []


def is_sell_rug_pull(transfers, mints, burns, swaps, weth_position):
    # expect 1 mint no burn and multiple swaps
    if len(swaps) == 0 or len(mints) != 1 or len(burns) > 0:
        return False, []
    swaps_df = pd.DataFrame(swaps)
    # find max swap
    hv_swap_amounts = swaps_df[f"amount{weth_position}Out"].astype(float) / 10 ** Constant.WETH_BNB_DECIMALS
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
        return True, scammers
    return False, []


def is_rug_pull(transfers, mints, burns, swaps, weth_position):
    if len(mints) < 1:
        return False, []
    result, scammers = is_simple_rug_pull(transfers)
    if result:
        return 1, scammers
    result, scammers = is_sell_rug_pull(transfers, mints, burns, swaps, weth_position)
    if result:
        return 2, scammers
    return 0, []


def is_1d_token(token_transfers):
    token_life_time = 0
    ts = np.array([e["timeStamp"] for e in token_transfers])
    if len(ts) > 1:
        first_event_ts = np.min(ts)
        last_event_ts = np.max(ts)
        token_life_time = last_event_ts - first_event_ts
    return token_life_time <= Constant.ONE_DAY_TIMESTAMP


def is_1d_pool(pool_transfers, pool_swaps, pool_burns, pool_mints):
    ts_pools = np.array([e["timeStamp"] for e in pool_transfers + pool_swaps + pool_burns + pool_mints])
    pool_life_time = 0
    if len(ts_pools) > 1:
        first_event_ts = np.min(ts_pools)
        last_event_ts = np.max(ts_pools)
        pool_life_time = last_event_ts - first_event_ts
    return pool_life_time <= Constant.ONE_DAY_TIMESTAMP

def is_valid_scammer_address(address, pool_address, scam_token_address, idx=0):
    if (address == pool_address.lower() or
        address == scam_token_address.lower() or
        address in Constant.SPECIAL_ADDRESS or
        ut.is_contract_address(address, idx)):
        return False
    return True

def rug_pull_detection(job, dex='univ2'):
    out_pool_label_path = os.path.join(eval('path.{}_processed_path'.format(dex)), "1_pair_pool_labels.csv")
    out_scammer_path = os.path.join(eval('path.{}_processed_path'.format(dex)), "1_pair_scammers.csv")
    processed_pools = []
    if os.path.exists(out_pool_label_path):
        processed_pools = pd.read_csv(out_pool_label_path)["pool"].values
    event_path = eval('path.{}_pool_events_path'.format(dex))

    pool_path = os.path.join(eval('path.{}_processed_path'.format(dex)), "pool_addresses.csv")
    pool_addresses = pd.read_csv(pool_path)["pool"].values

    pool_infos = pd.read_csv(os.path.join(eval('path.{}_processed_path'.format(dex)), "pool_info.csv"), low_memory=False)
    pool_infos.drop_duplicates(inplace=True)

    pool_creations = pd.read_csv(os.path.join(eval('path.{}_processed_path'.format(dex)), "pool_creation_info.csv"))
    pool_creations.drop_duplicates(inplace=True)

    token_infos = pd.read_csv(os.path.join(eval('path.{}_processed_path'.format(dex)), "token_info.csv"), low_memory=False)
    token_infos.drop_duplicates(inplace=True)

    token_creations = pd.read_csv(os.path.join(eval('path.{}_processed_path'.format(dex)), "token_creation_info.csv"))
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
        pool_creation = pool_creations[(pool_creations["contractAddress"] == pool_address) | (pool_creations["contractAddress"] == pool_address.lower())]
        pool_creator = pool_creation["contractCreator"].values[0]
        pool_label = 0
        # only consider WETH-paired pool
        if token0.lower() == Constant.WETH or token1.lower() == Constant.WETH:
            low_value_token = eval(f"token{1 - weth_position}")
            num_pairs = pool_infos[(pool_infos["token0"].str.lower() == low_value_token.lower()) | (pool_infos["token1"].str.lower() == low_value_token.lower())]["pool"].count()
            # check of token live 1 day and has 1 pair only to avoid the case of migration.(https://etherscan.io/address/0xa86B8938ed9017693c5883e1b20741b8f735Bf2b#tokentxns)
            # 1 day token will miss the case of https://etherscan.io/address/0x8927E6432a75F98C664863500537afB7970936d9#events
            # if num_pairs == 1 and is_1d_token(low_value_token, dex):
            if num_pairs == 1:
                pool_transfers = contract_event_collector.get_event(pool_address, "Transfer", event_path, dex)
                pool_swaps = contract_event_collector.get_event(pool_address, "Swap", event_path, dex)
                pool_burns = contract_event_collector.get_event(pool_address, "Burn", event_path, dex)
                pool_mints = contract_event_collector.get_event(pool_address, "Mint", event_path, dex)
                check_result, event_scammers = is_rug_pull(pool_transfers, pool_mints, pool_burns, pool_swaps, weth_position)
                if check_result != 0:
                    scam_token = low_value_token
                    # check if scam token is 1 day token
                    pool_label = check_result
                    token_creation = token_creations[(token_creations["contractAddress"] == scam_token) | (token_creations["contractAddress"] == scam_token.lower())]
                    token_creator = token_creation["contractCreator"].values[0]
                    suspicious = {pool_creator.lower(), token_creator.lower()}
                    if event_scammers is not None:
                        suspicious.update(event_scammers)
                    scammers = [s.lower() for s in suspicious if is_valid_scammer_address(s.lower(), pool_address, scam_token, job)]
                    if len(scammers) > 0:
                        scammer_dict = []
                        for s in set(scammers):
                            scammer_dict.append({"pool": pool_address, "scammer": s})
                        ut.save_or_append_if_exist(scammer_dict, out_scammer_path)
                    else:
                        pool_label = -1
        pool_labels.append({"pool": pool_address,
                            "creator": pool_creator,
                            "is_rp": pool_label,
                            "token0": token0,
                            "token1": token1,
                            "scam_token": scam_token,
                            })
        if len(pool_labels) >= 10:
            print(pool_labels)
            ut.save_or_append_if_exist(pool_labels, out_pool_label_path)
            pool_labels = []
    if len(pool_labels) > 0:
        ut.save_or_append_if_exist(pool_labels, out_pool_label_path)


if __name__ == '__main__':
    #0, 2, 3, 4, 5, 6 7, 8, 9, 10, 11, 12, 13
    #out of quota 1
    rug_pull_detection(1)
