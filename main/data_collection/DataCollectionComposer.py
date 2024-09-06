import json
import math

from numpy.ma.core import argmin

import utils.Utils as ut
from tqdm import tqdm
import os
import pandas as pd

from data_collection.AccountCollector import TransactionCollector
from utils.Settings import Setting
from utils.Path import Path
from api import EtherscanAPI, BSCscanAPI
from EventCollector import PoolEventCollector
from ContractCollector import PoolInfoCollector, TokenInfoCollector
from AccountCollector import CreatorCollector
import numpy as np

path = Path()
setting = Setting()

key_index = 0

explorer_api = {
    "univ2": {"explorer": EtherscanAPI, "keys": setting.ETHERSCAN_API_KEYS},
    "panv2": {"explorer": BSCscanAPI, "keys": setting.BSCSCAN_API_KEYS},
}
router_address = "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D"  # V2 ROUTER
router2_address = "0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45"  # V3 ROUTER
dead_address = "0x000000000000000000000000000000000000dEaD"
zero_address = "0x0000000000000000000000000000000000000000"
burn_address_1 = "0x0000000000000000000000000000000000000001"
burn_address_2 = "0x0000000000000000000000000000000000000002"
special_addresses = [router_address, dead_address, zero_address, burn_address_1, burn_address_2]
burn_addresses = [dead_address, zero_address, burn_address_1, burn_address_2]
pool_decimals = 18


def extract_1d_events(events, start_ts):
    od_from_start = start_ts + (24 * 3600)
    return [e for e in events if int(e["timeStamp"]) <= od_from_start]


def is_mint_transfer(transfer):
    return transfer["from"] == zero_address and transfer["to"] != zero_address


def is_burn_transfer(transfer):
    return transfer["from"] != zero_address and transfer["to"] in burn_addresses


def is_simple_rug_pull(transfers):
    mints = []
    burns = []
    for transfer in transfers:
        if is_mint_transfer(transfer):
            mints.append(transfer)
        if is_burn_transfer(transfer):
            burns.append(transfer)
    if len(mints) != 1 or len(burns) != 1:
        return False
    minted_liq_tokens = float(mints[0]["value"]) / 10 ** pool_decimals
    burned_liq_tokens = float(burns[0]["value"]) / 10 ** pool_decimals
    return (burned_liq_tokens / minted_liq_tokens) >= 0.99


def get_balance_of_high_value_token_before_sell_rug(mints, burns, swaps, hv_token_position, hv_token_decimals, max_swap_idx):
    total_mint_amount, total_burn_amount, total_swap_in, total_swap_out = 0, 0, 0, 0
    swaps_df = pd.DataFrame(swaps)
    max_swap_time_stamp = swaps_df["timeStamp"].tolist()[max_swap_idx]
    for mint in mints:
        if int(mint["timeStamp"]) <= max_swap_time_stamp:
            total_mint_amount += mint[f"amount{hv_token_position}"] / 10 ** hv_token_decimals
    for burn in burns:
        if int(burn["timeStamp"]) <= max_swap_time_stamp:
            total_burn_amount += burn[f"amount{hv_token_position}"] / 10 ** hv_token_decimals
    swap_out_amounts = swaps_df[f"amount{hv_token_position}Out"].astype(float) / 10 ** hv_token_decimals
    swap_in_amounts = swaps_df[f"amount{hv_token_position}In"].astype(float) / 10 ** hv_token_decimals
    swap_out_amounts = swap_out_amounts[:max_swap_idx]
    swap_in_amounts = swap_in_amounts[:max_swap_idx]
    total_swap_in = np.sum(swap_in_amounts)
    total_swap_out = np.sum(swap_out_amounts)
    return total_mint_amount + total_swap_in - total_burn_amount - total_swap_out


def is_sell_rug_pull(mints, burns, swaps, hv_token_position, hv_token_decimals):
    # TODO value token is token that has lower reserve --> will check later by scanning popular tokens,
    # calculate balance of high value token before 99% swap event
    if len(swaps) == 0:
        return False,[]
    swaps_df = pd.DataFrame(swaps)
    hv_swap_amounts = swaps_df[f"amount{hv_token_position}Out"].astype(float) / 10 ** hv_token_decimals
    max_swap_amount = np.max(hv_swap_amounts)
    max_swap_idx = np.argmax(hv_swap_amounts)
    hv_balance = get_balance_of_high_value_token_before_sell_rug(mints, burns, swaps, hv_token_position, hv_token_decimals, max_swap_idx)
    print("High value token balance before max swap:", hv_balance)
    print("Max swap amount", max_swap_amount)
    if max_swap_amount >= 0.99 * hv_balance:
        return True, [swaps_df["to"].tolist()[max_swap_idx], swaps_df["sender"].tolist()[max_swap_idx]]
    return False, []


def is_1d_rug_pull(transfers, mints, burns, swaps, token0_info, token1_info):
    if len(mints) < 1:
        return False, [], ""
    first_mint = mints[0]
    mints = extract_1d_events(mints,  int(first_mint["timeStamp"]))
    transfers = extract_1d_events(transfers, int(first_mint["timeStamp"]))
    swaps = extract_1d_events(swaps, int(first_mint["timeStamp"]))
    burns = extract_1d_events(burns, int(first_mint["timeStamp"]))
    token_0_decimals = int(token0_info["decimals"])
    token_1_decimals = int(token1_info["decimals"])
    lv_token_position = 0
    if (first_mint["amount0"]/ 10**token_0_decimals) < (first_mint["amount1"]/10**token_1_decimals):
        lv_token_position = 1
    hv_token_position = 1 - lv_token_position
    hv_token_decimals = int(eval(f'token{hv_token_position}_info["decimals"]'))
    if is_simple_rug_pull(transfers):
        return 1, [], eval(f"token{lv_token_position}_info")
    result, sell_scammers = is_sell_rug_pull(mints, burns, swaps, hv_token_position, hv_token_decimals)
    if result:
        return 2, sell_scammers, eval(f"token{lv_token_position}_info")
    return 0, [], eval(f"token{lv_token_position}_info")


def data_collection(pool_addresses=None, dex='univ2'):
    event_path = eval('path.{}_pool_events_path'.format(dex))
    if pool_addresses is None:
        pool_path = os.path.join(eval('path.{}_pool_path'.format(dex)), "pool_addresses.csv")
        pool_addresses = pd.read_csv(pool_path)["pool"].values
    pool_event_collector = PoolEventCollector()
    pool_info_collector = PoolInfoCollector()
    token_info_collector = TokenInfoCollector()
    creator_collector = CreatorCollector()
    transaction_collector = TransactionCollector()
    pool_labels = []
    for pool_address in tqdm(pool_addresses):
        pool_info = pool_info_collector.get_pool_info(pool_address, dex=dex)
        token0 = pool_info["token0"]
        token1 = pool_info["token1"]
        if token0 == "" or token1 == "":  # If data fails to retrieve, silently continue
            continue

        token0_info = token_info_collector.get_token_info(token0, dex=dex)
        token1_info = token_info_collector.get_token_info(token1, dex=dex)
        swaps = pool_event_collector.get_event(pool_address, "Swap", event_path, dex)
        burns = pool_event_collector.get_event(pool_address, "Burn", event_path, dex)
        mints = pool_event_collector.get_event(pool_address, "Mint", event_path, dex)
        transfers = pool_event_collector.get_event(pool_address, "Transfer", event_path, dex)
        is_rp, sell_scammers, rp_token_info = is_1d_rug_pull(transfers, mints, burns, swaps, token0_info, token1_info)
        pool_labels.append({"address": pool_address, "is_rp": is_rp})
        scammers = []
        is_rp = 0  # TODO - Remove
        if is_rp >= 1:
            # print(scammers)
            _scammers = creator_collector.get_pool_creator(pool_address, dex)
            # print(_scammers)
            scammers.append(_scammers["contractAddress"])
            scammers.append(_scammers["contractCreator"])
            # print(scammers)
            scammers.extend(pd.DataFrame(mints)["sender"].tolist())
            scammers.extend(pd.DataFrame(burns)["sender"].tolist())
            scammers.extend(pd.DataFrame(burns)["to"].tolist())
            scammers.extend(pd.DataFrame(transfers)["from"].tolist())
            scammers.extend(pd.DataFrame(transfers)["to"].tolist())
            # print(scammers)
            if is_rp == 2:
                scammers.extend(sell_scammers)
                # print(scammers)
            print(scammers)
            scammers = set(scammers) - set(special_addresses)
        if len(scammers) > 0:
            for scammer in scammers:
                transaction_collector.download_transactions(scammer, dex)
        if len(pool_labels) >= 10:
            ut.save_or_append_if_exist(pool_labels, os.path.join(eval('path.{}_pool_path'.format(dex)), "pool_labels.csv"))
            pool_labels = []
    if len(pool_labels) > 0:
        ut.save_or_append_if_exist(pool_labels, os.path.join(eval('path.{}_pool_path'.format(dex)), "pool_labels.csv"))


def test_rug_pull(pool_address, dex='univ2'):
    pool_info_collector = PoolInfoCollector()
    token_info_collector = TokenInfoCollector()
    pool_event_collector = PoolEventCollector()

    pool_info = pool_info_collector.get_pool_info(pool_address)
    token0 = pool_info["token0"]
    token1 = pool_info["token1"]
    token0_info = token_info_collector.get_token_info(token0)
    token1_info = token_info_collector.get_token_info(token1)
    event_path = eval('path.{}_pool_events_path'.format(dex))
    burns = pool_event_collector.get_event(pool_address, "Burn", event_path, dex)
    swaps = pool_event_collector.get_event(pool_address, "Swap", event_path, dex)
    mints = pool_event_collector.get_event(pool_address, "Mint", event_path, dex)

    transfers = pool_event_collector.get_event(pool_address, "Transfer", event_path, dex)
    return is_1d_rug_pull(transfers, mints, burns, swaps, token0_info, token1_info)


if __name__ == '__main__':
    data_collection(dex='panv2')
    # print(test_rug_pull(pool_address='0xA886F2c269Be3a8C04511C6c23c95E09bB459949'))
    # print(test_rug_pull(pool_address='0xF0A3C6787Ff0c6d912060fa156E2Fd925974f93F'))
    # print(test_rug_pull(pool_address='0xB6909B960DbbE7392D405429eB2b3649752b4838'))
