import utils.Utils as ut
from tqdm import tqdm
import os
import pandas as pd

from data_collection.AccountCollector import TransactionCollector
from utils.Settings import Setting
from utils.Path import Path
from api import EtherscanAPI, BSCscanAPI
from data_collection.EventCollector import ContractEventCollector
from data_collection.ContractCollector import PoolInfoCollector, TokenInfoCollector
from data_collection.AccountCollector import CreatorCollector
import numpy as np
from web3 import Web3

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
WETH = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
ONE_DAY_TS = 24 * 3600


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


def is_sell_rug_pull(mints, burns, swaps, weth_position):
    # value token is token that has lower reserve --> will check later by scanning popular tokens,
    # calculate balance of high value token before 99% swap event
    if len(swaps) == 0:
        return False, []
    swaps_df = pd.DataFrame(swaps)
    hv_swap_amounts = swaps_df[f"amount{weth_position}Out"].astype(float) / 10 ** 18
    max_swap_amount = np.max(hv_swap_amounts)
    max_swap_idx = np.argmax(hv_swap_amounts)
    weth_balance = get_balance_of_weth_before_sell_rug(mints, burns, swaps, weth_position, max_swap_idx)
    # print("High value token balance before max swap:", hv_balance)
    # print("Max swap amount", max_swap_amount)
    if max_swap_amount >= 0.99 * weth_balance:
        return True, [swaps_df["to"].tolist()[max_swap_idx], swaps_df["sender"].tolist()[max_swap_idx]]
    return False, []


def is_1d_rug_pull(transfers, mints, burns, swaps, weth_position):
    if len(mints) < 1:
        return False, []
    if is_simple_rug_pull(transfers):
        return 1, []
    result, sell_scammers = is_sell_rug_pull(mints, burns, swaps, weth_position)
    if result:
        return 2, sell_scammers
    return 0, []


def rug_pull_detection(dex='univ2'):
    out_pool_label_path = os.path.join(eval('path.{}_processed_path'.format(dex)), "pool_labels.csv")
    out_scammer_path = os.path.join(eval('path.{}_processed_path'.format(dex)), "scammers.csv")
    processed_pools = []
    if os.path.exists(out_pool_label_path):
        processed_pools = pd.read_csv(out_pool_label_path)["pool"].values
    event_path = eval('path.{}_pool_events_path'.format(dex))
    pool_event_collector = ContractEventCollector()

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

    pool_labels = []
    for pool_address in tqdm(pool_addresses):
        if pool_address in processed_pools:
            continue
        pool_label = 0
        scam_token = None
        pool_info = pool_infos[pool_infos["pool"] == pool_address]
        token0 = pool_info["token0"].values[0]
        token1 = pool_info["token1"].values[0]
        if token0 == WETH or token1 == WETH:
            weth_position = 0 if token0 == WETH else 1
            transfers = pool_event_collector.get_event(pool_address, "Transfer", event_path, dex)
            swaps = pool_event_collector.get_event(pool_address, "Swap", event_path, dex)
            burns = pool_event_collector.get_event(pool_address, "Burn", event_path, dex)
            mints = pool_event_collector.get_event(pool_address, "Mint", event_path, dex)
            ts_pools =  np.array([e["timeStamp"] for e in transfers + swaps + burns + mints])
            if len(ts_pools) > 1:
                first_event_ts = np.min(ts_pools)
                last_event_ts = np.max(ts_pools)
                pool_life_time = last_event_ts - first_event_ts
                if pool_life_time <= ONE_DAY_TS:
                    pool_label, sell_scammers = is_1d_rug_pull(transfers, mints, burns, swaps, weth_position)
                    if pool_label != 0:
                        scam_token =  eval(f"token{1-weth_position}")
                        pool_creation = pool_creations[(pool_creations["contractAddress"] == pool_address) | (pool_creations["contractAddress"] == pool_address.lower())]
                        pool_creator = pool_creation["contractCreator"].values[0]
                        token_creation = token_creations[(token_creations["contractAddress"] == scam_token) | (token_creations["contractAddress"] == scam_token.lower())]
                        token_creator = token_creation["contractCreator"].values[0]
                        scammers = [pool_creator, token_creator]
                        if len(mints) > 0:
                            scammers.extend(pd.DataFrame(mints)["sender"].tolist())
                        if len(burns) > 0:
                            scammers.extend(pd.DataFrame(burns)["sender"].tolist())
                            scammers.extend(pd.DataFrame(burns)["to"].tolist())
                        if len(transfers) > 0:
                            scammers.extend(pd.DataFrame(transfers)["from"].tolist())
                            scammers.extend(pd.DataFrame(transfers)["to"].tolist())
                        if sell_scammers is not None:
                            scammers.extend(sell_scammers)
                        if len(scammers) > 0:
                            scammers = set([str(Web3.to_checksum_address(s)) for s in scammers])
                            scammers = scammers - set(special_addresses)
                            scammer_dict = [{"pool": pool_address, "scammer": s} for s in scammers]
                            ut.save_or_append_if_exist(scammer_dict, out_scammer_path)
        pool_labels.append({"pool": pool_address, "is_rp": pool_label, "scam_token": scam_token})
        if len(pool_labels) >= 10:
            print(pool_labels)
            ut.save_or_append_if_exist(pool_labels, out_pool_label_path)
            pool_labels = []
    if len(pool_labels) > 0:
        ut.save_or_append_if_exist(pool_labels, out_pool_label_path)


if __name__ == '__main__':
    rug_pull_detection()