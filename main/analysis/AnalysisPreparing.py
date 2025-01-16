import os
import pandas as pd
import requests
from tqdm import tqdm
from hexbytes import HexBytes
from web3 import Web3

from api import EtherscanAPI, BSCscanAPI
from utils.ProjectPath import ProjectPath
from utils import Utils as ut
from utils.Settings import Setting
from data_collection.EventCollector import ContractEventCollector
from sql.DataQuerier import DataQuerier


# GLOBAL VARS
setting = Setting()
path = ProjectPath()


uni_sql_querier = DataQuerier("univ2")
pan_sql_querier = DataQuerier("panv2")

explorer_api = {
    "univ2": {"explorer": EtherscanAPI, "keys": setting.ETHERSCAN_API_KEYS},
    "panv2": {"explorer": BSCscanAPI, "keys": setting.BSCSCAN_API_KEYS},
}

infura_api = {
    "univ2": {"node_url": setting.INFURA_ETH_NODE_URL, "num_pairs": setting.UNIV2_NUM_OF_PAIRS, "factory_abi": setting.UNIV2_FACTORY_ABI, "factory_address": setting.UNIV2_FACTORY_ADDRESS,
              "pool_abi": setting.UNIV2_POOL_ABI, "token_abi": setting.ETH_TOKEN_ABI},
    "panv2": {"node_url": setting.INFURA_BSC_NODE_URL, "num_pairs": setting.PANV2_NUM_OF_PAIRS, "factory_abi": setting.PANV2_FACTORY_ABI, "factory_address": setting.PANV2_FACTORY_ADDRESS,
              "pool_abi": setting.PANV2_POOL_ABI, "token_abi": setting.BSC_TOKEN_ABI},
}

def load_pool(pool_address, contract_event_collector, pool_event_path, dex):
        transfer_list = contract_event_collector.get_event(
            pool_address, "Transfer", pool_event_path, dex
        )
        swaps_list = contract_event_collector.get_event(
            pool_address, "Swap", pool_event_path, dex
        )
        burns_list = contract_event_collector.get_event(
            pool_address, "Burn", pool_event_path, dex
        )
        mint_list = contract_event_collector.get_event(
            pool_address, "Mint", pool_event_path, dex
        )
        pool = {"pool_address": pool_address, "Mint": len(mint_list), "Burn": len(burns_list), "Transfer": len(transfer_list), "Swap": len(swaps_list) }
        transfer_ts =  [{"address": pool_address, "timestamp": e["timeStamp"]} for e  in transfer_list]
        swap_ts =  [{"address": pool_address, "timestamp": e["timeStamp"]} for e  in swaps_list]
        burn_ts =  [{"address": pool_address, "timestamp": e["timeStamp"]} for e  in burns_list]
        mint_ts =  [{"address": pool_address, "timestamp": e["timeStamp"]} for e  in mint_list]
        return pool, transfer_ts, swap_ts, burn_ts, mint_ts

def pool_with_washtrader_and_real_victims(dex="univ2"):
    querier =  uni_sql_querier if dex == "univ2" else pan_sql_querier


def load_pools(addresses, dex):
    pool_event_path = eval("path.{}_pool_events_path".format(dex))
    contract_event_collector = ContractEventCollector()
    for address in tqdm(addresses):
        pool, transfer_ts, swap_ts, burn_ts, mint_ts =  load_pool(address, contract_event_collector, pool_event_path, dex)
        ut.save_or_append_if_exist(transfer_ts, os.path.join("data", f"{dex}_transfer_ts_stats.csv"))
        ut.save_or_append_if_exist(burn_ts, os.path.join("data", f"{dex}_burn_ts_stats.csv"))
        ut.save_or_append_if_exist(mint_ts, os.path.join("data", f"{dex}_mint_ts_stats.csv"))
        ut.save_or_append_if_exist(swap_ts, os.path.join("data", f"{dex}_swap_ts_stats.csv"))
        ut.save_or_append_if_exist([pool], os.path.join("data", f"{dex}_events_stats.csv"))

def create_pool_statistic_data(job, size, dex):
    pool_path = os.path.join(eval('path.{}_processed_path'.format(dex)), "pool_addresses.csv")
    pools = pd.read_csv(pool_path)["pool"].values
    chunks = ut.partitioning(0, len(pools), int(len(pools) /size))
    chunk = chunks[job]
    addresses = pools[chunk["from"]:(chunk["to"] + 1)]
    print(f"DOWNLOAD ALL POOL EVENTS FROM {chunk['from']} TO {chunk['to']} (JOB {job})")
    load_pools(addresses, dex)


def rpc_single_call(url, key, method, params):
    """
    Infura JSON RPC calling
    :param method: JSON-RPC method name (see: https://docs.infura.io/infura/networks/ethereum/json-rpc-methods)
    :param params: method's params
    :return:
    """
    base_url = url + key
    data = {"jsonrpc": "2.0", "method": method, "params": params, "id": 1}
    response = requests.post(base_url, headers={"Content-Type": "application/json"}, json=data)
    return response.json()

def eth_getBlockByNumber(block, url, key):
    response = rpc_single_call(url, key, "eth_getBlockByNumber", [hex(block), False])
    return response



def fulfill_block_number_for_pool(job, dex):
    result = []
    api = explorer_api[dex]["explorer"]
    keys = explorer_api[dex]["keys"]
    key = keys[job % len(keys)]

    infura_key = setting.INFURA_API_KEYS[job % len(setting.INFURA_API_KEYS)]
    infura_node_url = infura_api[dex]["node_url"]

    pool_creation_path = os.path.join(eval('path.{}_processed_path'.format(dex)), "pool_creation_info.csv")
    scam_pool_path = os.path.join(eval('path.{}_processed_path'.format(dex)), "filtered_simple_rp_pool.csv")
    output_path = os.path.join("data", f"{dex}_scam_pool_creation_with_block_number.csv")
    pool_creation = pd.read_csv(pool_creation_path)
    scam_pools = pd.read_csv(scam_pool_path)["pool"].str.lower().values
    print("SCAM:", len(scam_pools))
    pool_creation= pool_creation[pool_creation["contractAddress"].str.lower().isin(scam_pools)]
    print("CREATION:", len(pool_creation))

    pool_creation.drop_duplicates(inplace=True)
    chunks = ut.partitioning(0, len(pool_creation), int(len(pool_creation) / len(keys)))
    chunk = chunks[job]
    addresses = pool_creation.iloc[chunk["from"]:(chunk["to"] + 1), :]
    print(f"DOWNLOAD ALL POOL EVENTS FROM {chunk['from']} TO {chunk['to']} (JOB {job}/{len(chunks)})")
    processed_addresses = []
    if os.path.isfile(output_path):
        processed_addresses = list(pd.read_csv(output_path, low_memory=False)["pool"].str.lower().values)
    print("Processed addresses: ", len(processed_addresses))
    for idx, row in tqdm(addresses.iterrows()):
        if row["contractAddress"].lower() in processed_addresses:
            print("SKIP", row["contractAddress"].lower())
            continue
        creation_tx = row["txHash"].lower()
        blockNumber = ut.hex_to_dec(api.get_tx_by_hash(creation_tx, key)["blockNumber"])
        timestamp = ut.hex_to_dec(eth_getBlockByNumber(blockNumber, infura_node_url, infura_key)["result"]["timestamp"])
        data = {
            "pool": row["contractAddress"],
            "creator": row["contractCreator"],
            "creation_tx": creation_tx,
            "block_number": blockNumber,
            "timestamp": timestamp
        }
        print(data)
        result.append(data)
        if len(result) >= 10:
            save_df = pd.DataFrame.from_records(result)
            if os.path.isfile(output_path):
                print("APPEND ", len(result), "RECORDS")
                save_df.to_csv(output_path, mode='a', header=False, index=False)
            else:
                print("SAVE", len(result), "RECORDS")
                save_df.to_csv(output_path, index=False)
            result = []
    if len(result) > 0:
        save_df = pd.DataFrame.from_records(result)
        if os.path.isfile(output_path):
            print("APPEND ", len(result), "RECORDS")
            save_df.to_csv(output_path, mode='a', header=False, index=False)
        else:
            print("SAVE", len(result), "RECORDS")
            save_df.to_csv(output_path, index=False)
    print("DONE")

if __name__ == '__main__':
    job = 23
    dex = 'panv2'
    fulfill_block_number_for_pool(job, dex)