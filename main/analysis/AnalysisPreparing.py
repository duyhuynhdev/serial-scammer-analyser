import os
import pandas as pd
from tqdm import tqdm
from hexbytes import HexBytes
from api import EtherscanAPI, BSCscanAPI
from utils.ProjectPath import ProjectPath
from utils import Utils as ut
from utils.Settings import Setting
from data_collection.EventCollector import ContractEventCollector



# GLOBAL VARS
setting = Setting()
path = ProjectPath()


explorer_api = {
    "univ2": {"explorer": EtherscanAPI, "keys": setting.ETHERSCAN_API_KEYS},
    "panv2": {"explorer": BSCscanAPI, "keys": setting.BSCSCAN_API_KEYS},
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

def fulfill_block_number_for_pool(job, size, dex):
    result = []
    api = explorer_api[dex]["explorer"]
    keys = explorer_api[dex]["keys"]
    key = keys[job % len(keys)]
    pool_creation_path = os.path.join(eval('path.{}_processed_path'.format(dex)), "pool_creation_info.csv")
    output_path = os.path.join("data", "pool_creation_with_block_number.csv")
    pool_creation = pd.read_csv(pool_creation_path)
    pool_creation.drop_duplicates(inplace=True)
    chunks = ut.partitioning(0, len(pool_creation), int(len(pool_creation) / size))
    chunk = chunks[job]
    addresses = pool_creation.iloc[chunk["from"]:(chunk["to"] + 1), :]
    print(f"DOWNLOAD ALL POOL EVENTS FROM {chunk['from']} TO {chunk['to']} (JOB {job})")
    processed_addresses = []
    if os.path.isfile(output_path):
        processed_addresses = list(pd.read_csv(output_path, low_memory=False)["pool"].str.lower().values)
    print("Processed addresses: ", len(processed_addresses))
    for idx, row in tqdm(addresses.iterrows()):
        if row["contractAddress"].lower() in processed_addresses:
            print("SKIP", row["contractAddress"].lower())
            continue
        creation_tx = row["txHash"].lower()
        blockNumber = api.get_tx_by_hash(creation_tx, key)["blockNumber"]
        data = {
            "pool": row["contractAddress"],
            "creator": row["contractCreator"],
            "creation_tx": creation_tx,
            "block_number": ut.hex_to_dec(blockNumber)
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
    job = 20
    dex = 'univ2'
    fulfill_block_number_for_pool(job, 20, dex)