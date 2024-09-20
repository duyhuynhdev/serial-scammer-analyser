import json

import utils.Utils as ut
from tqdm import tqdm
import os
import pandas as pd

from data_collection import DataProcessor
from utils.Settings import Setting
from utils.Path import Path
from api import EtherscanAPI, BSCscanAPI

path = Path()
setting = Setting()

key_index = 0

explorer_api = {
    "univ2": {"explorer": EtherscanAPI, "keys": setting.ETHERSCAN_API_KEYS},
    "panv2": {"explorer": BSCscanAPI, "keys": setting.BSCSCAN_API_KEYS},
}


class PoolEventCollector:
    univ2_last_block = 20606150  # Aug-25-2024 02:17:59 PM +UTC
    panv2_last_block = 41674250  # Aug-25-2024 02:20:03 PM +UTC

    pool_event_info = {
        "Sync": {"signature": "Sync(uint112,uint112)", "inputs": ['reserve0', 'reserve1']},
        "Swap": {"signature": "Swap(address,uint256,uint256,uint256,uint256,address)", "inputs": ['sender', 'amount0In', 'amount1In', 'amount0Out', 'amount1Out', 'to']},
        "Burn": {"signature": "Burn(address,uint256,uint256,address)", "inputs": ['sender', 'amount0', 'amount1', 'to']},
        "Mint": {"signature": "Mint(address,uint256,uint256)", "inputs": ['sender', 'amount0', 'amount1']},
        "Transfer": {"signature": "Transfer(address,address,uint256)", "inputs": ['from', 'to', 'value']},
    }

    def download_event_logs(self, pool_address, last_block, event, dex="univ2", explorer=EtherscanAPI, apikey=setting.ETHERSCAN_API_KEY):
        outpath = os.path.join(eval('path.{}_pool_events_path'.format(dex)), event, pool_address + ".json")
        if os.path.exists(outpath):
            with open(outpath, 'r') as f:
                logs = json.load(f)
                f.close()
            return logs
        event_signature_hash = ut.keccak_hash(self.pool_event_info[event]["signature"])
        logs = explorer.get_event_logs(pool_address, fromBlock=0, toBlock=last_block, topic=event_signature_hash, apikey=apikey)
        with open(outpath, 'w') as wf:
            wf.write(json.dumps(logs, default=lambda x: getattr(x, '__dict__', str(x))))
            wf.close()
        return logs

    def download_pool_event(self, pool_address, event, dex="univ2", explorer=EtherscanAPI, apikey=setting.ETHERSCAN_API_KEY):
        self.download_event_logs(pool_address, eval('self.{}_last_block'.format(dex)), event, explorer=explorer, apikey=apikey, dex=dex)

    def download_all_pool_events(self, pool_addresses, dex="univ2", explorer=EtherscanAPI,
                                 apikey=setting.ETHERSCAN_API_KEY):
        events = ["Burn", "Mint", "Swap", "Transfer", "Sync"]
        for pool_address in tqdm(pool_addresses):
            for event in events:
                self.download_event_logs(pool_address, eval('self.{}_last_block'.format(dex)), event, dex=dex, explorer=explorer,
                                         apikey=apikey)

    def download_download_pool_events_by_patch(self, job, dex="univ2"):
        explorer = explorer_api[dex]["explorer"]
        keys = explorer_api[dex]["keys"]
        pool_path = os.path.join(eval('path.{}_pool_path'.format(dex)), "pool_addresses.csv")
        pools = pd.read_csv(pool_path)["pool"].values
        chunks = ut.partitioning(0, len(pools), int(len(pools) / len(keys)))
        print("Num Keys: ", len(keys))
        chunk = chunks[job]
        chunk_addresses = pools[chunk["from"]:(chunk["to"] + 1)]
        print(f"DOWNLOAD ALL POOL EVENTS FROM {chunk['from']} TO {chunk['to']} WITH KEY {keys[job]}")
        self.download_all_pool_events(chunk_addresses, dex=dex, explorer=explorer, apikey=keys[job])

    def download_pool_events(self, event, dex="univ2", explorer=EtherscanAPI, apikey=setting.ETHERSCAN_API_KEY):
        print("DOWNLOAD EVENT {} WITH KEY {}".format(event, apikey))
        pool_path = os.path.join(eval('path.{}_pool_path'.format(dex)), "pool_addresses.csv")
        pools = pd.read_csv(pool_path)["pool"].values
        for pool in tqdm(pools):
            self.download_event_logs(pool, eval('self.{}_last_block'.format(dex)), event, explorer=explorer, apikey=apikey, dex=dex)

    def parse_event(self, event, event_logs_path):
        decoder = DataProcessor.PoolLogDecoder(event)
        events = []
        with open(event_logs_path, 'r') as f:
            logs = json.load(f)
            f.close()
        for log in logs:
            decoded_event = decoder.decode_event(log)
            events.append(decoded_event)
        return events

    def get_event(self, pool_address, event, event_path, dex):
        global key_index
        event_logs_path = os.path.join(event_path, event, pool_address + ".json")
        if not os.path.exists(event_logs_path):  # if not exist , starts download corresponding event
            while key_index < len(explorer_api[dex]["keys"]):
                try:
                    collector = PoolEventCollector()
                    explorer = explorer_api[dex]["explorer"]
                    api_key = explorer_api[dex]["keys"][key_index]
                    collector.download_pool_event(pool_address, event, dex, explorer=explorer, apikey=api_key)
                    break
                except Exception as e:
                    # try other key if error occurs
                    key_index += 1
                    print(e)
        return self.parse_event(event, event_logs_path)

def clean_fail_data(event, dex="univ2"):
    print("CLEAN EVENT {}".format(event))
    pool_path = os.path.join(eval('path.{}_pool_path'.format(dex)), "pool_addresses.csv")
    pools = pd.read_csv(pool_path)["pool"].values
    fails = []
    log_lists = []
    count = 0
    for pool_address in tqdm(pools):
        outpath = os.path.join(eval('path.{}_pool_events_path'.format(dex)), event, pool_address + ".json")
        if os.path.exists(outpath):
            count+=1
            with open(outpath, 'r') as f:
                logs = json.load(f)
                if not isinstance(logs, list):
                    fails.append(outpath)
                    log_lists.append(logs)
                f.close()
    for fail in fails:
        os.remove(fail)
    print(len(fails))
    print(len(log_lists))
    print(count)
    print(set(log_lists))


if __name__ == '__main__':
    job = 10
    collector = PoolEventCollector()
    collector.download_download_pool_events_by_patch(job, dex="panv2")

    # collector = PoolEventCollector()
    # # collector.download_pool_events("Burn", "univ2", EtherscanAPI, setting.ETHERSCAN_API_KEYS[5])
    # # collector.download_pool_events("Mint", "univ2", EtherscanAPI, setting.ETHERSCAN_API_KEYS[6])
    # collector.download_pool_events("Swap", "univ2", EtherscanAPI, setting.ETHERSCAN_API_KEYS[7])
    # # collector.download_pool_events("Transfer", "univ2", EtherscanAPI, setting.ETHERSCAN_API_KEYS[8])
    # # collector.download_pool_events("Sync", "univ2", EtherscanAPI, setting.ETHERSCAN_API_KEYS[9])
    # # clean_fail_data("Sync")
# Complete: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 12 13, 14, 15, 16, 17