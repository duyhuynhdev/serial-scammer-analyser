import json

import utils.Utils as ut
from tqdm import tqdm
import os
import pandas as pd

from data_collection import DataDecoder
from utils.Settings import Setting
from utils.ProjectPath import ProjectPath
from api import EtherscanAPI, BSCscanAPI

path = ProjectPath()
setting = Setting()

key_index = 0

explorer_api = {
    "univ2": {"explorer": EtherscanAPI, "keys": setting.ETHERSCAN_API_KEYS},
    "panv2": {"explorer": BSCscanAPI, "keys": setting.BSCSCAN_API_KEYS},
}


class ContractEventCollector:
    univ2_last_block = 20606150  # Aug-25-2024 02:17:59 PM +UTC
    panv2_last_block = 41674250  # Aug-25-2024 02:20:03 PM +UTC

    event_info = {
        "Sync": {"signature": "Sync(uint112,uint112)", "inputs": ['reserve0', 'reserve1']},
        "Swap": {"signature": "Swap(address,uint256,uint256,uint256,uint256,address)", "inputs": ['sender', 'amount0In', 'amount1In', 'amount0Out', 'amount1Out', 'to']},
        "Burn": {"signature": "Burn(address,uint256,uint256,address)", "inputs": ['sender', 'amount0', 'amount1', 'to']},
        "Mint": {"signature": "Mint(address,uint256,uint256)", "inputs": ['sender', 'amount0', 'amount1']},
        "Transfer": {"signature": "Transfer(address,address,uint256)", "inputs": ['from', 'to', 'value']},
    }

    def download_event_logs(self, pool_address, outpath, last_block, event, explorer=EtherscanAPI, apikey=setting.ETHERSCAN_API_KEY):
        if os.path.exists(outpath):
            with open(outpath, 'r') as f:
                logs = json.load(f)
                f.close()
            return logs
        event_signature_hash = ut.keccak_hash(self.event_info[event]["signature"])
        logs = explorer.get_event_logs(pool_address, fromBlock=0, toBlock=last_block, topic=event_signature_hash, apikey=apikey)
        with open(outpath, 'w') as wf:
            wf.write(json.dumps(logs, default=lambda x: getattr(x, '__dict__', str(x))))
            wf.close()
        return logs

    def download_event(self, address, event, event_path, dex="univ2", explorer=EtherscanAPI, apikey=setting.ETHERSCAN_API_KEY):
        outpath = os.path.join(event_path, event, address + ".json")
        self.download_event_logs(address, outpath, eval('self.{}_last_block'.format(dex)), event, explorer=explorer, apikey=apikey)

    def download_multiple_events(self, addresses, path, events, dex="univ2", explorer=EtherscanAPI, apikey=setting.ETHERSCAN_API_KEY):
        for address in tqdm(addresses):
            if address == "":
                continue
            for event in events:
                outpath = os.path.join(path, event, address + ".json")
                self.download_event_logs(address, outpath, eval('self.{}_last_block'.format(dex)), event, explorer=explorer, apikey=apikey)

    def download_pool_events_by_patch(self, job, dex="univ2"):
        explorer = explorer_api[dex]["explorer"]
        keys = explorer_api[dex]["keys"]
        pool_path = os.path.join(eval('path.{}_processed_path'.format(dex)), "pool_addresses.csv")
        pools = pd.read_csv(pool_path)["pool"].values
        chunks = ut.partitioning(0, len(pools), int(len(pools) / len(keys)))
        chunk = chunks[job]
        chunk_addresses = pools[chunk["from"]:(chunk["to"] + 1)]
        print(f"DOWNLOAD ALL POOL EVENTS FROM {chunk['from']} TO {chunk['to']} WITH KEY {keys[job % len(keys)]} (JOB {job})")
        events = ["Burn", "Mint", "Swap", "Transfer"]
        path = eval('path.{}_pool_events_path'.format(dex))
        self.download_multiple_events(chunk_addresses, path, events, dex=dex, explorer=explorer, apikey=keys[job % len(keys)])

    def download_pool_events(self, event, dex="univ2", explorer=EtherscanAPI, apikey=setting.ETHERSCAN_API_KEY):
        print("DOWNLOAD EVENT {} WITH KEY {}".format(event, apikey))
        pool_path = os.path.join(eval('path.{}_pool_path'.format(dex)), "pool_addresses.csv")
        pools = pd.read_csv(pool_path)["pool"].values
        for pool in tqdm(pools):
            outpath = os.path.join(eval('path.{}_pool_events_path'.format(dex)), event, pool + ".json")
            self.download_event_logs(pool, outpath, eval('self.{}_last_block'.format(dex)), event, explorer=explorer, apikey=apikey)

    def download_download_token_events_by_patch(self, job, dex="univ2"):
        explorer = explorer_api[dex]["explorer"]
        keys = explorer_api[dex]["keys"]
        pool_labels_path = os.path.join(eval('path.{}_processed_path'.format(dex)), "1_pair_pool_labels.csv")
        pool_labels_df = pd.read_csv(pool_labels_path)
        pool_labels_df.fillna("", inplace=True)
        scam_tokens = pool_labels_df[pool_labels_df["is_rp"] != '0']["scam_token"].values
        chunks = ut.partitioning(0, len(scam_tokens), int(len(scam_tokens) / len(keys)))
        chunk = chunks[job]
        chunk_addresses = scam_tokens[chunk["from"]:(chunk["to"] + 1)]
        print(f"DOWNLOAD ALL TOKEN EVENTS FROM {chunk['from']} TO {chunk['to']} WITH KEY {keys[job % len(keys)]} (JOB {job})")
        events = ["Transfer"]
        path = eval('path.{}_token_events_path'.format(dex))
        self.download_multiple_events(chunk_addresses, path, events, dex=dex, explorer=explorer, apikey=keys[job % len(keys)])

    def parse_event(self, event, event_logs_path):
        decoder = DataDecoder.EventLogDecoder(event)
        events = []
        with open(event_logs_path, 'r') as f:
            logs = json.load(f)
            f.close()
        for log in logs:
            decoded_event = decoder.decode_event(log)
            events.append(decoded_event)
        return events

    def get_event(self, address, event, event_path, dex):
        global key_index
        event_logs_path = os.path.join(event_path, event, address + ".json")
        if not os.path.exists(event_logs_path):  # if not exist , starts download corresponding event
            while key_index < len(explorer_api[dex]["keys"]):
                try:
                    explorer = explorer_api[dex]["explorer"]
                    api_key = explorer_api[dex]["keys"][key_index]
                    self.download_event(address, event, event_path, dex="univ2", explorer=explorer, apikey=api_key)
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
            count += 1
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
    job = 17
    collector = ContractEventCollector()
    collector.download_download_token_events_by_patch(job)
    # collector.get_event("0x590fcAdC577810658Cc225E26d78C642cf08be4e","Transfer", path.univ2_token_events_path, "univ2")