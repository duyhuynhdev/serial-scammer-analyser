import json

import utils.Utils as ut
from tqdm import tqdm
import os
import pandas as pd
from utils.Settings import Setting
from utils.Path import Path
from api import EtherscanAPI

path = Path()
setting = Setting()


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
        logs = explorer.get_event_logs(pool_address, 0, last_block, event_signature_hash,apikey)
        with open(outpath, 'w') as wf:
            wf.write(json.dumps(logs, default=lambda x: getattr(x, '__dict__', str(x))))
            wf.close()
        return logs

    def download_pool_events(self, event, dex="univ2", explorer=EtherscanAPI, apikey=setting.ETHERSCAN_API_KEY):
        print("DOWNLOAD EVENT {} WITH KEY {}".format(event, apikey))
        pool_path = os.path.join(eval('path.{}_pool_path'.format(dex)), "pool_addresses.csv")
        pools = pd.read_csv(pool_path)["pool"].values
        for pool in tqdm(pools):
            self.download_event_logs(pool, eval('self.{}_last_block'.format(dex)), event, explorer=explorer, apikey=apikey)

if __name__ == '__main__':
    collector = PoolEventCollector()
    collector.download_pool_events("Burn", "univ2", EtherscanAPI, setting.ETHERSCAN_API_KEYS[0])
    # collector.download_pool_events("Mint", "univ2", EtherscanAPI, setting.ETHERSCAN_API_KEYS[1])
    # collector.download_pool_events("Swap", "univ2", EtherscanAPI, setting.ETHERSCAN_API_KEYS[2])
    # collector.download_pool_events("Transfer", "univ2", EtherscanAPI, setting.ETHERSCAN_API_KEYS[3])
    # collector.download_pool_events("Sync", "univ2", EtherscanAPI, setting.ETHERSCAN_API_KEYS[4])
