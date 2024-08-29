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

explorer_api = {
    "univ2": {"explorer": EtherscanAPI, "keys": setting.ETHERSCAN_API_KEYS},
    "panv2": {"explorer": BSCscanAPI, "keys": setting.BSCSCAN_API_KEYS},
}

key_idx = 0


class CreatorCollector:
    def get_creators(self, addresses, job, output_path, dex='univ2'):
        data = []
        five_patch = []
        downloaded_addresses = []
        api = explorer_api[dex]["explorer"]
        key = explorer_api[dex]["keys"][job]
        if os.path.isfile(output_path):
            df = pd.read_csv(output_path)
            downloaded_addresses = df["contractAddress"].values
        print(f'START DOWNLOADING DATA (JOB {job})')
        for address in addresses:
            if address in downloaded_addresses:
                continue
            five_patch.append(address)
            if len(five_patch) < 5:
                continue
            data.extend(api.get_contract_creation_info(five_patch, key))
            five_patch = []
            if len(data) >= 50:
                ut.save_or_append_if_exist(data, output_path)
                data = []
        if len(five_patch) > 0:
            data.extend(api.get_contract_creation_info(five_patch, key))
        if len(data) > 0:
            ut.save_or_append_if_exist(data, output_path)
        print(f'FINISHED DOWNLOADING DATA (JOB {job})')

    def download_creator(self, address, output_path, dex='univ2'):
        global key_idx
        api = explorer_api[dex]["explorer"]
        keys = explorer_api[dex]["keys"]
        while key_idx < len(keys):
            try:
                result = api.get_contract_creation_info([address], keys[key_idx])
                ut.save_or_append_if_exist(result, output_path)
                return result[0]
            except Exception as e:
                print(e)

    def get_pool_creator(self, address, dex='univ2'):
        pool_creation_path = os.path.join(eval('path.{}_pool_path'.format(dex)), "pool_creation_info.csv")
        if not os.path.isfile(pool_creation_path):
            return self.download_creator(address, pool_creation_path, dex)
        existed_data = pd.read_csv(pool_creation_path)
        if not address in existed_data["contractAddress"].values:
            return self.download_creator(address, pool_creation_path, dex)
        existed_data.set_index("contractAddress", inplace=True)
        record = existed_data.loc[address]
        return {"contractAddress": address, "contractCreator": record["contractCreator"], "txHash": record["txHash"]}

    def get_token_creator(self, address, dex='univ2'):
        token_creation_path = os.path.join(eval('path.{}_token_path'.format(dex)), "token_creation_info.csv")
        if not os.path.isfile(token_creation_path):
            return self.download_creator(address, token_creation_path, dex)
        existed_data = pd.read_csv(token_creation_path)
        if not address in existed_data["contractAddress"].values:
            return self.download_creator(address, token_creation_path, dex)
        existed_data.set_index("contractAddress", inplace=True)
        record = existed_data.loc[address]
        return {"contractAddress": address, "contractCreator": record["contractCreator"], "txHash": record["txHash"]}


class TransactionCollector:
    univ2_last_block = 20606150  # Aug-25-2024 02:17:59 PM +UTC
    panv2_last_block = 41674250

    def download_transactions(self, address, dex='univ2'):
        self.download_normal_transactions(address, dex)
        self.download_internal_transactions(address, dex)

    def download_normal_transactions(self, address, dex='univ2'):
        global key_idx
        api = explorer_api[dex]["explorer"]
        keys = explorer_api[dex]["keys"]
        while key_idx < len(keys):
            output_path = os.path.join(eval('path.{}_normal_tx_token_path'.format(dex, type)), address + ".csv")
            try:
                result = api.get_normal_transactions(address, fromBlock=0, toBlock=eval('self.{}_last_block'.format(dex)), apikey=keys[key_idx])
                ut.save_or_append_if_exist(result, output_path)
                return result[0]
            except Exception as e:
                print(e)

    def download_internal_transactions(self, address, dex='univ2'):
        global key_idx
        api = explorer_api[dex]["explorer"]
        keys = explorer_api[dex]["keys"]
        while key_idx < len(keys):
            output_path = os.path.join(eval('path.{}_internal_tx_token_path'.format(dex, type)), address + ".csv")
            try:
                result = api.get_normal_transactions(address, fromBlock=0, toBlock=eval('self.{}_last_block'.format(dex)), apikey=keys[key_idx])
                ut.save_or_append_if_exist(result, output_path)
                return result[0]
            except Exception as e:
                print(e)


if __name__ == '__main__':
    for i in tqdm(range(0, 12, 5)):
        print(i)
