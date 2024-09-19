import json

import utils.Utils as ut
from tqdm import tqdm
import os
import pandas as pd

from data_collection import DataDecoder
from entity.blockchain.Transaction import NormalTransaction, InternalTransaction
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
    def get_creators(self, addresses, job, contract_type='pool', dex='univ2'):
        data = []
        five_patch = []
        downloaded_addresses = []
        api = explorer_api[dex]["explorer"]
        keys = explorer_api[dex]["keys"]
        key = keys[(job % len(keys)) + 5]
        output_path = os.path.join(eval(f'path.{dex}_{contract_type}_path'), f"{contract_type}_creation_info.csv")
        if os.path.isfile(output_path):
            df = pd.read_csv(output_path)
            downloaded_addresses = df["contractAddress"].str.lower().values
        chunks = ut.partitioning(0, len(addresses), 50000)
        chunk = chunks[job]
        chunk_addresses = addresses[chunk["from"]:(chunk["to"] + 1)]
        print(f'START DOWNLOADING DATA (JOB {job}):{chunk["from"]}_{chunk["to"]} (size: {len(chunk_addresses)})')
        print(f'WITH KEY {key}')
        for address in tqdm(chunk_addresses):
            if address.lower() in downloaded_addresses:
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
        # global key_idx
        api = explorer_api[dex]["explorer"]
        keys = explorer_api[dex]["keys"]
        # while key_idx < len(keys):
        try:
            result = api.get_contract_creation_info([address], keys[key_idx])
            ut.save_or_append_if_exist(result, output_path)
            return result[0]
        except Exception as e:
            print("CANNOT FIND CREATOR OF", address)
            return None

    def get_contract_creator(self, address, dex='univ2'):
        address = address.lower()
        pool_creation_path = os.path.join(eval('path.{}_pool_path'.format(dex)), "pool_creation_info.csv")
        if os.path.isfile(pool_creation_path):
            existed_data = pd.read_csv(pool_creation_path)
            existed_data.drop_duplicates(inplace=True)
            if address in existed_data["contractAddress"].values:
                existed_data.set_index("contractAddress", inplace=True)
                record = existed_data.loc[address]
                return {"contractAddress": address, "contractCreator": record["contractCreator"], "txHash": record["txHash"]}
        token_creation_path = os.path.join(eval('path.{}_token_path'.format(dex)), "token_creation_info.csv")
        if os.path.isfile(token_creation_path):
            existed_data = pd.read_csv(token_creation_path)
            existed_data.drop_duplicates(inplace=True)
            if address in existed_data["contractAddress"].values:
                existed_data.set_index("contractAddress", inplace=True)
                record = existed_data.loc[address]
                return {"contractAddress": address, "contractCreator": record["contractCreator"], "txHash": record["txHash"]}
        contract_creation_path = os.path.join(eval('path.{}_account_path'.format(dex)), "contract_creation_info.csv")
        if not os.path.isfile(contract_creation_path):
            return self.download_creator(address, contract_creation_path, dex)
        existed_data = pd.read_csv(contract_creation_path)
        existed_data.drop_duplicates(inplace=True)
        if not address in existed_data["contractAddress"].values:
            return self.download_creator(address, contract_creation_path, dex)
        existed_data.set_index("contractAddress", inplace=True)
        record = existed_data.loc[address]
        return {"contractAddress": address, "contractCreator": record["contractCreator"], "txHash": record["txHash"]}

    def get_pool_creator(self, address, dex='univ2'):
        address = address.lower()
        pool_creation_path = os.path.join(eval('path.{}_pool_path'.format(dex)), "pool_creation_info.csv")
        # if not os.path.isfile(pool_creation_path):
        #     return self.download_creator(address, pool_creation_path, dex)
        existed_data = pd.read_csv(pool_creation_path)
        # if not address in existed_data["contractAddress"].values:
        #     return self.download_creator(address, pool_creation_path, dex)
        existed_data.drop_duplicates(inplace=True)
        existed_data.set_index("contractAddress", inplace=True)
        record = existed_data.loc[address]
        return {"contractAddress": address, "contractCreator": record["contractCreator"], "txHash": record["txHash"]}

    def get_token_creator(self, address, dex='univ2'):
        address = address.lower()
        token_creation_path = os.path.join(eval('path.{}_token_path'.format(dex)), "token_creation_info.csv")
        # if not os.path.isfile(token_creation_path):
        #     return self.download_creator(address, token_creation_path, dex)
        existed_data = pd.read_csv(token_creation_path)
        # if not address in existed_data["contractAddress"].values:
        #     return self.download_creator(address, token_creation_path, dex)
        existed_data.drop_duplicates(inplace=True)
        existed_data.set_index("contractAddress", inplace=True)
        record = existed_data.loc[address]
        return {"contractAddress": address, "contractCreator": record["contractCreator"], "txHash": record["txHash"]}


class TransactionCollector:
    univ2_last_block = 20606150  # Aug-25-2024 02:17:59 PM +UTC
    panv2_last_block = 41674250

    def get_transactions(self, address, dex='univ2'):
        if address == "0x98f2ee6e58778f13a975fe1d6c3a8c773779cc73":
            print()
        api = explorer_api[dex]["explorer"]
        keys = explorer_api[dex]["keys"]
        normal_txs_path = os.path.join(eval('path.{}_normal_tx_path'.format(dex)), f"{address}.csv")
        parsed_normal_txs = []
        parsed_internal_txs = []
        if os.path.isfile(normal_txs_path):
            try:
                normal_txs = pd.read_csv(normal_txs_path)
            except Exception as e:
                print(address, e)
                normal_txs = None
        else:
            normal_txs = pd.DataFrame(self.download_normal_transactions(address, api, keys[key_idx], dex))
        internal_txs_path = os.path.join(eval('path.{}_internal_tx_path'.format(dex)), f"{address}.csv")
        if os.path.isfile(internal_txs_path):
            try:
                internal_txs = pd.read_csv(internal_txs_path)
            except Exception as e:
                print(address, e)
                internal_txs = None
        else:
            internal_txs = pd.DataFrame(self.download_internal_transactions(address, api, keys[key_idx], dex))
        if normal_txs is not None:
            normal_txs.rename(columns={'from': 'sender'}, inplace=True)
            for tx in normal_txs.to_dict('records'):
                ptx = NormalTransaction()
                ptx.from_dict(tx)
                parsed_normal_txs.append(ptx)
        if internal_txs is not None:
            internal_txs.rename(columns={'from': 'sender'}, inplace=True)
            for tx in internal_txs.to_dict('records'):
                ptx = InternalTransaction()
                ptx.from_dict(tx)
                parsed_internal_txs.append(ptx)
        return parsed_normal_txs, parsed_internal_txs

    def download_transactions(self, job, addresses, dex='univ2'):
        api = explorer_api[dex]["explorer"]
        keys = explorer_api[dex]["keys"]
        chunks = ut.partitioning(0, len(addresses), int(len(addresses) / len(keys)))
        chunk = chunks[job]
        chunk_addresses = addresses[chunk["from"]:(chunk["to"] + 1)]
        print(f"DOWNLOAD ACCOUNT TXS FROM {chunk['from']} TO {chunk['to']} WITH KEY {keys[job % len(keys)]} (JOB {job})")
        for address in tqdm(chunk_addresses):
            self.download_normal_transactions(address, api, keys[job % len(keys)], dex)
            self.download_internal_transactions(address, api, keys[job % len(keys)], dex)

    def download_normal_transactions(self, address, api, apikey, dex='univ2'):
        output_path = os.path.join(eval('path.{}_normal_tx_path'.format(dex)), address + ".csv")
        if not os.path.isfile(output_path):
            result = api.get_normal_transactions(address, fromBlock=0, toBlock=eval('self.{}_last_block'.format(dex)), apikey=apikey)
            ut.save_overwrite_if_exist(result, output_path)
            print(f"SAVED NORMAL TXs OF {address}")
            return result

    def download_internal_transactions(self, address, api, apikey, dex='univ2'):
        output_path = os.path.join(eval('path.{}_internal_tx_path'.format(dex)), address + ".csv")
        if not os.path.isfile(output_path):
            result = api.get_internal_transactions(address, fromBlock=0, toBlock=eval('self.{}_last_block'.format(dex)), apikey=apikey)
            ut.save_overwrite_if_exist(result, output_path)
            print(f"SAVED INTERNAL TXs OF {address}")
            return result


if __name__ == '__main__':
    dex = 'univ2'
    job = 17
    # pool_path = os.path.join(eval('path.{}_pool_path'.format(dex)), "pool_addresses.csv")
    # pools = pd.read_csv(pool_path)["pool"].values
    # collectors = CreatorCollector()
    # collectors.get_creators(addresses=pools, job=job, contract_type='pool', dex=dex)
    # pool_info_path = os.path.join(eval('path.{}_pool_path'.format(dex)), "pool_info.csv")
    # df = pd.read_csv(pool_info_path)
    # token_addresses = df["token0"].to_list()
    # token_addresses.extend(df["token1"].to_list())
    # token_addresses = list(dict.fromkeys(token_addresses))
    # print(len(token_addresses))
    # collectors = CreatorCollector()
    # collectors.get_creators(addresses=token_addresses, job=job, contract_type='pool', dex=dex)
    # print(collectors.get_pool_creator("0x2102A87B61Ca83a947473808677f1cF33A260c69", dex=dex))
    tx_collector = TransactionCollector()
    # normal, internal = tx_collector.get_transactions("0x48f0fc8dfc672dd45e53b6c53cd5b09c71d9fbd6", dex=dex)
    # print(normal)
    # print(internal)
    # scammers = pd.read_csv(os.path.join(path.univ2_processed_path, "scammers.csv"))["scammer"].values.tolist()
    # tx_collector.download_transactions(job, scammers, dex)
    transaction = tx_collector.get_transactions("0x19b98792e98c54f58c705cddf74316aec0999aa6", dex)
    print(transaction)