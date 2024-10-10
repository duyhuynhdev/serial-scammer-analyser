import json
import utils.Utils as ut
from tqdm import tqdm
import os
import pandas as pd

from entity.blockchain.Transaction import NormalTransaction, InternalTransaction
from utils.Settings import Setting
from utils.ProjectPath import ProjectPath
from api import EtherscanAPI, BSCscanAPI
from utils import Constant

path = ProjectPath()
setting = Setting()
explorer_api = {
    "univ2": {"explorer": EtherscanAPI, "keys": setting.ETHERSCAN_API_KEYS},
    "panv2": {"explorer": BSCscanAPI, "keys": setting.BSCSCAN_API_KEYS},
}


class CreatorCollector:

    def __init__(self, dex='univ2'):
        print("In constructor of CreatorCollector! Caching objects")
        pool_creation_path = os.path.join(eval('path.{}_processed_path'.format(dex)), "pool_creation_info.csv")
        self.pool_existed_data = pd.read_csv(pool_creation_path)
        self.pool_existed_data.drop_duplicates(inplace=True)
        self.pool_existed_data.set_index("contractAddress", inplace=True)

        token_creation_path = os.path.join(eval('path.{}_processed_path'.format(dex)), "token_creation_info.csv")
        self.token_existed_data = pd.read_csv(token_creation_path)
        self.token_existed_data.drop_duplicates(inplace=True)
        self.token_existed_data.set_index("contractAddress", inplace=True)

    def get_creators(self, addresses, job, contract_type='pool', dex='univ2'):
        data = []
        five_patch = []
        downloaded_addresses = []
        api = explorer_api[dex]["explorer"]
        keys = explorer_api[dex]["keys"]
        key = keys[(job % len(keys))]
        output_path = os.path.join(eval(f'path.{dex}_{contract_type}_path'), f"{contract_type}_creation_info.csv")
        if os.path.isfile(output_path):
            df = pd.read_csv(output_path)
            downloaded_addresses = df["contractAddress"].str.lower().values
        chunks = ut.partitioning(0, len(addresses), int(len(addresses) / len(keys)))
        chunk = chunks[job]
        chunk_addresses = addresses[chunk["from"]:(chunk["to"] + 1)]
        print(f'START DOWNLOADING DATA (JOB {job}/ {len(chunks)}):{chunk["from"]}_{chunk["to"]} (size: {len(chunk_addresses)})')
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

    def download_creator(self, address, output_path, dex='univ2', key_idx=0):
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
        # pool_creation_path = os.path.join(eval('path.{}_processed_path'.format(dex)), "pool_creation_info.csv")
        # if not os.path.isfile(pool_creation_path):
        #     return self.download_creator(address, pool_creation_path, dex)
        # existed_data = pd.read_csv(pool_creation_path)
        # if not address in existed_data["contractAddress"].values:
        #     return self.download_creator(address, pool_creation_path, dex)
        # existed_data.drop_duplicates(inplace=True)
        # existed_data.set_index("contractAddress", inplace=True)
        record = self.pool_existed_data.loc[address]
        return {"contractAddress": address, "contractCreator": record["contractCreator"], "txHash": record["txHash"]}

    def get_token_creator(self, address, dex='univ2'):
        address = address.lower()
        # token_creation_path = os.path.join(eval('path.{}_processed_path'.format(dex)), "token_creation_info.csv")
        # if not os.path.isfile(token_creation_path):
        #     return self.download_creator(address, token_creation_path, dex)
        # existed_data = pd.read_csv(token_creation_path)
        # if not address in existed_data["contractAddress"].values:
        #     return self.download_creator(address, token_creation_path, dex)
        # existed_data.drop_duplicates(inplace=True)
        # existed_data.set_index("contractAddress", inplace=True)
        record = self.token_existed_data.loc[address]
        return {"contractAddress": address, "contractCreator": record["contractCreator"], "txHash": record["txHash"]}


class TransactionCollector:
    univ2_first_block = Constant.UNISWAP_START_BLOCK
    univ2_last_block = Constant.UNISWAP_END_BLOCK  # Aug-25-2024 02:17:59 PM +UTC
    panv2_first_block = Constant.PANCAKESWAP_START_BLOCK
    panv2_last_block = Constant.PANCAKESWAP_END_BLOCK

    def get_transactions(self, address, dex='univ2', key_idx=0):
        from_block = eval('self.{}_first_block'.format(dex))
        to_block = eval('self.{}_last_block'.format(dex))
        api = explorer_api[dex]["explorer"]
        keys = explorer_api[dex]["keys"]
        key_idx = key_idx % len(keys)
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
        # disable internal transactions, it's not needed
        # if os.path.isfile(internal_txs_path):
        #     try:
        #         internal_txs = pd.read_csv(internal_txs_path)
        #     except Exception as e:
        #         print(address, e)
        #         internal_txs = None
        # else:
        #     internal_txs = pd.DataFrame(self.download_internal_transactions(address, api, keys[key_idx], dex))
        if normal_txs is not None:
            normal_txs.rename(columns={'from': 'sender'}, inplace=True)
            for tx in normal_txs.to_dict('records'):
                ptx = NormalTransaction()
                ptx.from_dict(tx)
                parsed_normal_txs.append(ptx)
        # if internal_txs is not None:
        #     internal_txs.rename(columns={'from': 'sender'}, inplace=True)
        #     for tx in internal_txs.to_dict('records'):
        #         ptx = InternalTransaction()
        #         ptx.from_dict(tx)
        #         parsed_internal_txs.append(ptx)
        return parsed_normal_txs

    def download_transactions(self, job, addresses, dex='univ2'):
        api = explorer_api[dex]["explorer"]
        keys = explorer_api[dex]["keys"]
        chunks = ut.partitioning(0, len(addresses), int(len(addresses) / len(keys)))
        chunk = chunks[job]
        chunk_addresses = addresses[chunk["from"]:(chunk["to"] + 1)]
        print(f"DOWNLOAD ACCOUNT TXS FROM {chunk['from']} TO {chunk['to']} WITH KEY {keys[job % len(keys)]} (JOB {job}/{len(chunks)})")
        for address in tqdm(chunk_addresses):
            self.download_normal_transactions(address, api, keys[job % len(keys)], dex)
            self.download_internal_transactions(address, api, keys[job % len(keys)], dex)

    def download_normal_transactions(self, address, api, apikey, dex='univ2'):
        output_path = os.path.join(eval('path.{}_normal_tx_path'.format(dex)), address + ".csv")
        if not os.path.isfile(output_path):
            result = api.get_normal_transactions(address, fromBlock=eval('self.{}_first_block'.format(dex)), toBlock=eval('self.{}_last_block'.format(dex)), apikey=apikey)
            ut.save_overwrite_if_exist(result, output_path)
            print(f"\t\tSAVED NORMAL TXs OF {address}")
            return result

    def download_internal_transactions(self, address, api, apikey, dex='univ2'):
        output_path = os.path.join(eval('path.{}_internal_tx_path'.format(dex)), address + ".csv")
        if not os.path.isfile(output_path):
            result = api.get_internal_transactions(address, fromBlock=eval('self.{}_first_block'.format(dex)), toBlock=eval('self.{}_last_block'.format(dex)), apikey=apikey)
            ut.save_overwrite_if_exist(result, output_path)
            print(f"\t\tSAVED INTERNAL TXs OF {address}")
            return result


if __name__ == '__main__':
    dex = 'univ2'
    # job = 24
    # pool_path = os.path.join(eval('path.{}_processed_path'.format(dex)), "pool_addresses.csv")
    # pools = pd.read_csv(pool_path)["pool"].values
    # collectors = CreatorCollector()
    # collectors.get_creators(addresses=pools, job=job, contract_type='pool', dex=dex)
    ###########################################################################################
    # pool_info_path = os.path.join(eval('path.{}_processed_path'.format(dex)), "pool_info.csv")
    # df = pd.read_csv(pool_info_path)
    # token_addresses = df["token0"].to_list()
    # token_addresses.extend(df["token1"].to_list())
    # token_addresses = list(dict.fromkeys(token_addresses))
    # print(len(token_addresses))
    # collectors = CreatorCollector()
    # collectors.get_creators(addresses=token_addresses, job=job, contract_type='token', dex=dex)
    # print(collectors.get_pool_creator("0x2102A87B61Ca83a947473808677f1cF33A260c69", dex=dex))
    #############################################################################
    # scammers = pd.read_csv(os.path.join(eval('path.{}_processed_path'.format(dex)), "1_pair_scammers.csv"))
    # index_issue = scammers[(scammers["pool"] == scammers["scammer"])].index
    # scammers.drop(index_issue, inplace=True)
    # scammers["pool"] = scammers["pool"].str.lower()
    # scammers["scammer"] = scammers["scammer"].str.lower()
    tx_collector = TransactionCollector()
    # tx_collector.download_transactions(job, scammers["scammer"].str.lower().to_list(), dex)
    #########################################################################################
    transactions = tx_collector.get_transactions("0x5b5d8c8eed6c85ac215661de026676823faa0a0c", dex)
    print([tx.blockNumber for tx in transactions[0]])
