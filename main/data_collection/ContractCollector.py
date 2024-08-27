import utils.Utils as ut
from tqdm import tqdm
import os
import pandas as pd
from web3 import Web3
from utils.Settings import Setting
from utils.Path import Path
from api import CoinMarketCapAPI, OtherAPI
path = Path()
setting = Setting()

uni_chunks = [{'from': 0, 'to': 69999}, {'from': 70000, 'to': 139999}, {'from': 140000, 'to': 209999}, {'from': 210000, 'to': 279999}, {'from': 280000, 'to': 349999},
              {'from': 350000, 'to': setting.UNIV2_NUM_OF_PAIRS}]
pancake_chunks = [{'from': 0, 'to': 69999}, {'from': 70000, 'to': 139999}, {'from': 140000, 'to': 209999}, {'from': 210000, 'to': 279999}, {'from': 280000, 'to': 349999},
              {'from': 350000, 'to': 419999}, {'from': 420000, 'to': 489999}, {'from': 490000, 'to': 559999}, {'from': 560000, 'to': 629999},
              {'from': 630000, 'to': 699999}, {'from': 700000, 'to': 769999}, {'from': 770000, 'to': 839999}, {'from': 840000, 'to': 909999},
              {'from': 910000, 'to': 979999}, {'from': 980000, 'to': 1049999}, {'from': 1050000, 'to': 1119999}, {'from': 1120000, 'to': 1189999},
              {'from': 1190000, 'to': 1259999}, {'from': 1260000, 'to': 1329999}, {'from': 1330000, 'to': 1399999}, {'from': 1400000, 'to': 1469999},
              {'from': 1470000, 'to': 1539999}, {'from': 1540000, 'to': 1609999}, {'from': 1610000, 'to': 1679999}, {'from': 1680000, 'to': setting.PANV2_NUM_OF_PAIRS}]

class PoolDataCollector:
    def download_pool_address(self, job, chunks, dex="univ2", node_url=setting.INFURA_ETH_NODE_URL, factory_address=setting.UNIV2_FACTORY_ADDRESS, factory_abi=setting.UNIV2_FACTORY_ABI):
        chunk = chunks[job]
        key = setting.INFURA_API_KEYS[job % len(setting.INFURA_API_KEYS)]
        node_web3 = Web3(Web3.HTTPProvider(node_url + key))
        file_name = f'{chunk["from"]}_{chunk["to"]}.csv'
        output_path = os.path.join(eval('path.{}_address_path'.format(dex)), file_name)
        downloaded_idxs = []
        print(f'START DOWNLOADING DATA (JOB {job})')
        print(f'WITH KEY {key}')
        print(f'DATA IS WRITTEN INTO FILE {file_name}')

        if os.path.isfile(output_path):
            df = pd.read_csv(output_path)
            downloaded_idxs = set(df["id"])
        data = []
        factory = node_web3.eth.contract(factory_address, abi=factory_abi)
        for i in tqdm(range(chunk["from"], chunk["to"] + 1)):
            if i in downloaded_idxs:
                # print("DOWNLOADED ALREADY: ", i)
                continue
            pool_address = factory.functions.allPairs(i).call()
            data.append({"id": i, "pool": pool_address})
            if len(data) >= 10:
                ut.save_or_append_if_exist(data, output_path)
                data = []
        if len(data) > 0:
            ut.save_or_append_if_exist(data, output_path)
        print(f'FINISHED DOWNLOADING DATA (JOB {job})')


    def removing_duplication(self, chunks, dex="univ2"):
        for chunk in chunks:
            file_name = f'{chunk["from"]}_{chunk["to"]}.csv'
            output_path = os.path.join(eval('path.{}_address_path'.format(dex)), file_name)
            df = pd.read_csv(output_path)
            before = len(df)
            print(file_name, len(df))
            df.drop_duplicates(inplace=True)
            after = len(df)
            if before != after:
                df.to_csv(output_path, index=False)

    def merge_all_pools(self, chunks, dex="univ2"):
        all_pools = []
        output_path =  os.path.join(eval('path.{}_pool_path'.format(dex)), "pool_addresses.csv")
        for chunk in chunks:
            file_name = f'{chunk["from"]}_{chunk["to"]}.csv'
            file_path = os.path.join(eval('path.{}_address_path'.format(dex)), file_name)
            df = pd.read_csv(file_path)
            all_pools.extend(df.to_dict("records"))
        pd.DataFrame.from_records(all_pools).to_csv(output_path, index=False)

    def uniswap_pools_download(self, job):
        # chunks = ut.partitioning(0, setting.UNIV2_NUM_OF_PAIRS, 70000)
        if job >= len(uni_chunks):
            return
        self.download_pool_address(job,
                              chunks=uni_chunks,
                              dex="univ2",
                              node_url=setting.INFURA_ETH_NODE_URL,
                              factory_address=setting.UNIV2_FACTORY_ADDRESS,
                              factory_abi=setting.UNIV2_FACTORY_ABI)


    def pancakeswap_pools_download(self, job):
        # chunks = ut.partitioning(0, setting.PANV2_NUM_OF_PAIRS, 70000)

        if job >= len(pancake_chunks):
            return
        self.download_pool_address(job,
                              chunks=pancake_chunks,
                              dex="panv2",
                              node_url=setting.INFURA_BSC_NODE_URL,
                              factory_address=setting.PANV2_FACTORY_ADDRESS,
                              factory_abi=setting.PANV2_FACTORY_ABI)

class TokenDataCollector:
    def download_tokens_from_pool(self, job, chunks, dex="univ2", node_url=setting.INFURA_ETH_NODE_URL, pool_abi=setting.UNIV2_POOL_ABI):
        chunk = chunks[job]
        key = setting.INFURA_API_KEYS[(job % len(setting.INFURA_API_KEYS))]
        node_web3 = Web3(Web3.HTTPProvider(node_url + key))
        pool_file_name = f'{chunk["from"]}_{chunk["to"]}.csv'
        pool_path = os.path.join(eval('path.{}_address_path'.format(dex)), pool_file_name)
        pool_addresses = []
        if os.path.isfile(pool_path):
            df = pd.read_csv(pool_path)
            pool_addresses = df["pool"].values
        pool_info_file_name = f'{chunk["from"]}_{chunk["to"]}.csv'
        output_path = os.path.join(eval('path.{}_info_path'.format(dex)), pool_info_file_name)
        print(f'START DOWNLOADING DATA (JOB {job})')
        print(f'WITH KEY {key}')
        print(f'DATA IS WRITTEN INTO FILE {pool_info_file_name}')
        downloaded_addresses = []
        if os.path.isfile(output_path):
            df = pd.read_csv(output_path)
            downloaded_addresses = df["pool"].values
        data = []
        for address in tqdm(pool_addresses):
            if address in downloaded_addresses:
                # print("DOWNLOADED ALREADY: ", address)
                continue
            pool = node_web3.eth.contract(address, abi=pool_abi)
            token0 = pool.functions.token0().call()
            token1 = pool.functions.token1().call()
            data.append({"pool": address, "token0": token0, "token1": token1})
            if len(data) >= 10:
                ut.save_or_append_if_exist(data, output_path)
                data = []
        if len(data) > 0:
            ut.save_or_append_if_exist(data, output_path)
        print(f'FINISHED DOWNLOADING DATA (JOB {job})')

    def uniswap_token_download(self, job):
        if job >= len(uni_chunks):
            return
        self.download_tokens_from_pool(job, uni_chunks, dex="univ2", node_url=setting.INFURA_ETH_NODE_URL, pool_abi=setting.UNIV2_POOL_ABI)

    def pancakeswap_token_download(self, job):
        if job >= len(pancake_chunks):
            return
        self.download_tokens_from_pool(job, pancake_chunks, dex="panv2", node_url=setting.INFURA_BSC_NODE_URL, pool_abi=setting.PANV2_POOL_ABI)

class PopularTokenDataCollector:
    def get_cmc_top_token(self):
        for i in tqdm(range(0, 10000, 5000)):
            file_name = f"cmc_{str(i + 1)}_{ str(i + 5000)}_ranking.json"
            output_path = os.path.join(path.popular_tokens, file_name)
            result = CoinMarketCapAPI.get_top_crypto_ranking(i + 1)
            ut.write_json(output_path, result)


    def get_cmc_latest_token_with_marketcap(self):
        for i in tqdm(range(0, 10000, 5000)):
            file_name = f"cmc_{str(i + 1)}_{ str(i + 5000)}_latest_listing.json"
            output_path = os.path.join(path.popular_tokens, file_name)
            result = CoinMarketCapAPI.get_latest_crypto_listing(i + 1)
            ut.write_json(output_path, result)

    def get_cgk_top_token(self):
        output_path = os.path.join(path.popular_tokens, "coingecko_top_tokens.json")
        OtherAPI.get_tokens_coingecko(output_path)

    def download_popular_tokens(self):
        self.get_cmc_top_token()
        self.get_cmc_latest_token_with_marketcap()
        self.get_cgk_top_token()


if __name__ == '__main__':
    job = 0
    collector = TokenDataCollector()
    collector.uniswap_token_download(job)
    # pancakeswap_pools_download(job)
    # # download_popular_tokens()
    # collector =  PoolDataCollector()
    # collector.merge_all_pools(pancake_chunks, "panv2")