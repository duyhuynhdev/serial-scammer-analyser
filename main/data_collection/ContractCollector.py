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

infura_api = {
    "univ2": {"node_url": setting.INFURA_ETH_NODE_URL, "num_pairs": setting.UNIV2_NUM_OF_PAIRS, "factory_abi": setting.UNIV2_FACTORY_ABI, "factory_address": setting.UNIV2_FACTORY_ADDRESS,
              "pool_abi": setting.UNIV2_POOL_ABI, "token_abi": setting.ETH_TOKEN_ABI},
    "panv2": {"node_url": setting.INFURA_BSC_NODE_URL, "num_pairs": setting.PANV2_NUM_OF_PAIRS, "factory_abi": setting.PANV2_FACTORY_ABI, "factory_address": setting.PANV2_FACTORY_ADDRESS,
              "pool_abi": setting.PANV2_POOL_ABI, "token_abi": setting.BSC_TOKEN_ABI},
}

key_idx = 0


class PoolDataCollector:
    def download_pool_address(self, job, chunks, dex="univ2"):
        chunk = chunks[job]
        key = setting.INFURA_API_KEYS[job % len(setting.INFURA_API_KEYS)]
        node_url = infura_api[dex]["node_url"]
        factory_address = infura_api[dex]["factory_address"]
        factory_abi = infura_api[dex]["factory_abi"]
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
        factory = node_web3.eth.contract(Web3.to_checksum_address(factory_address), abi=factory_abi)
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
        output_path = os.path.join(eval('path.{}_pool_path'.format(dex)), "pool_addresses.csv")
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
                                   dex="univ2")

    def pancakeswap_pools_download(self, job):
        # chunks = ut.partitioning(0, setting.PANV2_NUM_OF_PAIRS, 70000)

        if job >= len(pancake_chunks):
            return
        self.download_pool_address(job,
                                   chunks=pancake_chunks,
                                   dex="panv2")


class PoolInfoCollector:
    def download_tokens_from_pool(self, job, dex="univ2"):
        key = setting.INFURA_API_KEYS[(job % len(setting.INFURA_API_KEYS))]
        node_url = infura_api[dex]["node_url"]
        pool_abi = infura_api[dex]["pool_abi"]
        node_web3 = Web3(Web3.HTTPProvider(node_url + key))
        pool_path = os.path.join(eval('path.{}_processed_path'.format(dex)), "pool_addresses.csv")
        df = pd.read_csv(pool_path)
        pool_addresses = df["pool"].values
        output_path = os.path.join(eval('path.{}_processed_path'.format(dex)), "pool_info.csv")
        chunks = ut.partitioning(0, len(pool_addresses), int(len(pool_addresses) / len(setting.INFURA_API_KEYS)))
        chunk = chunks[job]
        chunk_addresses = pool_addresses[chunk["from"]:(chunk["to"] + 1)]
        print(f"DOWNLOAD ALL POOL EVENTS FROM {chunk['from']} TO {chunk['to']} WITH KEY {setting.INFURA_API_KEYS[job % len(setting.INFURA_API_KEYS)]} (JOB {job} / {len(chunks)})")
        print(f'START DOWNLOADING DATA (JOB {job})')
        print(f'WITH KEY {key}')
        print(f'DATA IS WRITTEN INTO FILE {output_path}')
        downloaded_addresses = []
        if os.path.isfile(output_path):
            df = pd.read_csv(output_path)
            downloaded_addresses = df["pool"].values
        data = []
        for address in tqdm(chunk_addresses):
            if address in downloaded_addresses:
                # print("DOWNLOADED ALREADY: ", address)
                continue
            pool = node_web3.eth.contract(Web3.to_checksum_address(address), abi=pool_abi)
            token0 = pool.functions.token0().call()
            token1 = pool.functions.token1().call()
            data.append({"pool": address, "token0": token0, "token1": token1})
            if len(data) >= 10:
                ut.save_or_append_if_exist(data, output_path)
                data = []
        if len(data) > 0:
            ut.save_or_append_if_exist(data, output_path)
        print(f'FINISHED DOWNLOADING DATA (JOB {job})')

    def download_pool_info(self, pool_address, dex="univ2"):
        global key_idx
        node_url = infura_api[dex]["node_url"]
        pool_abi = infura_api[dex]["pool_abi"]
        output_path = os.path.join(eval('path.{}_pool_path'.format(dex)), "pool_info.csv")
        while key_idx < len(setting.INFURA_API_KEYS):
            try:
                node_web3 = Web3(Web3.HTTPProvider(node_url + setting.INFURA_API_KEYS[key_idx]))
                pool = node_web3.eth.contract(Web3.to_checksum_address(pool_address), abi=pool_abi)
                token0 = pool.functions.token0().call()
                token1 = pool.functions.token1().call()
                info = {"pool": pool_address, "token0": token0, "token1": token1}
                ut.save_or_append_if_exist([info], output_path)
                return info
            except Exception as e:
                print(e)

    def get_pool_info(self, pool_address, dex="univ2"):
        info_path = os.path.join(eval('path.{}_pool_path'.format(dex)), "pool_info.csv")
        # if not os.path.isfile(info_path):
        #     return self.download_pool_info(pool_address, dex)
        existed_infos = pd.read_csv(info_path)
        # if not pool_address in existed_infos["pool"].values:
        #     return self.download_pool_info(pool_address, dex)
        existed_infos.set_index("pool", inplace=True)
        record = existed_infos.loc[pool_address]
        return {"pool": pool_address, "token0": record["token0"], "token1": record["token1"]}

    def merge_all_pool_infos(self, chunks, dex="univ2"):
        all_pools = []
        output_path = os.path.join(eval('path.{}_pool_path'.format(dex)), "pool_info.csv")
        for chunk in chunks:
            file_name = f'{chunk["from"]}_{chunk["to"]}.csv'
            file_path = os.path.join(eval('path.{}_info_path'.format(dex)), file_name)
            df = pd.read_csv(file_path)
            all_pools.extend(df.to_dict("records"))
        pd.DataFrame.from_records(all_pools).to_csv(output_path, index=False)

    def uniswap_token_download(self, job):
        self.download_tokens_from_pool(job, dex="univ2")

    def pancakeswap_token_download(self, job):
        self.download_tokens_from_pool(job, dex="panv2")


class TokenInfoCollector:
    def download_tokens_info(self, job, dex="univ2"):
        key = setting.INFURA_API_KEYS[(job % len(setting.INFURA_API_KEYS))]
        node_url = infura_api[dex]["node_url"]
        token_abi = infura_api[dex]["token_abi"]
        node_web3 = Web3(Web3.HTTPProvider(node_url + key))
        pool_info_path = os.path.join(eval('path.{}_pool_path'.format(dex)), "pool_info.csv")
        token_addresses = []
        if os.path.isfile(pool_info_path):
            df = pd.read_csv(pool_info_path)
            token_addresses = df["token0"].to_list()
            token_addresses.extend(df["token1"].to_list())
            token_addresses = list(dict.fromkeys(token_addresses))
        chunks = ut.partitioning(0, len(token_addresses), 50000)
        chunk = chunks[job]
        chunk_addresses = list(token_addresses)[chunk["from"]:(chunk["to"] + 1)]
        output_path = os.path.join(eval('path.{}_token_path'.format(dex)), "token_info.csv")
        print(f'START DOWNLOADING DATA (JOB {job}):{chunk["from"]}_{chunk["to"]} (size: {len(chunk_addresses)})')
        print(f'WITH KEY {key}')
        downloaded_addresses = []
        if os.path.isfile(output_path):
            df = pd.read_csv(output_path)
            downloaded_addresses = df["token"].values
        data = []
        for token_address in tqdm(chunk_addresses):
            if token_address in downloaded_addresses:
                # print("DOWNLOADED ALREADY: ", address)
                continue
            token = node_web3.eth.contract(Web3.to_checksum_address(token_address), abi=token_abi)
            info = {
                'token': token_address,
                'name': ut.try_except_assigning(token.functions.name().call, "").rstrip('\x00'),
                'symbol': ut.try_except_assigning(token.functions.symbol().call, "").rstrip('\x00'),
                'decimals': ut.try_except_assigning(token.functions.decimals().call, 0),
                'totalSupply': ut.try_except_assigning(token.functions.totalSupply().call, 0),
            }
            data.append(info)
            if len(data) >= 10:
                ut.save_or_append_if_exist(data, output_path)
                data = []
        if len(data) > 0:
            ut.save_or_append_if_exist(data, output_path)
        print(f'FINISHED DOWNLOADING DATA (JOB {job})')

    def get_token_info(self, token_address, dex="univ2"):
        info_path = os.path.join(eval('path.{}_token_path'.format(dex)), "token_info.csv")
        # if not os.path.isfile(info_path):
        #     return self.download_token_info(token_address, dex)
        existed_infos = pd.read_csv(info_path, low_memory=False)
        # if not token_address in existed_infos["token"].values:
        #     return self.download_token_info(token_address, dex)
        existed_infos.drop_duplicates(inplace=True)
        existed_infos.set_index("token", inplace=True)
        record = existed_infos.loc[token_address]
        return {"token": token_address, "name": record["name"], "symbol": record["symbol"], "decimals": record["decimals"], "totalSupply": record["totalSupply"]}

    def download_token_info(self, token_address, dex="univ2"):
        global key_idx
        node_url = infura_api[dex]["node_url"]
        token_abi = infura_api[dex]["token_abi"]
        output_path = os.path.join(eval('path.{}_token_path'.format(dex)), "token_info.csv")
        while key_idx < len(setting.INFURA_API_KEYS):
            try:
                node_web3 = Web3(Web3.HTTPProvider(node_url + setting.INFURA_API_KEYS[key_idx]))
                token = node_web3.eth.contract(Web3.to_checksum_address(token_address), abi=token_abi)

                info = {
                    'token': token_address,
                    'name': token.functions.name().call().rstrip('\x00'),
                    'symbol': token.functions.symbol().call().rstrip('\x00'),
                    'decimals': token.functions.decimals().call(),
                    'totalSupply': token.functions.totalSupply().call(),
                }
                ut.save_or_append_if_exist([info], output_path)
                return info
            except Exception as e:
                print(e)


class PopularTokenDataCollector:
    def get_cmc_top_token(self):
        for i in tqdm(range(0, 10000, 5000)):
            file_name = f"cmc_{str(i + 1)}_{str(i + 5000)}_ranking.json"
            output_path = os.path.join(path.popular_tokens, file_name)
            result = CoinMarketCapAPI.get_top_crypto_ranking(i + 1)
            ut.write_json(output_path, result)

    def get_cmc_latest_token_with_marketcap(self):
        for i in tqdm(range(0, 10000, 5000)):
            file_name = f"cmc_{str(i + 1)}_{str(i + 5000)}_latest_listing.json"
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
    collector = PoolInfoCollector()
    job = 9
    collector.pancakeswap_token_download(job)
    # pancakeswap_pools_download(job)
    # # download_popular_tokens()
    # collector =  PoolDataCollector()
    # collector.merge_all_pools(pancake_chunks, "panv2")
    # collector.merge_all_pool_infos(uni_chunks, dex="univ2")
    # collector = TokenInfoCollector()
    # collector.download_tokens_info(6)
