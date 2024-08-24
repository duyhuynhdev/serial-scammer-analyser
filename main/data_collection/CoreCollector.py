import utils.Utils as ut
from tqdm import tqdm
import os
import pandas as pd
from web3 import Web3
from utils.Settings import Setting
from utils.Path import Path

path = Path()
setting = Setting()


def save_or_append_data(data, output_path):
    save_df = pd.DataFrame.from_records(data)
    if os.path.isfile(output_path):
        print("APPEND ", len(data), "RECORDS")
        save_df.to_csv(output_path, mode='a', header=False, index=False)
    else:
        print("SAVE", len(data), "RECORDS")
        save_df.to_csv(output_path, index=False)


def download_pool_address(job, chunks, dex="univ2", node_url=setting.INFURA_ETH_NODE_URL, factory_address=setting.UNIV2_FACTORY_ADDRESS, factory_abi=setting.UNIV2_FACTORY_ABI):
    chunk = chunks[job]
    key = setting.INFURA_API_KEYS[job % len(setting.INFURA_API_KEYS)]
    node_web3 = Web3(Web3.HTTPProvider(node_url + key))
    file_name = f'{chunk["from"]}_{chunk["to"]}.csv'
    output_path = os.path.join(eval('path.{}_address_path'.format(dex)), "", file_name)
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
            save_or_append_data(data, output_path)
            data = []
    if len(data) > 0:
        save_or_append_data(data, output_path)
    print(f'FINISHED DOWNLOADING DATA (JOB {job})')


def removing_duplication(chunks, dex="univ2"):
    for chunk in chunks:
        file_name = f'{chunk["from"]}_{chunk["to"]}.csv'
        output_path = os.path.join(eval('path.{}_address_path'.format(dex)), "", file_name)
        df = pd.read_csv(output_path)
        before = len(df)
        print(file_name, len(df))
        df.drop_duplicates(inplace=True)
        after = len(df)
        if before != after:
            df.to_csv(output_path, index=False)


def uniswap_pools_download(job):
    # chunks = ut.partitioning(0, setting.UNIV2_NUM_OF_PAIRS, 70000)
    chunks = [{'from': 0, 'to': 69999}, {'from': 70000, 'to': 139999}, {'from': 140000, 'to': 209999}, {'from': 210000, 'to': 279999}, {'from': 280000, 'to': 349999},
              {'from': 350000, 'to': 356295}]
    if job >= len(chunks):
        return

    download_pool_address(job,
                          chunks=chunks,
                          dex="univ2",
                          node_url=setting.INFURA_ETH_NODE_URL,
                          factory_address=setting.UNIV2_FACTORY_ADDRESS,
                          factory_abi=setting.UNIV2_FACTORY_ABI)


def pancakeswap_pools_download(job):
    # chunks = ut.partitioning(0, setting.PANV2_NUM_OF_PAIRS, 70000)
    chunks = [{'from': 0, 'to': 69999}, {'from': 70000, 'to': 139999}, {'from': 140000, 'to': 209999}, {'from': 210000, 'to': 279999}, {'from': 280000, 'to': 349999},
              {'from': 350000, 'to': 419999}, {'from': 420000, 'to': 489999}, {'from': 490000, 'to': 559999}, {'from': 560000, 'to': 629999},
              {'from': 630000, 'to': 699999}, {'from': 700000, 'to': 769999}, {'from': 770000, 'to': 839999}, {'from': 840000, 'to': 909999},
              {'from': 910000, 'to': 979999}, {'from': 980000, 'to': 1049999}, {'from': 1050000, 'to': 1119999}, {'from': 1120000, 'to': 1189999},
              {'from': 1190000, 'to': 1259999}, {'from': 1260000, 'to': 1329999}, {'from': 1330000, 'to': 1399999}, {'from': 1400000, 'to': 1469999},
              {'from': 1470000, 'to': 1539999}, {'from': 1540000, 'to': 1609999}, {'from': 1610000, 'to': 1679999}, {'from': 1680000, 'to': setting.PANV2_NUM_OF_PAIRS}]
    if job >= len(chunks):
        return
    download_pool_address(job,
                          chunks=chunks,
                          dex="panv2",
                          node_url=setting.INFURA_BSC_NODE_URL,
                          factory_address=setting.PANV2_FACTORY_ADDRESS,
                          factory_abi=setting.PANV2_FACTORY_ABI)


if __name__ == '__main__':
    job = 24
    pancakeswap_pools_download(job)
