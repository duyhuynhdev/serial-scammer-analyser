import pandas as pd
import os

from marshmallow.orderedset import OrderedSet

from utils.Settings import Setting
from utils.Path import Path
from utils import Utils as ut

path = Path()
setting = Setting()

bridge_files = ["bridge.csv", "bridge_addresses.csv"]
defi_files = ["dex.csv", "cex_address.csv", "exchange_addresses.csv", "factory_addresses.csv", "deployer_addresses.csv", "proxy_addresses.csv", "router_addresses.csv"]
mev_bot_files = ["mev_bot_addresses.csv", "MEV_bots.csv"]
mixer_files = ["tonador_cash.csv"]
wallet_files = ["wallet_addresses.csv"]
other_files = ["multisender_addresses.csv", "multisig_addresses.csv"]


def load_end_nodes(dex='univ2'):
    print("LOAD END NODES")
    bridge_addresses = set()
    defi_addresses = set()
    MEV_addresses = set()
    mixer_addresses = set()
    wallet_addresses = set()
    other_addresses = set()
    for bf in bridge_files:
        df = pd.read_csv(os.path.join(eval('path.{}_public_addresses_path'.format(dex)), bf))
        bridge_addresses.update(df["address"].str.lower().values)
    for defi in defi_files:
        df = pd.read_csv(os.path.join(eval('path.{}_public_addresses_path'.format(dex)), defi))
        defi_addresses.update(df["address"].str.lower().values)
    for mev in mev_bot_files:
        df = pd.read_csv(os.path.join(eval('path.{}_public_addresses_path'.format(dex)), mev))
        MEV_addresses.update(df["address"].str.lower().values)
    for mixer in mixer_files:
        df = pd.read_csv(os.path.join(eval('path.{}_public_addresses_path'.format(dex)), mixer))
        mixer_addresses.update(df["address"].str.lower().values)
    for wallet in wallet_files:
        df = pd.read_csv(os.path.join(eval('path.{}_public_addresses_path'.format(dex)), wallet))
        wallet_addresses.update(df["address"].str.lower().values)
    for other in other_files:
        df = pd.read_csv(os.path.join(eval('path.{}_public_addresses_path'.format(dex)), other))
        other_addresses.update(df["address"].str.lower().values)
    return bridge_addresses, defi_addresses, MEV_addresses, mixer_addresses, wallet_addresses, other_addresses


def load_creation_info(dex='univ2'):
    print("LOAD CREATION INFO")
    creation_info = dict()
    pool_creation_path = os.path.join(eval('path.{}_pool_path'.format(dex)), "pool_creation_info.csv")
    token_creation_path = os.path.join(eval('path.{}_token_path'.format(dex)), "token_creation_info.csv")
    pool_creations = pd.read_csv(pool_creation_path)
    creation_info.update(dict(zip(pool_creations["contractAddress"].str.lower(), pool_creations["contractCreator"].str.lower())))
    token_creations = pd.read_csv(token_creation_path)
    creation_info.update(dict(zip(token_creations["contractAddress"].str.lower(), token_creations["contractCreator"].str.lower())))
    return creation_info


def load_rug_pull_dataset(dex='univ2'):
    print("LOAD RUG PULL INFO")
    scam_pools = list()
    # scammers = list()
    scammers = pd.read_csv(os.path.join(eval('path.{}_processed_path'.format(dex)), "1d_rp_scammers.csv"))
    index_issue = scammers[(scammers["pool"] == scammers["scammer"])].index
    scammers.drop(index_issue)
    scammers["pool"] = scammers["pool"].str.lower()
    scammers["scammer"] = scammers["scammer"].str.lower()
    scam_pools.extend(scammers["pool"].unique())
    pool_scammers = scammers.groupby('pool')['scammer'].apply(list).to_dict()
    rp_pools = pd.read_csv(os.path.join(eval('path.{}_processed_path'.format(dex)), "1d_rp.csv"))
    rp_pools.fillna("", inplace=True)
    rp_pools["pool"] = rp_pools["pool"].str.lower()
    rp_pools["scam_token"] = rp_pools["scam_token"].str.lower()
    scam_pools.extend(rp_pools["pool"].unique())
    scam_token_pool = dict(zip( rp_pools["scam_token"], rp_pools["pool"]))
    return pool_scammers, scam_token_pool, OrderedSet(scam_pools), scammers["scammer"].str.lower().to_list()


class DataLoader(object):
    def __init__(self, dex='univ2'):
        ### ALL ADDRESSES MUST BE IN LOWER CASES ###
        # sets of address
        (self.bridge_addresses,
         self.defi_addresses,
         self.MEV_addresses,
         self.mixer_addresses,
         self.wallet_addresses,
         self.other_addresses) = load_end_nodes(dex=dex)
        # key is token/pool address - value is creator address
        self.creators = load_creation_info(dex=dex)
        self.pool_scammers, self.scam_token_pool, self.scam_pools, self.scammers = load_rug_pull_dataset(dex=dex)


if __name__ == '__main__':
    dataloader = DataLoader(dex='univ2')
    print(dataloader.pool_scammers)
