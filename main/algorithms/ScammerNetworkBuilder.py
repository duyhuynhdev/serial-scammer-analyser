import matplotlib.pyplot as plt
import numpy as np
from tqdm import tqdm
from web3 import Web3
import networkx as nx
import pandas as pd
import os
from data_collection.EventCollector import ContractEventCollector
from data_collection.ContractCollector import PoolInfoCollector, TokenInfoCollector
from data_collection.AccountCollector import CreatorCollector
from entity import Node, OrderedQueue
from entity.Cluster import Cluster
from entity.OrderedQueue import OrderedSetQueue
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


def collect_end_nodes(dex='univ2'):
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
    return bridge_addresses | defi_addresses | MEV_addresses | mixer_addresses | wallet_addresses | other_addresses


def get_base_datasets(dex='univ2'):
    end_nodes = pd.read_csv(os.path.join(eval('path.{}_account_path'.format(dex)), "end_nodes.csv"))["address"].str.lower().values
    scam_tokens = pd.read_csv(os.path.join(eval('path.{}_token_path'.format(dex)), "scam_tokens.csv"))["token"].str.lower().values
    scammers = pd.read_csv(os.path.join(eval('path.{}_account_path'.format(dex)), "scammers.csv"))["scammer"].str.lower().values
    scam_pools = pd.read_csv(os.path.join(eval('path.{}_pool_path'.format(dex)), "pool_labels.csv"))
    scam_pools = scam_pools[scam_pools["is_rp"] > 0]["pool"].str.lower().values
    return end_nodes, scam_tokens, scammers, scam_pools


def get_rug_pull_creators(dex='univ2'):
    pool_labels_path = os.path.join(eval('path.{}_processed_path'.format(dex)), "pool_labels.csv")
    pool_labels_df = pd.read_csv(pool_labels_path)
    pool_labels_df.fillna("", inplace=True)
    scam_pools = pool_labels_df[pool_labels_df["is_rp"] != '0']
    scam_pools["pool"] = scam_pools["pool"].str.lower()
    pool_creation_path = os.path.join(eval('path.{}_pool_path'.format(dex)), "pool_creation_info.csv")
    pool_creations = pd.read_csv(pool_creation_path)
    pool_creations.drop_duplicates(inplace=True)
    scam_pool_full_info = pd.merge(scam_pools, pool_creations, left_on='pool', right_on='contractAddress', how='inner')
    return scam_pool_full_info["contractCreator"].str.lower().values


def scammer_clustering(dex='univ2'):
    scammers = get_rug_pull_creators(dex=dex)
    finish_set = set()
    for s in tqdm(scammers):
        if s in finish_set:
            continue
        cluster, scanned_nodes = explore_scammer_network(s, dex)
        finish_set.update(set(scanned_nodes))


def explore_scammer_network(scammer_address, dex='univ2'):
    cluster_path = eval('path.{}_cluster_path'.format(dex))
    if ut.is_contract_address(scammer_address):
        return None, list()
    # end_nodes, scam_tokens, scammers, scam_pools = get_base_datasets(dex)
    end_nodes = collect_end_nodes(dex=dex)
    creator_collector = CreatorCollector()
    # create node for an address with downloading all transactions and discovering/classifying neighbours
    node = Node.create_node(scammer_address, None, dex)
    queue = OrderedSetQueue()
    traversed_nodes = set()
    dead_nodes = set()
    queue.put(node)
    cluster = Cluster()
    cluster.add_node(node)
    # start exploring
    while not queue.empty():
        root: Node.Node = queue.get()
        print("ROOT ADDRESS", root.address)
        print("PATH", "->".join(root.path()))

        if root.address in traversed_nodes or root.address in dead_nodes:
            continue
        traversed_nodes.add(root.address)
        print("EOA NODES", len(root.eoa_neighbours))
        print("CONTRACT NODES", len(root.contract_neighbours))
        # EOA neighbours
        for eoa_neighbour_address in root.eoa_neighbours:
            eoa_neighbour_address = eoa_neighbour_address.lower()
            if eoa_neighbour_address not in traversed_nodes:
                if eoa_neighbour_address.lower() in end_nodes:
                    # add all end nodes into traversed nodes
                    dead_nodes.add(eoa_neighbour_address)
                    if not cluster.is_address_exist(eoa_neighbour_address):
                        eoa_end_node = Node.create_end_node(eoa_neighbour_address, root)
                        cluster.add_node(eoa_end_node)
                else:
                    if not cluster.is_address_exist(eoa_neighbour_address):
                        eoa_node = Node.create_node(eoa_neighbour_address, root, dex)
                        cluster.add_node(eoa_node)
                        # put unvisited neighbours into queue
                        queue.put(eoa_node)
        # Contract neighbours
        for contract_neighbour_address in root.contract_neighbours:
            contract_neighbour_address = contract_neighbour_address.lower()
            if contract_neighbour_address not in traversed_nodes:
                if contract_neighbour_address.lower() in end_nodes:
                    if not cluster.is_address_exist(contract_neighbour_address):
                        dead_nodes.add(contract_neighbour_address)
                        contract_end_node = Node.create_end_node(contract_neighbour_address, root)
                        cluster.add_node(contract_end_node)
                else:
                    contract_creation = creator_collector.get_contract_creator(contract_neighbour_address, dex)
                    if contract_creation is not None:
                        contract_creator_address = contract_creation["contractCreator"]
                        contract_creator_address = contract_creator_address.lower()
                        if ut.is_contract_address(contract_creator_address) or contract_creator_address.lower() == scammer_address.lower():
                            if not cluster.is_address_exist(contract_neighbour_address):
                                dead_nodes.add(contract_neighbour_address)
                                contract_end_node = Node.create_end_node(contract_neighbour_address, root)
                                cluster.add_node(contract_end_node)
                        else:
                            if not cluster.is_address_exist(contract_neighbour_address) and not cluster.is_address_exist(contract_creator_address):
                                contract_node = Node.create_end_node(contract_neighbour_address, root)
                                creator_node = Node.create_node(contract_creator_address, contract_node, dex)
                                cluster.add_node(contract_node)
                                cluster.add_node(creator_node)
                                queue.put(creator_node)
                                cluster.add_connection(creator_node.address, contract_node.address, 0, contract_creation)
                    else:
                        if not ut.is_contract_address(contract_neighbour_address) and cluster.is_address_exist(contract_neighbour_address):
                            eoa_node = Node.create_node(contract_neighbour_address, root, dex)
                            cluster.add_node(eoa_node)
                            # put unvisited neighbours into queue
                            queue.put(eoa_node)
        # add connections
        for tx in root.normal_txs:
            cluster.add_connection(tx.sender, tx.to, float(tx.value) / 10 ** 18, tx)
        for tx in root.internal_txs:
            cluster.add_connection(tx.sender, tx.to, float(tx.value) / 10 ** 18, tx)
        print("QUEUE LEN", queue.qsize())
        print("SCANNED NODES", len(traversed_nodes))
        print("DEAD NODES", len(dead_nodes))
        print("=" * 50)
    cluster.export(cluster_path)
    print(cluster.network.nodes())
    print(cluster.network.edges())
    return cluster, set(traversed_nodes | dead_nodes)


if __name__ == '__main__':
    # explore_scammer_network("0x48f0fc8dfc672dd45e53b6c53cd5b09c71d9fbd6")
    # explore_scammer_network("0xad21f63b8290f26ef1bddb6738cfb892309cefa6")
    # explore_scammer_network_by_ignoring_creator("0x48f0fc8dfc672dd45e53b6c53cd5b09c71d9fbd6")
    # explore_scammer_network("0x81cfe8efdb6c7b7218ddd5f6bda3aa4cd1554fd2")
    # explore_scammer_network("0x30a9173686eb332ff2bdcea95da3b2860597a19d")
    # explore_scammer_network("0xb16a24e954739a2bbc68c5d7fbbe2e27f17dfff9")
    # print(is_contract_address("0x81cfe8efdb6c7b7218ddd5f6bda3aa4cd1554fd2"))
    # print(len(collect_end_nodes()))
    scammer_clustering()
