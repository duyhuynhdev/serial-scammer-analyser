import matplotlib.pyplot as plt
from web3 import Web3
import networkx as nx
import pandas as pd
import os
from data_collection.EventCollector import PoolEventCollector
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


def get_base_datasets(dex='univ2'):
    end_nodes = pd.read_csv(os.path.join(eval('path.{}_account_path'.format(dex)), "end_nodes.csv"))["address"].str.lower().values
    scam_tokens = pd.read_csv(os.path.join(eval('path.{}_token_path'.format(dex)), "scam_tokens.csv"))["token"].str.lower().values
    scammers = pd.read_csv(os.path.join(eval('path.{}_account_path'.format(dex)), "scammers.csv"))["scammer"].str.lower().values
    scam_pools = pd.read_csv(os.path.join(eval('path.{}_pool_path'.format(dex)), "pool_labels.csv"))
    scam_pools = scam_pools[scam_pools["is_rp"] > 0]["pool"].str.lower().values
    return end_nodes, scam_tokens, scammers, scam_pools


def explore_scammer_network_by_ignoring_creator(scammer_address, dex='univ2'):
    if ut.is_contract_address(scammer_address):
        return None
    # end_nodes, scam_tokens, scammers, scam_pools = get_base_datasets(dex)
    end_nodes = pd.read_csv(os.path.join(eval('path.{}_account_path'.format(dex)), "end_nodes.csv"))["address"].str.lower().values
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
        root = queue.get()
        print("ROOT ADDRESS", root.address)
        print("PATH", "->".join(root.path()))

        if root.address in traversed_nodes or root.address in dead_nodes:
            continue
        traversed_nodes.add(root.address)
        eoa_nodes = (root.eoa_prev_neighbours | root.eoa_next_neighbours)
        contract_nodes = (root.contract_prev_neighbours | root.contract_next_neighbours)
        print("EOA NODES", len(eoa_nodes))
        print("CONTRACT NODES", len(contract_nodes))
        # EOA neighbours
        for eoa_neighbour_address in eoa_nodes:
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
        for contract_neighbour_address in contract_nodes:
            if contract_neighbour_address not in traversed_nodes:
                if not cluster.is_address_exist(contract_neighbour_address):
                    dead_nodes.add(contract_neighbour_address)
                    contract_end_node = Node.create_end_node(contract_neighbour_address, root)
                    cluster.add_node(contract_end_node)
        # add connections
        for tx in root.in_txs:
            cluster.add_connection(tx["from"], tx["to"], float(tx["value"]) / 10 ** 18, tx)
        for tx in root.out_txs:
            cluster.add_connection(tx["from"], tx["to"], float(tx["value"]) / 10 ** 18, tx)
        print("QUEUE LEN", queue.qsize())
        print("SCANNED NODES", len(traversed_nodes))
        print("DEAD NODES", len(dead_nodes))
        print("=" * 50)
    nx.draw(cluster.network)
    print(cluster.network.nodes())
    print(cluster.network.edges())
    plt.show()


def explore_scammer_network(scammer_address, dex='univ2'):
    if ut.is_contract_address(scammer_address):
        return None
    # end_nodes, scam_tokens, scammers, scam_pools = get_base_datasets(dex)
    end_nodes = pd.read_csv(os.path.join(eval('path.{}_account_path'.format(dex)), "end_nodes.csv"))["address"].str.lower().values
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
        root = queue.get()
        print("ROOT ADDRESS", root.address)
        print("PATH", "->".join(root.path()))

        if root.address in traversed_nodes or root.address in dead_nodes:
            continue
        traversed_nodes.add(root.address)
        eoa_nodes = (root.eoa_prev_neighbours | root.eoa_next_neighbours)
        contract_nodes = (root.contract_prev_neighbours | root.contract_next_neighbours)
        print("EOA NODES", len(eoa_nodes))
        print("CONTRACT NODES", len(contract_nodes))
        # EOA neighbours
        for eoa_neighbour_address in eoa_nodes:
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
        for contract_neighbour_address in contract_nodes:
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
        for tx in root.in_txs:
            cluster.add_connection(tx["from"], tx["to"], float(tx["value"]) / 10 ** 18, tx)
        for tx in root.out_txs:
            cluster.add_connection(tx["from"], tx["to"], float(tx["value"]) / 10 ** 18, tx)
        print("QUEUE LEN", queue.qsize())
        print("SCANNED NODES", len(traversed_nodes))
        print("DEAD NODES", len(dead_nodes))
        print("=" * 50)
    nx.draw(cluster.network)
    print(cluster.network.nodes())
    print(cluster.network.edges())
    plt.show()


if __name__ == '__main__':
    # explore_scammer_network("0x48f0fc8dfc672dd45e53b6c53cd5b09c71d9fbd6")
    explore_scammer_network_by_ignoring_creator("0x48f0fc8dfc672dd45e53b6c53cd5b09c71d9fbd6")
    # explore_scammer_network("0x81cfe8efdb6c7b7218ddd5f6bda3aa4cd1554fd2")
    # explore_scammer_network("0x30a9173686eb332ff2bdcea95da3b2860597a19d")
    # explore_scammer_network("0xb16a24e954739a2bbc68c5d7fbbe2e27f17dfff9")
    # print(is_contract_address("0x81cfe8efdb6c7b7218ddd5f6bda3aa4cd1554fd2"))
