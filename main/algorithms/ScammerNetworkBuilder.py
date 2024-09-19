import os

from tqdm import tqdm
from data_collection.AccountCollector import CreatorCollector
from entity import Node
from entity.Cluster import Cluster
from entity.Node import NodeLabel
from entity.OrderedQueue import OrderedQueue
from utils.DataLoader import DataLoader
from utils.Settings import Setting
from utils.Path import Path
from utils import Utils as ut
from utils import Constant
import logging

path = Path()
setting = Setting()
dataloader = DataLoader()


def scammer_clustering(dex='univ2'):
    finish_set = {}
    for rp in tqdm(dataloader.scam_pools):
        scammers = dataloader.pool_scammers[rp]
        scammers = [s for s in scammers if s.lower() not in finish_set and s not in finish_set]
        if len(scammers) == 0:
            continue
        print("*" * 200)
        print(f"START CLUSTERING (ADDRESS {scammers[0]})")
        cluster, scanned_nodes = explore_scammer_network(scammers, dex)
        finish_set.update(set(scanned_nodes))
        print(f"END CLUSTERING (ADDRESS {scammers[0]})")
        print("*" * 200)


def check_if_end_node(address):
    if address.lower() in dataloader.bridge_addresses:
        return True, NodeLabel.BRIDGE
    if address.lower() in dataloader.defi_addresses:
        return True, NodeLabel.DEFI
    if address.lower() in dataloader.MEV_addresses:
        return True, NodeLabel.MEV
    if address.lower() in dataloader.mixer_addresses:
        return True, NodeLabel.MIXER
    if address.lower() in dataloader.wallet_addresses:
        return True, NodeLabel.WALLET
    if address.lower() in dataloader.other_addresses:
        return True, NodeLabel.OTHER
    return False, None

# load if exist or create a new cluster
def init(scammer_address, scammers, cluster_path, dex):
    # create node for an address with downloading all transactions and discovering/classifying neighbours
    cluster = Cluster(scammer_address)
    cluster.load_cluster(cluster_path)
    queue, traversed_nodes = cluster.read_queue(cluster_path, dataloader)
    node = Node.create_node(scammer_address, [], dataloader, NodeLabel.SCAMMER, dex)
    for s in [n for n in scammers if n != scammer_address]:
        node.eoa_neighbours.add(s)
    if node.address not in traversed_nodes:
        queue.put(node)
        cluster.add_node(node)
    return cluster, queue, traversed_nodes


def explore_scammer_network(scammers, dex='univ2'):
    cluster_path = eval('path.{}_cluster_path'.format(dex))
    scammer_address = scammers[0]
    if ut.is_contract_address(scammer_address):
        return None, list()
    # create node for an address with downloading all transactions and discovering/classifying neighbours
    cluster, queue, traversed_nodes = init(scammer_address, scammers, cluster_path, dex)
    count = 0
    # start exploring
    while not queue.empty():
        print("QUEUE LEN", queue.qsize())
        print("SCANNED NODES", len(traversed_nodes))

        root: Node.Node = queue.get()
        print("\t ROOT ADDRESS", root.address)
        print("\t PATH", " -> ".join(root.path))
        if root.address in traversed_nodes:
            continue
        traversed_nodes.add(root.address)
        print("\t EOA NODES", len(root.eoa_neighbours))
        print("\t CONTRACT NODES", len(root.contract_neighbours))
        print("\t LABELS", root.labels)

        if NodeLabel.BIG in root.labels:
            if (NodeLabel.COORDINATOR not in root.labels) and (NodeLabel.WASHTRADER not in root.labels) and (root.address.lower() not in dataloader.scammers):
                print("SKIP BIG NODE")
                print("=" * 100)
                continue
            if ut.is_contract_address(root.address):
                print("SKIP CONTRACT NODE")
                print("=" * 100)
                continue
        # EOA neighbours
        for eoa_neighbour_address in root.eoa_neighbours:
            eoa_neighbour_address = eoa_neighbour_address.lower()
            if (eoa_neighbour_address not in traversed_nodes) and (eoa_neighbour_address not in queue.addresses):
                check_endnode, label = check_if_end_node(eoa_neighbour_address)
                if check_endnode:
                    # add all end nodes into traversed nodes
                    traversed_nodes.add(eoa_neighbour_address)
                    if not cluster.is_address_exist(eoa_neighbour_address):
                        eoa_end_node = Node.create_end_node(eoa_neighbour_address, root.path, label)
                        cluster.add_node(eoa_end_node)
                else:
                    if not cluster.is_address_exist(eoa_neighbour_address):
                        label = NodeLabel.EOA
                        if eoa_neighbour_address.lower() in dataloader.scammers:
                            label = NodeLabel.SCAMMER
                        eoa_node = Node.create_node(eoa_neighbour_address, root.path, dataloader, label, dex)
                        cluster.add_node(eoa_node)
                        # put unvisited neighbours into queue
                        queue.put(eoa_node)
        # Contract neighbours
        for contract_neighbour_address in root.contract_neighbours:
            contract_neighbour_address = contract_neighbour_address.lower()
            if contract_neighbour_address not in traversed_nodes:
                if not cluster.is_address_exist(contract_neighbour_address):
                    traversed_nodes.add(contract_neighbour_address)
                    check_endnode, label = check_if_end_node(contract_neighbour_address)
                    if check_endnode:
                        contract_end_node = Node.create_end_node(contract_neighbour_address, root.path, label)
                        cluster.add_node(contract_end_node)
                    else:
                        contract_end_node = Node.create_end_node(contract_neighbour_address, root.path, NodeLabel.UC)
                    cluster.add_node(contract_end_node)
        count += 1
        if count == 10:
            print(">>> SAVE QUEUE & CLUSTER STATE")
            cluster.export(cluster_path)
            cluster.write_queue(cluster_path, queue, traversed_nodes)
            count = 0
        print("=" * 100)
    cluster.export(cluster_path)
    cluster.write_queue(cluster_path, queue, traversed_nodes)
    return cluster, set(traversed_nodes)


if __name__ == '__main__':
    # explore_scammer_network("0x19b98792e98c54f58c705cddf74316aec0999aa6")
    # explore_scammer_network("0x43e129c47dfd4abcf48a24f6b2d8ba6f49261f39")
    # explore_scammer_network_by_ignoring_creator("0x48f0fc8dfc672dd45e53b6c53cd5b09c71d9fbd6")
    # explore_scammer_network("0x81cfe8efdb6c7b7218ddd5f6bda3aa4cd1554fd2")
    # explore_scammer_network("0x30a9173686eb332ff2bdcea95da3b2860597a19d")
    # explore_scammer_network("0xb16a24e954739a2bbc68c5d7fbbe2e27f17dfff9")
    # print(is_contract_address("0x81cfe8efdb6c7b7218ddd5f6bda3aa4cd1554fd2"))
    # print(len(collect_end_nodes()))
    scammer_clustering()
    # print(ut.hex_to_dec("0x10afe6222f") * ut.hex_to_dec("0x29cbe2")/ 10**18)
    # print((107143841398 * 2208003)/ 10**18)
