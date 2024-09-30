
import sys
import os

import pandas as pd

sys.path.append( os.path.join(os.path.dirname(sys.path[0])))

import numpy as np
from data_collection.AccountCollector import CreatorCollector, TransactionCollector
from data_collection.EventCollector import ContractEventCollector
from entity import Node
from entity.Cluster import Cluster
from entity.Node import NodeLabel
from utils.DataLoader import DataLoader
from utils.S3Syncer import S3Syncer
from utils.Settings import Setting
from utils.ProjectPath import ProjectPath
from utils import Utils as ut
import networkx as nx
import itertools
from tqdm import tqdm
path = ProjectPath()
setting = Setting()
dataloader = DataLoader()
contract_event_collector = ContractEventCollector()
transaction_collector = TransactionCollector()


def get_related_scammer_from_pool_events(pool, event_path, dataloader, dex='univ2'):
    pool_transfers = contract_event_collector.get_event(pool, "Transfer", event_path, dex)
    pool_swaps = contract_event_collector.get_event(pool, "Swap", event_path, dex)
    connected_scammer = set()
    for transfer in pool_transfers:
        if transfer["sender"].lower() in dataloader.scammers:
            connected_scammer.add(transfer["sender"].lower())
        if transfer["to"].lower() in dataloader.scammers:
            connected_scammer.add(transfer["to"].lower())
    for swap in pool_swaps:
        if swap["sender"].lower() in dataloader.scammers:
            connected_scammer.add(swap["sender"].lower())
        if swap["to"].lower() in dataloader.scammers:
            connected_scammer.add(swap["to"].lower())
    print(f"FOUND {len(connected_scammer)} SCAM INVESTOR FROM POOL {pool}")
    return connected_scammer


def get_scam_neighbours(address, dataloader, dex='univ2'):
    normal_txs, _ = transaction_collector.get_transactions(address, dex)
    connected_scammer = set()
    for tx in normal_txs:
        if tx.sender.lower() in dataloader.scammers:
            connected_scammer.add(tx.sender.lower())
        if not isinstance(tx.to, float) and tx.to != "" and tx.to.lower() in dataloader.scammers:
            connected_scammer.add(tx.to.lower())
    print(f"FOUND {len(connected_scammer)} SCAM NEIGHBOURS FROM ADDRESS {address}")
    return connected_scammer


def scammer_grouping(dex='univ2'):
    graph = nx.Graph()
    event_path = eval('path.{}_pool_events_path'.format(dex))
    for pool in tqdm(dataloader.pool_scammers):
        scammers = set(dataloader.pool_scammers[pool])
        scammers.update(get_related_scammer_from_pool_events(pool, event_path, dataloader, dex))
        scam_neighbours = set()
        for s in scammers:
            sn = get_scam_neighbours(s, dataloader, dex)
            scam_neighbours.update(sn)
        scammers.update(scam_neighbours)
        if len(scammers) == 1:
            for ss in scammers:
                if not graph.has_node(ss):
                    graph.add_node(ss)
        adj_list = list(itertools.combinations(scammers, 2))
        for u, v in adj_list:
            graph.add_edge(u, v)
    print("GRAPH HAVE", len(nx.nodes(graph)), "NODES")
    groups = list(nx.connected_components(graph))
    isolates = set(nx.isolates(graph))
    return groups, isolates


def pre_clusterting(dex='univ2'):
    file_path = os.path.join(eval('path.{}_processed_path'.format(dex)), "scammer_group.csv")
    groups, isolates = scammer_grouping(dex)
    data = []
    id = 1
    for group in groups:
        for s in group:
            data.append({"group_id": id, "scammer": s})
        id += 1
    for i in isolates:
        data.append({"group_id": id, "scammer": s})
        id += 1
    print("DATA SIZE", len(data))
    ut.save_overwrite_if_exist(data, file_path)


def is_eoa_node(node):
    if NodeLabel.CONTRACT in node.labels:
        return False
    for ntx in node.normal_txs:
        if ntx.sender.lower() == node.address.lower():  # sender of normal txs must be EOA
            return True
        if ntx.is_creation_contract() and ntx.contractAddress.lower() == node.address.lower():  # address is a contract created by an EOA
            return False
    for itx in node.internal_txs:
        if itx.sender.lower() == node.address.lower():  # sender of normal txs must be contract
            return False
        if itx.is_creation_contract() and itx.contractAddress.lower() == node.address.lower():  # address is a contract created by a contract
            return False
    return True


def is_end_node(address):
    if address.lower() in dataloader.bridge_addresses:
        return True, NodeLabel.BRIDGE
    if address.lower() in dataloader.cex_addresses:
        return True, NodeLabel.CEX
    if address.lower() in dataloader.MEV_addresses:
        return True, NodeLabel.MEV
    if address.lower() in dataloader.mixer_addresses:
        return True, NodeLabel.MIXER
    if address.lower() in dataloader.wallet_addresses:
        return True, NodeLabel.WALLET
    if address.lower() in dataloader.other_addresses:
        return True, NodeLabel.OTHER
    return False, None


def is_valid_neighbour(node):
    if is_eoa_node(node) and (NodeLabel.BIG not in node.labels
                              or NodeLabel.COORDINATOR in node.labels
                              or NodeLabel.WASHTRADER in node.labels):
        return True
    return False


def run_clustering(group_id, dex='univ2', max_iter = 0):
    # account_path = eval(f"path.{dex}_account_path")
    # s3_file_manager = S3Syncer(abs_local_path=account_path)
    # s3_file_manager.sync()
    if group_id not in dataloader.group_scammers.keys():
        print(f"CANNOT FIND GROUP {group_id}")
        return None,
    scammers = dataloader.group_scammers[group_id]
    print(f"LOAD {len(scammers)} SCAMMER FROM GROUP {group_id}")
    scammers.sort()
    cluster, existing_groups, it = None, set(), 0
    if len(scammers) > 0:
        print("*" * 100)
        print(f"START CLUSTERING (ADDRESS {scammers[0]}) GROUP {group_id}")
        cluster, existing_groups, it = explore_scammer_network(group_id, scammers, dex, max_iter)
        print(f"END CLUSTERING (ADDRESS {scammers[0]}) GROUP {group_id}")
        print("*" * 100)
    # s3_file_manager.sync()
    return cluster, existing_groups, it


# load if exist or create a new cluster
def init(group_id, scammer_address, scammers, cluster_path, dex):
    # create node for an address with downloading all transactions and discovering/classifying neighbours
    existing_groups = {group_id}
    cluster = Cluster(group_id)
    cluster.load_cluster(cluster_path)
    queue, traversed_nodes = cluster.read_queue(cluster_path, dataloader)
    node = Node.create_node(scammer_address, [], dataloader, existing_groups, dex)
    for s in [n for n in scammers if n != scammer_address]:
        node.eoa_neighbours.add(s)
    if node.address not in traversed_nodes:
        queue.put(node)
        cluster.add_node(node)
    return cluster, queue, traversed_nodes, existing_groups


def explore_scammer_network(group_id, scammers, dex='univ2', max_iter = 0):
    cluster_path = eval('path.{}_cluster_path'.format(dex))
    scammer_address = scammers[0]
    if ut.is_contract_address(scammer_address):
        return None, list()
    # create node for an address with downloading all transactions and discovering/classifying neighbours
    cluster, queue, traversed_nodes, existing_groups = init(group_id, scammer_address, scammers, cluster_path, dex)
    count = 0
    # start exploring
    it = 0
    while not queue.empty():
        print("QUEUE LEN:", queue.qsize())
        print("SCANNED NODES:", len(traversed_nodes))
        print("GROUPS:", len(existing_groups))
        print("ITERATION:", it)
        root: Node.Node = queue.get()
        if not is_eoa_node(root):
            print("ROOT IS A CONTRACT >> SKIP")
            continue
        print("\t ROOT ADDRESS", root.address)
        print("\t PATH", " -> ".join(root.path))
        if root.address in traversed_nodes:
            continue
        traversed_nodes.add(root.address)
        print("\t EOA NODES", len(root.eoa_neighbours))
        print("\t CONTRACT NODES", len(root.contract_neighbours))
        print("\t LABELS", root.labels)
        # EOA neighbours
        it += 1
        for eoa_neighbour_address in root.eoa_neighbours:
            eoa_neighbour_address = eoa_neighbour_address.lower()
            if ((eoa_neighbour_address not in traversed_nodes)
                    and (eoa_neighbour_address not in queue.addresses)
                    and not cluster.is_address_exist(eoa_neighbour_address)):
                endnode_check, endnode_label = is_end_node(eoa_neighbour_address)
                if endnode_check:
                    # add all end nodes into traversed nodes
                    traversed_nodes.add(eoa_neighbour_address)
                    eoa_end_node = Node.create_end_node(eoa_neighbour_address, root.path, endnode_label)
                    cluster.add_node(eoa_end_node)
                else:
                    eoa_node = Node.create_node(eoa_neighbour_address, root.path, dataloader, existing_groups, dex)
                    cluster.add_node(eoa_node)
                    if is_valid_neighbour(eoa_node):
                        queue.put(eoa_node)
                    else:
                        traversed_nodes.add(eoa_neighbour_address)
        # Create end nodes for all contract neighbours
        for contract_neighbour_address in root.contract_neighbours:
            contract_neighbour_address = contract_neighbour_address.lower()
            if contract_neighbour_address not in traversed_nodes and not cluster.is_address_exist(contract_neighbour_address):
                traversed_nodes.add(contract_neighbour_address)
                endnode_check, endnode_label = is_end_node(contract_neighbour_address)
                contract_end_node = Node.create_end_node(contract_neighbour_address, root.path, endnode_label if endnode_check else NodeLabel.CONTRACT)
                cluster.add_node(contract_end_node)
        count += 1
        if count == 10:
            print(">>> SAVE QUEUE & CLUSTER STATE <<<")
            cluster.export(cluster_path)
            cluster.write_queue(cluster_path, queue, traversed_nodes)
            count = 0
        print("=" * 100)
        if 0 < max_iter <= it:
            break
    cluster.export(cluster_path)
    cluster.write_queue(cluster_path, queue, traversed_nodes)
    return cluster, existing_groups, it

def explore_with_max_iter(dex='univ2'):
    file_path =  os.path.join(eval(f'path.{dex}_processed_path'), "max_iter_cluster_results.csv")
    max_iter = 100
    data = []
    processed_gids = []
    if os.path.exists(file_path):
        df = pd.read_csv(file_path)
        processed_gids = df["start_gid"].values.tolist()
    for gid in dataloader.group_scammers.keys():
        if gid in processed_gids:
            continue
        cluster, existing_groups, it = run_clustering(gid, dex, max_iter)
        record = {
            "start_gid": gid,
            "cluster_size": len(cluster.nodes),
            "groups": "-".join([str(g) for g in existing_groups]),
            "num_iter": it
        }
        data.append(record)
        if len(data) >= 10:
            ut.save_or_append_if_exist(data, file_path)
            data = []
    if len(data) > 0:
        ut.save_or_append_if_exist(data, file_path)


if __name__ == '__main__':
    # run_clustering(1)
    # explore_with_max_iter()
    # node = Node.create_node("0x7e6c3ab97ca86571778375cbb90abfeb6a4e1f7a",[] ,dataloader, 'univ2')
    # print(len(node.eoa_neighbours))
    print(len(dataloader.scam_token_pool))