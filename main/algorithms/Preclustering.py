import sys
import os

sys.path.append(os.path.join(os.path.dirname(sys.path[0])))

from data_collection.AccountCollector import TransactionCollector
from data_collection.EventCollector import ContractEventCollector
from utils import DataLoader
from utils.Settings import Setting
from utils.ProjectPath import ProjectPath
from utils import Utils as ut
import networkx as nx
import itertools
from tqdm import tqdm

path = ProjectPath()
setting = Setting()
# dataloader = DataLoader()
contract_event_collector = ContractEventCollector()
transaction_collector = TransactionCollector()


def get_related_scammer_from_pool_events(pool, event_path, scammers, dex='univ2'):
    pool_transfers = contract_event_collector.get_event(pool, "Transfer", event_path, dex)
    pool_swaps = contract_event_collector.get_event(pool, "Swap", event_path, dex)
    connected_scammer = set()
    for transfer in pool_transfers:
        if transfer["sender"].lower() in scammers:
            connected_scammer.add(transfer["sender"].lower())
        if transfer["to"].lower() in scammers:
            connected_scammer.add(transfer["to"].lower())
    for swap in pool_swaps:
        if swap["sender"].lower() in scammers:
            connected_scammer.add(swap["sender"].lower())
        if swap["to"].lower() in scammers:
            connected_scammer.add(swap["to"].lower())
    print(f"FOUND {len(connected_scammer)} SCAM INVESTOR FROM POOL {pool}")
    return connected_scammer


def get_scam_neighbours(address, scammers, dex='univ2'):
    normal_txs, _ = transaction_collector.get_transactions(address, dex)
    connected_scammer = set()
    for tx in normal_txs:
        if tx.sender.lower() in scammers:
            connected_scammer.add(tx.sender.lower())
        if not isinstance(tx.to, float) and tx.to != "" and tx.to.lower() in scammers:
            connected_scammer.add(tx.to.lower())
    print(f"FOUND {len(connected_scammer)} SCAM NEIGHBOURS FROM ADDRESS {address}")
    return connected_scammer


def scammer_grouping(dex='univ2'):
    graph = nx.Graph()
    event_path = eval('path.{}_pool_events_path'.format(dex))
    (
        pool_scammers,
        _,
        _,
        total_scammers,
        _,
    ) = DataLoader.load_rug_pull_dataset(dex=dex, scammer_file_name="filtered_simple_rp_scammers.csv", pool_file_name="filtered_simple_rp_pool.csv")
    for pool in tqdm(pool_scammers):
        scammers = set(pool_scammers[pool])
        # scammers.update(get_related_scammer_from_pool_events(pool, event_path, total_scammers, dex))
        scam_neighbours = set()
        for s in scammers:
            sn = get_scam_neighbours(s, total_scammers, dex)
            scam_neighbours.update(sn)
        scammers.update(scam_neighbours)
        if len(scammers) == 1:
            scammer = scammers.pop()
            if not graph.has_node(scammer):
                graph.add_node(scammer)
        else:
            adj_list = list(itertools.combinations(scammers, 2))
            for u, v in adj_list:
                graph.add_edge(u, v)

    print("GRAPH HAVE", len(nx.nodes(graph)), "NODES")
    groups = list(nx.connected_components(graph))
    isolates = set(nx.isolates(graph))
    return groups, isolates


def pre_clusterting(dex='univ2'):
    file_path = os.path.join(eval('path.{}_processed_path'.format(dex)), "non_swap_simple_rp_scammer_group.csv")
    groups, isolates = scammer_grouping(dex)
    data = []
    id = 1
    existing_scammers = set()
    for group in groups:
        for s in group:
            if s not in existing_scammers:
                data.append({"group_id": id, "scammer": s})
                existing_scammers.add(s)
        id += 1
    for i in isolates:
        if i not in existing_scammers:
            data.append({"group_id": id, "scammer": i})
            id += 1
    print("DATA SIZE", len(data))
    ut.save_overwrite_if_exist(data, file_path)


if __name__ == '__main__':
    pre_clusterting(dex='panv2')
