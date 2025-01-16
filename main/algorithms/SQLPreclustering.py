import sys
import os

from sql.DataQuerier import DataQuerier

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
dex='univ2'
# dex='panv2'
querier = DataQuerier(dex)

def get_related_scammer_from_pool_events(pool_address, scammers):
    pool_transfers = querier.get_pool_transfer(pool_address)
    pool_swaps = querier.get_pool_swap(pool_address)
    connected_scammer = set()
    for transfer in pool_transfers:
        if transfer.sender.lower() in scammers:
            connected_scammer.add(transfer.sender.lower())
        if transfer.receiver.lower() in scammers:
            connected_scammer.add(transfer.receiver.lower())
    for swap in pool_swaps:
        if swap.sender.lower() in scammers:
            connected_scammer.add(swap.sender.lower())
        if swap.receiver.lower().lower() in scammers:
            connected_scammer.add(swap.receiver.lower())
    print(f"FOUND {len(connected_scammer)} SCAM INVESTOR FROM POOL {pool_address}")
    return connected_scammer


def get_scam_neighbours(address, scammers):
    normal_txs= querier.get_normal_transactions(address)
    connected_scammer = set()
    for tx in normal_txs:
        if tx.sender.lower() in scammers:
            connected_scammer.add(tx.sender.lower())
        if not tx.is_contract_creation and tx.receiver.lower() in scammers:
            connected_scammer.add(tx.receiver.lower())
    print(f"FOUND {len(connected_scammer)} SCAM NEIGHBOURS FROM ADDRESS {address}")
    return connected_scammer


def scammer_grouping():
    graph = nx.Graph()
    (
        pool_scammers,
        _,
        _,
        total_scammers,
        _,
    ) = DataLoader.load_rug_pull_dataset(dex=dex, scammer_file_name="filtered_simple_rp_scammers.csv", pool_file_name="filtered_simple_rp_pool.csv")
    for pool in tqdm(pool_scammers):
        scammers = set(pool_scammers[pool])
        scammers.update(get_related_scammer_from_pool_events(pool,total_scammers))
        scam_neighbours = set()
        for s in scammers:
            sn = get_scam_neighbours(s, total_scammers)
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
    return groups, isolates, graph


def pre_clusterting():
    file_path = os.path.join(eval('path.{}_processed_path'.format(dex)), "new_sql_scammer_group.csv")
    groups, isolates, graph = scammer_grouping()
    data = []
    id = 1
    existing_scammers = set()
    for group in groups:
        for s in group:
            if s not in existing_scammers:
                data.append({"group_id": id, "scammer": s, "degree": graph.degree[s]})
                existing_scammers.add(s)
        id += 1
    for i in isolates:
        if i not in existing_scammers:
            data.append({"group_id": id, "scammer": i, "degree": graph.degree[i]})
            id += 1
    print("DATA SIZE", len(data))
    ut.save_overwrite_if_exist(data, file_path)


if __name__ == '__main__':
    pre_clusterting()
