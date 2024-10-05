import sys
import os

import pandas as pd
from pycparser.c_ast import Constant
from tqdm import tqdm

sys.path.append(os.path.join(os.path.dirname(sys.path[0])))

from entity.LightCluster import LightCluster
from entity.LightNode import LightNodeFactory, LightNode, LightNodeLabel
from entity.OrderedQueue import OrderedQueue

from utils.DataLoader import DataLoader
from utils.S3Syncer import S3Syncer
from utils.Settings import Setting
from utils.ProjectPath import ProjectPath
from utils import Utils as ut
from utils import Constant

path = ProjectPath()
setting = Setting()
dataloader = DataLoader()
config = {
    "is_sync_s3": False,
    "is_load_from_last_run": False,
    "is_max_iter": False,
    "max_iter": 500
}


def init(group_id, scammer_address, scammers, cluster_path, node_factory):
    queue = OrderedQueue()
    traversed_nodes = set()
    cluster = LightCluster(group_id)
    if config["is_load_from_last_run"]:
        cluster.load(cluster_path)
        queue, traversed_nodes = cluster.read_queue(cluster_path, dataloader)
    if scammer_address not in traversed_nodes:
        node = node_factory.createNode(scammer_address, [], cluster.id)
        for s in scammers:
            node.valid_neighbours.append(s)
        queue.put(node)
        cluster.add_node(node)
    return cluster, queue, traversed_nodes


def is_slave_PA(suspected_node, target_node):
    for tx in suspected_node.normal_txs:
        # check if there is any tx from suspected_node to target_node with small value
        if tx.is_out_tx(suspected_node.address) and tx.to == target_node.address and float(tx.value) / 1e18 < Constant.SMALL_VALUE:
            time_in = int(tx.timeStamp)
            # get a list of addresses that target node sends tx to before time_in
            out_adds = set([tx_out.to for tx_out in target_node.normal_txs if tx_out.is_out_tx(target_node.address) and int(tx_out.timeStamp) < time_in])
            # try:
            #     tmp = []
            #     for tx_out in target_node.normal_txs:
            #         if tx_out.is_out_tx(target_node.address) and int(tx_out.timeStamp) < time_in:
            #             tmp.append(tx_out.to)
            #
            #     out_adds = set(tmp)
            # except Exception as e:
            #     print(f"tx_out.to = {tx_out.to}")
            # check if the address of suspected node is similar to any address in the out_adds list of the target node
            for out_add in out_adds:
                if suspected_node.address[0:3] == out_add[0:3] and suspected_node.address[-3:] == out_add[-3:]:
                    print(f"phishing_add = {suspected_node.address}, victim_add = {target_node.address}, 'similar_add = {out_add}")
                    return True
    return False


def explore_scammer_network(group_id, scammers, node_factory, dex='univ2'):
    cluster_path = eval('path.{}_cluster_path'.format(dex))
    scammers = [s for s in scammers if not ut.is_contract_address(s)]
    if len(scammers) == 0:
        return None, list(), 0
    scammer_address = scammers[0]
    cluster, queue, traversed_nodes = init(group_id, scammer_address, scammers, cluster_path, node_factory)
    suspicious_big_nodes = []
    it = 0
    while not queue.empty():
        it += 1
        print("*" * 100)
        print("GROUP:", group_id)
        print("QUEUE LEN:", queue.qsize())
        print("TRAVERSED NODES:", len(traversed_nodes))
        print("ITERATION:", it)
        root: LightNode = queue.get()
        print("\t ROOT ADDRESS", root.address)
        print("\t VALID NEIGHBOURS", len(root.valid_neighbours))
        print("\t LABELS", root.labels)
        print("\t PATH", " -> ".join(root.path))
        if LightNodeLabel.BOUNDARY in root.labels:
            print(f"\t REACH BOUNDARY AT {root.address} >> SKIP")
            continue
        if root.address.lower() in traversed_nodes:
            print(f"\t {root.address} HAS BEEN VISITED >> SKIP")
            continue
        traversed_nodes.add(root.address.lower())

        for neighbour_address in root.valid_neighbours:
            neighbour_address = neighbour_address.lower()
            if ((neighbour_address not in traversed_nodes)
                    and (neighbour_address not in queue.addresses)
                    and not cluster.is_address_exist(neighbour_address)):
                node = node_factory.createNode(neighbour_address, root.path, cluster.id)
                if not is_slave_PA(node, root) and not any(label in LightNodeLabel.SKIP_LABELS for label in node.labels):
                    if LightNodeLabel.BIG_CONNECTOR in node.labels:
                        suspicious_big_nodes.append(LightNode.to_sort_dict(node))
                    cluster.add_node(node)
                    queue.put(node)

        if it % 10 == 0:
            print(">>> SAVE QUEUE & CLUSTER STATE <<<")
            cluster.save(cluster_path)
            cluster.write_queue(cluster_path, queue, traversed_nodes)
            ut.save_overwrite_if_exist(suspicious_big_nodes, os.path.join(cluster_path, f"cluster_{cluster.id}_suspicious_nodes.csv"))
        print("*" * 100)
        if config["is_max_iter"] and config["max_iter"] <= it:
            break
    cluster.save(cluster_path)
    cluster.write_queue(cluster_path, queue, traversed_nodes)
    ut.save_overwrite_if_exist(suspicious_big_nodes, os.path.join(cluster_path, f"cluster_{cluster.id}_suspicious_nodes.csv"))
    return cluster, queue, it


def run_clustering(group_id, dex='univ2'):
    node_factory = LightNodeFactory(dataloader, dex)
    account_path = eval(f"path.{dex}_account_path")
    s3_file_manager = S3Syncer(abs_local_dir=account_path)
    if config["is_sync_s3"]:
        s3_file_manager.sync()
    if group_id not in dataloader.group_scammers.keys():
        print(f"CANNOT FIND GROUP {group_id}")
        return None,
    scammers = dataloader.group_scammers[group_id]
    print(f"LOAD {len(scammers)} SCAMMER FROM GROUP {group_id}")
    scammers.sort()
    cluster, queue, it = None, [], 0
    if len(scammers) > 0:
        print("*" * 100)
        print(f"START CLUSTERING (ADDRESS {scammers[0]}) GROUP {group_id}")
        cluster, queue, it = explore_scammer_network(group_id, scammers, node_factory, dex)
        print(f"END CLUSTERING (ADDRESS {scammers[0]}) GROUP {group_id}")
        print("*" * 100)
    if config["is_sync_s3"]:
        s3_file_manager.sync()
    return cluster,queue, it

def explore_with_max_iter(job, max_iter = 100, size = 20000, dex='univ2'):
    global config
    file_path =  os.path.join(eval(f'path.{dex}_processed_path'), "max_iter_cluster_results.csv")
    config["is_max_iter"] = True
    config["max_iter"] = max_iter
    processed_gids = []
    if os.path.exists(file_path):
        df = pd.read_csv(file_path)
        processed_gids = df["start_gid"].values.tolist()
    groups =  list(dataloader.group_scammers.keys())
    chunks = ut.partitioning(0, len(groups), size)
    print("NUM CHUNKS", len(chunks), "JOB", job)
    chunk = chunks[job]
    chunk_groups = groups[chunk["from"]:(chunk["to"] + 1)]
    print(f'START EXPLORING NETWORK (JOB {job}/ {len(chunks)}):{chunk["from"]}_{chunk["to"]} (size: {len(chunk_groups)})')
    for gid in tqdm(chunk_groups):
        if gid in processed_gids:
            continue
        cluster, queue, it = run_clustering(gid, dex)
        record = {
            "gid": gid,
            "cluster_size": len(cluster.nodes),
            "queue_size": queue.qsize(),
            "groups": "-".join([str(g) for g in cluster.groups]),
            "num_iter": it
        }
        ut.save_or_append_if_exist([record], file_path)


if __name__ == '__main__':
    job = 13
    explore_with_max_iter(job, 500, 5000)
    # run_clustering(5002)
