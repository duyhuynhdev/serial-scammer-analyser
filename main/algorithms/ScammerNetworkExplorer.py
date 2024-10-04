import sys
import os

from pycparser.c_ast import Constant

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
    "max_iter": 100
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
        return None, list()
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
    return cluster, it


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
    cluster, it = None, 0
    if len(scammers) > 0:
        print("*" * 100)
        print(f"START CLUSTERING (ADDRESS {scammers[0]}) GROUP {group_id}")
        cluster, it = explore_scammer_network(group_id, scammers, node_factory, dex)
        print(f"END CLUSTERING (ADDRESS {scammers[0]}) GROUP {group_id}")
        print("*" * 100)
    if config["is_sync_s3"]:
        s3_file_manager.sync()
    return cluster, it


if __name__ == '__main__':
    run_clustering(2000)
