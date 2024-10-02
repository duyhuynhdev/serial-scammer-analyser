import sys
import os

sys.path.append( os.path.join(os.path.dirname(sys.path[0])))

from entity.LightCluster import LightCluster
from entity.LightNode import LightNodeFactory, LightNode, LightNodeLabel
from entity.OrderedQueue import OrderedQueue

sys.path.append(os.path.join(os.path.dirname(sys.path[0])))

from utils.DataLoader import DataLoader
from utils.S3Syncer import S3Syncer
from utils.Settings import Setting
from utils.ProjectPath import ProjectPath
from utils import Utils as ut

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
        node = node_factory.create(scammer_address, [])
        for s in scammers:
            node.valid_neighbours.append(s)
        queue.put(node)
        cluster.add_node(node)
    return cluster, queue, traversed_nodes


def explore_scammer_network(group_id, scammers, node_factory, dex='univ2'):
    cluster_path = eval('path.{}_cluster_path'.format(dex))
    scammers = [s for s in scammers if not ut.is_contract_address(s)]
    if len(scammers) == 0:
        return None, list()
    scammer_address = scammers[0]
    cluster, queue, traversed_nodes = init(group_id, scammer_address, scammers, cluster_path, node_factory)
    it = 0
    while not queue.empty():
        it += 1
        print("*" * 100)
        print("GROUP:", group_id)
        print("QUEUE LEN:", queue.qsize())
        print("TRAVERSED NODES:", len(traversed_nodes))
        print("ITERATION:", it)
        root: LightNode = queue.get()
        if LightNodeLabel.BOUNDARY in root.labels:
            print(f"\t REACH BOUNDARY AT {root.address} >> SKIP")
            continue
        if root.address.lower() in traversed_nodes:
            print(f"\t {root.address} HAS BEEN VISITED >> SKIP")
            continue
        print("\t ROOT ADDRESS", root.address)
        print("\t VALID NEIGHBOURS", len(root.valid_neighbours))
        print("\t LABELS", root.labels)
        print("\t PATH", " -> ".join(root.path))
        traversed_nodes.add(root.address.lower())

        for neighbour_address in root.valid_neighbours:
            neighbour_address = neighbour_address.lower()
            if ((neighbour_address not in traversed_nodes)
                    and (neighbour_address not in queue.addresses)
                    and not cluster.is_address_exist(neighbour_address)):
                node = node_factory.create(neighbour_address, root.path)
                queue.put(node)
        if it % 10 == 0:
            print(">>> SAVE QUEUE & CLUSTER STATE <<<")
            cluster.save(cluster_path)
            cluster.write_queue(cluster_path, queue, traversed_nodes)
        print("*" * 100)
        if config["is_max_iter"] and config["max_iter"] <= it:
            break
    cluster.save(cluster_path)
    cluster.write_queue(cluster_path, queue, traversed_nodes)
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
    run_clustering(2)