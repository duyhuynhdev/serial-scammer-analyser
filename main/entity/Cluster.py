import pandas as pd
from entity import Node
from entity.Node import create_node
from entity.OrderedQueue import OrderedQueue
from utils import Utils as ut
import os


class ClusterNode:
    def __init__(
        self, address, path, eoa_nb, contract_nb, normal_txs, internal_txs, labels
    ):
        self.address = address
        self.eoa_nb = eoa_nb
        self.contract_nb = contract_nb
        self.normal_txs = normal_txs
        self.internal_txs = internal_txs
        self.labels = labels
        self.path = path.copy() if path is not Node else [address]

    def to_full_node(self, dataloader, existing_groups=None):
        if existing_groups is None:
            existing_groups = set()
        if self.address in self.path:
            self.path.remove(self.address)
        full_node = create_node(self.address, self.path, dataloader, existing_groups)
        full_node.labels = self.labels
        return full_node

    @staticmethod
    def from_dict(data):
        address = data["address"]
        eoa_nb = data["eoa_nb"] if "eoa_nb" in data else None
        contract_nb = data["contract_nb"] if "contract_nb" in data else None
        normal_txs = data["normal_txs"] if "normal_txs" in data else None
        internal_txs = data["internal_txs"] if "internal_txs" in data else None
        labels = (
            data["labels"].split(";")
            if "labels" in data and isinstance(data["labels"], str)
            else []
        )
        path = data["path"].split(">>") if "path" in data else []
        return ClusterNode(
            address, path, eoa_nb, contract_nb, normal_txs, internal_txs, labels
        )


class Cluster:
    def __init__(self, gid):
        self.id = gid
        self.nodes = dict()
        self.groups = set()

    def __contains__(self, node):
        return node.address in self.nodes.keys()

    def is_address_exist(self, address):
        return address in self.nodes.keys()

    def add_node(self, node: Node):
        cnode = ClusterNode(
            node.address,
            node.path,
            len(node.eoa_neighbours),
            len(node.contract_neighbours),
            len(node.normal_txs),
            len(node.internal_txs),
            node.labels,
        )
        self.nodes[cnode.address] = cnode

    def add_group(self, group):
        self.groups.add(group)

    def write_node(self, outpath, n: Node):
        node_list_file = os.path.join(outpath, f"cluster_{self.id}.csv")
        data = [
            {
                "address": n.address,
                "eoa_n": len(n.eoa_neighbours),
                "contract_n": len(n.contract_neighbours),
                "normal": len(n.normal_txs),
                "internal": len(n.internal_txs),
                "labels": ";".join(n.labels),
                "path": ">>".join(n.path),
            }
        ]
        ut.save_or_append_if_exist(data, node_list_file)

    def write_queue(self, outpath, q: OrderedQueue, traversed_nodes):
        queue_file = os.path.join(outpath, f"queue_{self.id}.csv")
        traversed_file = os.path.join(outpath, f"traversed_{self.id}.txt")
        nodes = []
        for node in q.queue:
            nodes.append(
                {
                    "address": node.address,
                    "path": ";".join(node.path) if node.path else None,
                }
            )
        ut.save_overwrite_if_exist(nodes, queue_file)
        ut.write_list_to_file(traversed_file, traversed_nodes)

    def read_queue(self, in_path, dataloader):
        queue = OrderedQueue()
        traversed_nodes = set()
        existing_groups = set()
        queue_file = os.path.join(in_path, f"queue_{self.id}.csv")
        traversed_file = os.path.join(in_path, f"traversed_{self.id}.txt")
        if os.path.exists(queue_file):
            print("LOAD EXISTING QUEUE")
            try:
                queue_df = pd.read_csv(queue_file)
                for idx, row in queue_df.iterrows():
                    path = row["path"].split(";") if "path" in row else []
                    if row["address"] in path:
                        path.remove(row["address"])
                    node = create_node(
                        row["address"], path, dataloader, existing_groups
                    )
                    queue.put(node)
            except Exception as e:
                print("CANNOT LOAD QUEUE FILE INTO DF >> START FROM SCRATCH")
        if os.path.exists(traversed_file):
            print("LOAD EXISTING TRAVERSAL LIST")
            traversed_nodes = set(ut.read_list_from_file(traversed_file))
        return queue, traversed_nodes

    def load_cluster(self, in_path):
        c_path = os.path.join(in_path, f"cluster_{self.id}.csv")
        if os.path.exists(c_path):
            print("LOAD EXISTING CLUSTER")
            cluster_df = pd.read_csv(c_path)
            for idx, row in cluster_df.iterrows():
                cnode = ClusterNode.from_dict(row.to_dict())
                self.nodes[cnode.address] = cnode

    def export(self, outpath):
        node_list_file = os.path.join(outpath, f"cluster_{self.id}.csv")
        data = []
        for n in self.nodes.values():
            data.append(
                {
                    "address": n.address,
                    "eoa_nb": n.eoa_nb,
                    "contract_nb": n.contract_nb,
                    "normal_txs": n.normal_txs,
                    "internal_txs": n.internal_txs,
                    "labels": ";".join(n.labels),
                    "path": ">>".join(n.path),
                }
            )
        ut.save_overwrite_if_exist(data, node_list_file)
