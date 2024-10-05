from entity.LightNode import LightNode
import pandas as pd
from entity.OrderedQueue import OrderedQueue
from utils import Utils as ut
import os


class LightCluster:
    def __init__(self, gid):
        self.id = gid
        self.nodes = dict()
        self.groups = set()

    def __contains__(self, node):
        return node.address in self.nodes.keys()

    def is_address_exist(self, address):
        return address.lower() in self.nodes.keys()

    def add_node(self, node: LightNode):
        self.nodes[node.address.lower()] = node
        if node.group is not None:
            self.groups.add(node.group)

    def write_queue(self, outpath, q: OrderedQueue, traversed_nodes):
        queue_file = os.path.join(outpath, f"queue_{self.id}.csv")
        traversed_file = os.path.join(outpath, f"traversed_{self.id}.txt")
        nodes = []
        for node in q.queue:
            nodes.append({"address": node.address, "path": ";".join(node.path) if node.path else None})
        ut.save_overwrite_if_exist(nodes, queue_file)
        ut.write_list_to_file(traversed_file, traversed_nodes)

    def read_queue(self, in_path, factory):
        queue = OrderedQueue()
        traversed_nodes = set()
        queue_file = os.path.join(in_path, f"queue_{self.id}.csv")
        traversed_file = os.path.join(in_path, f"traversed_{self.id}.txt")
        try:
            if os.path.exists(queue_file):
                print("LOAD EXISTING QUEUE")
                queue_df = pd.read_csv(queue_file)
                for idx, row in queue_df.iterrows():
                    path = row['path'].split(';') if 'path' in row else []
                    if row["address"] in path:
                        path.remove(row["address"])
                    node = factory.createNode(row["address"], path, self.id)
                    queue.put(node)
            if os.path.exists(traversed_file):
                print("LOAD EXISTING TRAVERSAL LIST")
                traversed_nodes = set(ut.read_list_from_file(traversed_file))
        except Exception as e:
            print("CANNOT LOAD QUEUE >> START FROM SCRATCH")
        return queue, traversed_nodes

    def load(self, in_path):
        c_path = os.path.join(in_path, f"cluster_{self.id}.csv")
        if os.path.exists(c_path):
            print("LOAD EXISTING CLUSTER")
            cluster_df = pd.read_csv(c_path)
            for idx, row in cluster_df.iterrows():
                node = LightNode.from_dict(row.to_dict())
                self.nodes[node.address] = node

    def save(self, outpath):
        node_list_file = os.path.join(outpath, f"cluster_{self.id}.csv")
        data = []
        for n in self.nodes.values():
            data.append({"address": n.address,
                         "normal_txs_len": n.normal_txs_len,
                         "valid_neighbours_len": len(n.valid_neighbours),
                         "valid_neighbours": ";".join(n.valid_neighbours),
                         "labels": ";".join(n.labels),
                         "path": ";".join(n.path)
                         })
        ut.save_overwrite_if_exist(data, node_list_file)
