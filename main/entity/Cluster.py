import networkx as nx
from networkx import DiGraph, has_path
from entity import Node
from entity.Node import NodeLabel, create_node
from utils import Utils as ut
import os
import itertools


class ClusterNode:
    def __init__(self, address, parent, labels):
        self.address = address
        self.parent = parent
        self.labels = labels

    def to_full_node(self, dataloader):
        full_node = create_node(self.address, self.parent, dataloader)
        full_node.labels = self.labels
        return full_node

    @staticmethod
    def from_dict(data):
        address = data['address']
        parent = data['parent']
        labels = data['labels'].split(';')
        return ClusterNode(address, parent, labels)


class Cluster:
    id_iter = itertools.count()

    def __init__(self, address):
        self.id = address
        self.nodes = dict()

    def __contains__(self, node):
        return node.address in self.nodes.keys()

    def is_address_exist(self, address):
        return address in self.nodes.keys()

    def add_node(self, node: Node):
        cnode = ClusterNode(node.address, node.parent, node.labels)
        self.nodes[cnode.address] = cnode

    def export(self, outpath):
        node_list_file = os.path.join(outpath, f"cluster_{self.id}.csv")
        data = []
        for n in self.nodes.values():
            p = n.parent.address if n.parent is not None else "_"
            data.append({"address": n.address, "parent": p, "labels": ";".join(n.labels)})
        ut.save_overwrite_if_exist(data, node_list_file)
