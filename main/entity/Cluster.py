import networkx as nx
from networkx import DiGraph, has_path
from entity import Node
from entity.Node import NodeLabel
from utils import Utils as ut
import os
import itertools


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
        self.nodes[node.address] = node

    def export(self, outpath):
        node_list_file = os.path.join(outpath, f"cluster_{self.id}.csv")
        data = []
        for n in self.nodes.values():
            p =  n.parent.address if n.parent is not None else "_"
            data.append({"address": n.address, "parent": p, "labels": ";".join(n.labels)})
        ut.save_overwrite_if_exist(data, node_list_file)
