import networkx as nx
from networkx import DiGraph, has_path
from entity import Node
from entity.Node import NodeLabel
from utils import Utils as ut
import os
import itertools


class Cluster:
    id_iter = itertools.count()

    def __init__(self):
        self.id = next(self.id_iter)
        self.nodes = dict()
        self.network = DiGraph()

    def __contains__(self, node):
        return node.address in self.nodes.keys()

    def is_address_exist(self, address):
        return address in self.nodes.keys()

    def add_node(self, node: Node):
        self.nodes[node.address] = node
        self.network.add_node(node.address, data=node)
    #TODO: accumulate amount & fee
    def add_connection(self, node_from, node_to, weight, transaction):
        if not self.network.has_node(node_from) or not self.network.has_node(node_to):
            return False
        self.network.add_edge(node_from, node_to, weight=weight, data=transaction)

    def get_scammers(self) -> (list, DiGraph):
        scammers = []
        scammer_network = DiGraph()
        for node in self.nodes.values():
            if node.label == NodeLabel.S:
                scammers.append(node)
                scammer_network.add_node(node.address, data=node)
        for i in scammers:
            for j in scammers:
                if i != j and has_path(self.network, i.address, j.address):
                    scammer_network.add_edge(i.address, j.address)
        return scammers, scammer_network

    def export(self, outpath):
        node_list_file = os.path.join(outpath, f"C{self.id}.node")
        graph_file = os.path.join(outpath, f"C{self.id}.nw")
        ut.write_list_to_file(node_list_file, self.nodes.keys())
        nx.write_adjlist(self.network, graph_file)
