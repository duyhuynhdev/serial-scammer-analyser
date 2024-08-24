from networkx import DiGraph, has_path
from entity import Node
from entity.Node import NodeLabel


class Cluster:
    def __init__(self):
        self.nodes = dict()
        self.network =  DiGraph()
    def add_node(self, node: Node):
        self.nodes[node.address] = node
        self.network.add_node(node.address)
    def add_connection(self, node_from, node_to, weight):
        if node_from not in self.nodes or node_to not in self.nodes:
            return False
        self.network.add_edge(node_from.address, node_to.address,  weight=weight)
    def get_scammers(self)->(list, DiGraph):
        scammers  = []
        scammer_network = DiGraph()
        for node in self.nodes.values():
            if node.label == NodeLabel.S:
                scammers.append(node)
                scammer_network.add_node(node.address)
        for i in scammers:
            for j in scammers:
                if i != j and has_path(self.network, i.address, j.address):
                    scammer_network.add_edge(i.address, j.address)
        return scammers, scammer_network