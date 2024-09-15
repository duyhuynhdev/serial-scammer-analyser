from entity.Cluster import Cluster
from entity import Node
from utils.DataLoader import DataLoader


def execute(scammer_address):
    node = Node.create_node(scammer_address, None, DataLoader())
    print(len(node.normal_txs))
    print(len(node.internal_txs))
    pass

def chain_pattern_detection(cluster: Cluster):
    pass


if __name__ == '__main__':
    execute("0x48f0fc8dfc672dd45e53b6c53cd5b09c71d9fbd6")