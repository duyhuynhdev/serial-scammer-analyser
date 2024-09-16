from algorithms.ScammerNetworkBuilder import dataloader
from entity.Cluster import Cluster
from entity import Node
from utils.DataLoader import DataLoader

dataloader = DataLoader()


def execute(scammer_address):
    node = Node.create_node(scammer_address, None, dataloader)
    scammer_address = "aaaa"
    is_scammer = scammer_address in dataloader.scammers
    end_nodes = (dataloader.bridge_addresses |
                 dataloader.defi_addresses |
                 dataloader.MEV_addresses |
                 dataloader.mixer_addresses |
                 dataloader.wallet_addresses |
                 dataloader.other_addresses)
    start_address = ""

    # while start_address not in end_nodes:
    #     start_address =  next_address()

    print(len(node.normal_txs))
    print(len(node.internal_txs))
    pass


def chain_pattern_detection(cluster: Cluster):
    pass


if __name__ == '__main__':
    # execute("0x48f0fc8dfc672dd45e53b6c53cd5b09c71d9fbd6")
    print(dataloader.scammers)