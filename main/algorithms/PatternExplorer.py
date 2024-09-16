from algorithms.ScammerNetworkBuilder import dataloader
from entity.Cluster import Cluster
from entity import Node
from utils.DataLoader import DataLoader

dataloader = DataLoader()

REMOVE_LIQUIDITY_SUBSTRING = "removeLiquidity"

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

    pass


def chain_pattern_detection(scammer_address):
    MAX_ITERATIONS = 10
    """Function that returns a single chain list scam from a scammer address. Includes the entire chain """
    chain = [scammer_address]
    current_node = Node.create_node(scammer_address, None, dataloader)
    # loop for the entire chain, once terminated
    number_of_iterations = 0
    while current_node.address in dataloader.scammers and number_of_iterations < MAX_ITERATIONS:
        # TODO maybe don't assume last element because this may not work for the termination condition
        largest_out_transaction = current_node.normal_txs[-1]
        # starting from the latest transaction, pick the largest until we reach the last remove liquidity pool call
        for index in range(len(current_node.normal_txs) - 2, -1, -1):
            if current_node.normal_txs[index].get_transaction_amount() > largest_out_transaction.get_transaction_amount():
                largest_out_transfer = current_node.normal_txs[index]
            if REMOVE_LIQUIDITY_SUBSTRING in current_node.normal_txs[index].functionName:
                break

        chain.append(largest_out_transfer.address)
        current_node = Node.create_node(largest_out_transfer.address, None, dataloader)
        number_of_iterations += 1

    return chain


if __name__ == '__main__':
    chain_pattern_detection("0x48f0fc8dfc672dd45e53b6c53cd5b09c71d9fbd6")
