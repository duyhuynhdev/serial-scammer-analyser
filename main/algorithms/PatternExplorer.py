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
    # TODO remove
    number_of_iterations = 0
    should_continue = current_node.address in dataloader.scammers
    while should_continue and number_of_iterations < MAX_ITERATIONS:
        should_continue = False
        largest_out_transaction = None
        # starting from the latest transaction, pick the largest until we reach the last remove liquidity pool call
        for index in range(len(current_node.normal_txs) - 1, -1, -1):
            if REMOVE_LIQUIDITY_SUBSTRING in str(current_node.normal_txs[index].functionName):
                # TODO condition needs to be added. If the scammer removed from liquidity pool.
                break
            # TODO verify this condition
            if current_node.normal_txs[index].is_to_eoa(current_node.normal_txs[index].sender):
                if largest_out_transaction is None or current_node.normal_txs[index].get_transaction_amount() > largest_out_transaction.get_transaction_amount():
                    largest_out_transaction = current_node.normal_txs[index]


        if largest_out_transaction is not None and largest_out_transaction.to in dataloader.scammers:
            # TODO you also need to verify that it's the largest IN transaction for this scammer BEFORE first add
            should_continue = True
            current_node = Node.create_node(largest_out_transaction.to, None, dataloader)
            chain.append(current_node.address)
            number_of_iterations += 1

    return chain


if __name__ == '__main__':
    chain_pattern_detection("0x48f0fc8dfc672dd45e53b6c53cd5b09c71d9fbd6")
