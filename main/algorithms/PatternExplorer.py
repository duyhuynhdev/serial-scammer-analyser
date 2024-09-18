
from algorithms.ScammerNetworkBuilder import dataloader
from entity import Node
from entity.blockchain import Transaction
from utils.DataLoader import DataLoader

dataloader = DataLoader()

REMOVE_LIQUIDITY_SUBSTRING = "removeLiquidity"
ADD_LIQUIDITY_SUBSTRING = "addLiquidity"


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
    """Function that returns a single chain list scam from a scammer address. Includes the entire chain the address
    is involved in"""
    MAX_ITERATIONS = 50
    chain = [scammer_address]
    current_node = Node.create_node(scammer_address, None, dataloader)
    # TODO remove this number_of_iterations to unlimited
    number_of_iterations = 0
    should_continue = current_node.address in dataloader.scammers
    # forward in the chain
    while should_continue and number_of_iterations < MAX_ITERATIONS:
        should_continue = False
        largest_out_transaction = None
        # starting from the latest transaction, pick the largest until we reach the last remove liquidity pool call
        for index in range(len(current_node.normal_txs) - 1, -1, -1):
            if REMOVE_LIQUIDITY_SUBSTRING in str(current_node.normal_txs[index].functionName):
                # TODO condition needs to be added. If the scammer removed from liquidity pool ( or maybe it isn't needed actually)
                break
            if current_node.normal_txs[index].is_to_eoa(current_node.address):
                if largest_out_transaction is None or current_node.normal_txs[index].get_transaction_amount() > largest_out_transaction.get_transaction_amount():
                    largest_out_transaction = current_node.normal_txs[index]

        current_node = exists_valid_address_to_add(largest_out_transaction, dataloader.scammers)
        if current_node is not None:
            should_continue = True
            print("address to:" + current_node.address + "trans hash:" + largest_out_transaction.hash)
            chain.append({"address to:" + current_node.address, "trans hash:" + largest_out_transaction.hash})
            number_of_iterations += 1

    return chain

# TODO create two separate functions. Find largest_out_transaction after last remove_liquidity()
# TODO and largets in transaction before first_add_liquidity. This way we can reuse it

# return node
def exists_valid_address_to_add(largest_out_transaction: Transaction, scammers):
    current_node = None
    valid_address = largest_out_transaction is not None and largest_out_transaction.to in dataloader.scammers
    if valid_address:
        current_node = Node.create_node(largest_out_transaction.to, None, dataloader)
        largest_in_transaction = None
        # verify that this transaction is the largest IN transaction for the to address before first liquidity add
        for index in range(len(current_node.normal_txs)):
            if ADD_LIQUIDITY_SUBSTRING in str(current_node.normal_txs[index].functionName):
                break
            if current_node.normal_txs[index].is_in_tx(current_node.address):
                if largest_in_transaction is None or current_node.normal_txs[index].get_transaction_amount() > largest_in_transaction.get_transaction_amount():
                    largest_in_transaction = current_node.normal_txs[index]

        valid_address = largest_out_transaction.hash == largest_in_transaction.hash

    return current_node if valid_address else None


if __name__ == '__main__':
    # print(*chain_pattern_detection("0x48f0fc8dfc672dd45e53b6c53cd5b09c71d9fbd6"), sep='\n')
    chain_pattern_detection("0x48f0fc8dfc672dd45e53b6c53cd5b09c71d9fbd6")
