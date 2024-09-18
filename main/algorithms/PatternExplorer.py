
from algorithms.ScammerNetworkBuilder import dataloader
from entity import Node
from utils.DataLoader import DataLoader

dataloader = DataLoader()

REMOVE_LIQUIDITY_SUBSTRING = "removeLiquidity"
ADD_LIQUIDITY_SUBSTRING = "addLiquidity"
MIN_CHAIN_SIZE = 2


def chain_pattern_detection(scammer_address):
    """Function that returns a single chain list scam from a scammer address. Includes the entire chain the address
    is involved in"""
    chain = [scammer_address]
    current_node = Node.create_node(scammer_address, None, dataloader)
    valid_address_fwd = current_node.address in dataloader.scammers
    valid_address_bwd = valid_address_fwd
    while valid_address_fwd:
        # TODO need more checks if these transactions are None
        largest_out_transaction = get_largest_out_transaction_after_remove_liquidity(current_node.address)[0]
        largest_in_transaction, next_node = get_largest_in_transaction_before_add_liquidity(largest_out_transaction.to)

        valid_address_fwd = largest_out_transaction.hash == largest_in_transaction.hash and largest_out_transaction.to in dataloader.scammers
        if valid_address_fwd:
            current_node = next_node
            # TODO remove this
            print("address to:" + current_node.address + "trans hash:" + largest_out_transaction.hash)
            # TODO just append the address not the whole debugging string
            chain.append({"address to:" + current_node.address, "trans hash:" + largest_out_transaction.hash})

    while valid_address_bwd:
        print('no logic!')
        valid_address_bwd = False

    return chain if len(chain) >= MIN_CHAIN_SIZE else None


def get_largest_out_transaction_after_remove_liquidity(scammer_address: str):
    node = Node.create_node(scammer_address, None, dataloader)
    return get_largest_transaction(node, REMOVE_LIQUIDITY_SUBSTRING, True, len(node.normal_txs) - 1, -1, -1)


def get_largest_in_transaction_before_add_liquidity(scammer_address: str):
    node = Node.create_node(scammer_address, None, dataloader)
    return get_largest_transaction(node, ADD_LIQUIDITY_SUBSTRING, False, 0, len(node.normal_txs), 1)


# TODO what to do if largest_transaction is None
def get_largest_transaction(node: Node, termination_function_name: str, is_out, *steps):
    largest_transaction = None
    for index in range(steps[0], steps[1], steps[2]):
        if termination_function_name in str(node.normal_txs[index].functionName):
            break
        elif (is_out and node.normal_txs[index].is_to_eoa(node.address)) or (
                not is_out and node.normal_txs[index].is_in_tx(node.address)):
            if largest_transaction is None or node.normal_txs[
                index].get_transaction_amount() > largest_transaction.get_transaction_amount():
                largest_transaction = node.normal_txs[index]

    return largest_transaction, node


if __name__ == '__main__':
    print(*chain_pattern_detection("0x48f0fc8dfc672dd45e53b6c53cd5b09c71d9fbd6"), sep='\n')
    # chain_pattern_detection("0x48f0fc8dfc672dd45e53b6c53cd5b09c71d9fbd6")
