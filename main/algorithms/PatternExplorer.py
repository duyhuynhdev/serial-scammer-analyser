import os

from algorithms.ScammerNetworkBuilder import dataloader
from entity import Node
from utils.DataLoader import DataLoader
from utils.Path import Path

dataloader = DataLoader()
path = Path()

REMOVE_LIQUIDITY_SUBSTRING = "removeLiquidity"
ADD_LIQUIDITY_SUBSTRING = "addLiquidity"


def chain_pattern_detection(scammer_address):
    """Function that returns a single chain list scam from a scammer address. Includes the entire chain the address
    is involved in"""
    chain = []
    current_node = Node.create_node(scammer_address, None, dataloader)
    valid_address_fwd = valid_address_bwd = current_node.address in dataloader.scammers
    # TODO verify, because can be in list of scammers but have no liquidity calls.
    if valid_address_fwd:
        chain.append(scammer_address)

    # chain forward
    while valid_address_fwd:
        valid_address_fwd = False
        largest_out_transaction = get_largest_out_after_remove_liquidity(current_node.address)[0]
        if largest_out_transaction and largest_out_transaction.to in dataloader.scammers:
            largest_in_transaction, next_node = get_largest_in_before_add_liquidity(largest_out_transaction.to)
            if largest_in_transaction:
                valid_address_fwd = largest_out_transaction.hash == largest_in_transaction.hash

        if valid_address_fwd:
            current_node = next_node
            chain.append(current_node.address)

    # TODO possibly don't recreate this
    current_node = Node.create_node(scammer_address, None, dataloader)
    bwd_chain = []
    while valid_address_bwd:
        valid_address_bwd = False
        largest_in_transaction = get_largest_in_before_add_liquidity(current_node.address)[0]
        if largest_in_transaction and largest_in_transaction.sender in dataloader.scammers:
            largest_out_transaction, prev_node = get_largest_out_after_remove_liquidity(largest_in_transaction.sender)
            if largest_out_transaction:
                valid_address_bwd = largest_out_transaction.hash == largest_in_transaction.hash

        if valid_address_bwd:
            current_node = prev_node
            bwd_chain.append(current_node.address)

    bwd_chain = bwd_chain[::-1]
    return bwd_chain + chain


def get_largest_out_after_remove_liquidity(scammer_address: str):
    node = Node.create_node(scammer_address, None, dataloader)
    return get_largest_transaction(node, REMOVE_LIQUIDITY_SUBSTRING, True, len(node.normal_txs) - 1, -1, -1)


def get_largest_in_before_add_liquidity(scammer_address: str):
    node = Node.create_node(scammer_address, None, dataloader)
    return get_largest_transaction(node, ADD_LIQUIDITY_SUBSTRING, False, 0, len(node.normal_txs), 1)


def get_largest_transaction(node: Node, liquidity_function_name: str, is_out, *range_loop_args):
    passed_liquidity_function = False
    exists_duplicate_amount = False
    largest_transaction = None
    for index in range(range_loop_args[0], range_loop_args[1], range_loop_args[2]):
        if not passed_liquidity_function and liquidity_function_name in str(node.normal_txs[index].functionName):
            passed_liquidity_function = True
            if largest_transaction is None:
                return None, node
        elif (is_out and node.normal_txs[index].is_to_eoa(node.address)) or (
                not is_out and node.normal_txs[index].is_in_tx(node.address)):

            # just set the largest_transaction for the first find
            if largest_transaction is None:
                largest_transaction = node.normal_txs[index]
            elif node.normal_txs[index].get_transaction_amount() >= largest_transaction.get_transaction_amount():
                # >= amount found then current largest, therefore is not the sole funder
                if passed_liquidity_function:
                    return None, node

                # if this new transaction is the same amount already, indicate that there is a duplicate
                if node.normal_txs[index].get_transaction_amount() == largest_transaction.get_transaction_amount():
                    exists_duplicate_amount = True
                else:
                    exists_duplicate_amount = False
                    largest_transaction = node.normal_txs[index]

    return largest_transaction if not exists_duplicate_amount and passed_liquidity_function else None, node


def read_from_csv_as_set(input_path):
    set_csv = set()
    file = open(input_path)
    for line in file:
        row = line.rstrip('\n').split(', ')
        # don't add length in set
        for index in range(1, len(row)):
            set_csv.add(row[index])

    file.close()
    return set_csv


def run_chain_on_scammers():
    max_scammers_to_run = 100
    scammer_chain_path = os.path.join(path.univ2_scammer_chain_path, "scammer_chain.txt")

    existing_addresses = read_from_csv_as_set(scammer_chain_path)
    scammers_remaining = set(dataloader.scammers)
    for processed_scammer in existing_addresses:
        if processed_scammer in scammers_remaining:
            scammers_remaining.remove(processed_scammer)

    count = 0
    with open(scammer_chain_path, "a") as f:
        for current_address in iter(scammers_remaining):
            if count == max_scammers_to_run:
                break
            chain = chain_pattern_detection(current_address)
            string_to_write = '{}, {}\n'.format(len(chain), ', '.join(chain))
            f.write(string_to_write)
            count = count + 1


if __name__ == '__main__':
    run_chain_on_scammers()
    # print(*chain_pattern_detection("0x48f0fc8dfc672dd45e53b6c53cd5b09c71d9fbd6"), sep='\n')
    # print(chain_pattern_detection("0x699f93da70298b49100080257e7fd4f44ed1fefa"))
