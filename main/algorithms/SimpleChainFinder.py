import os

from algorithms.ScammerNetworkBuilder import dataloader
from data_collection.AccountCollector import TransactionCollector
from utils.DataLoader import DataLoader
from utils.Path import Path

dataloader = DataLoader()
path = Path()
transaction_collector = TransactionCollector()

REMOVE_LIQUIDITY_SUBSTRING = "removeLiquidity"
ADD_LIQUIDITY_SUBSTRING = "addLiquidity"



def chain_pattern_detection(starter_address):
    """Function that returns a single chain list scam from a scammer address. Includes the entire chain the address
    is involved in"""
    fwd_chain = []
    valid_address_fwd = valid_address_bwd = starter_address in dataloader.scammers
    if valid_address_fwd:
        fwd_chain.append(starter_address)

    starter_transaction_history = transaction_collector.get_transactions(starter_address)
    current_transaction_history = starter_transaction_history
    current_address = starter_address

    # chain forward
    while valid_address_fwd:
        valid_address_fwd = False
        largest_out_transaction = get_largest_out_after_remove_liquidity(current_address, current_transaction_history)[0]
        if largest_out_transaction and largest_out_transaction.to in dataloader.scammers:
            largest_in_transaction, next_transaction_history = get_largest_in_before_add_liquidity(largest_out_transaction.to)
            if largest_in_transaction:
                valid_address_fwd = largest_out_transaction.hash == largest_in_transaction.hash

        if valid_address_fwd:
            current_address = largest_in_transaction.to
            current_transaction_history = next_transaction_history
            fwd_chain.append(current_address)

    current_address = starter_address
    current_transaction_history = starter_transaction_history
    bwd_chain = []
    while valid_address_bwd:
        valid_address_bwd = False
        largest_in_transaction = get_largest_in_before_add_liquidity(current_address, current_transaction_history)[0]
        if largest_in_transaction and largest_in_transaction.sender in dataloader.scammers:
            largest_out_transaction, prev_transaction_history = get_largest_out_after_remove_liquidity(largest_in_transaction.sender)
            if largest_out_transaction:
                valid_address_bwd = largest_out_transaction.hash == largest_in_transaction.hash

        if valid_address_bwd:
            current_address = largest_out_transaction.sender
            current_transaction_history = prev_transaction_history
            bwd_chain.append(current_address)

    bwd_chain = bwd_chain[::-1]
    return bwd_chain + fwd_chain


def get_largest_out_after_remove_liquidity(scammer_address: str, normal_txs=None):
    if normal_txs is None:
        normal_txs = transaction_collector.get_transactions(scammer_address)
    return get_largest_transaction(normal_txs, scammer_address, REMOVE_LIQUIDITY_SUBSTRING, True, len(normal_txs) - 1, -1, -1)


def get_largest_in_before_add_liquidity(scammer_address: str, normal_txs=None):
    if normal_txs is None:
        normal_txs = transaction_collector.get_transactions(scammer_address)
    return get_largest_transaction(normal_txs, scammer_address, ADD_LIQUIDITY_SUBSTRING, False, 0, len(normal_txs), 1)


def get_largest_transaction(normal_txs, scammer_address, liquidity_function_name: str, is_out, *range_loop_args):
    passed_liquidity_function = False
    exists_duplicate_amount = False
    largest_transaction = None
    for index in range(range_loop_args[0], range_loop_args[1], range_loop_args[2]):
        if not passed_liquidity_function and liquidity_function_name in str(normal_txs[index].functionName) and not normal_txs[index].isError:
            passed_liquidity_function = True
            if largest_transaction is None:
                return None, normal_txs
        elif (is_out and normal_txs[index].is_to_eoa(scammer_address)) or (not is_out and normal_txs[index].is_in_tx(scammer_address)):

            # just set the largest_transaction for the first find
            if largest_transaction is None:
                largest_transaction = normal_txs[index]
            elif normal_txs[index].get_transaction_amount() >= largest_transaction.get_transaction_amount():
                # >= amount found then current largest, therefore is not the sole funder
                if passed_liquidity_function:
                    return None, normal_txs

                # if this new transaction is the same amount already, indicate that there is a duplicate
                if normal_txs[index].get_transaction_amount() == largest_transaction.get_transaction_amount():
                    exists_duplicate_amount = True
                else:
                    exists_duplicate_amount = False
                    largest_transaction = normal_txs[index]

    return largest_transaction if not exists_duplicate_amount and passed_liquidity_function else None, normal_txs


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
    scammer_chain_path = os.path.join(path.univ2_scammer_chain_path, "simple_chain.txt")

    existing_addresses = read_from_csv_as_set(scammer_chain_path)
    scammers_remaining = set(dataloader.scammers)
    for processed_scammer in existing_addresses:
        scammers_remaining.discard(processed_scammer)

    # lower means will write to file more frequently, but lower performance
    # higher means less file writes, but better performance
    save_file_freq = 1000
    num_scammers_to_run = 100000
    overall_scammers_written = 0

    # save to file
    while overall_scammers_written <= num_scammers_to_run or len(scammers_remaining) > 0:
        with open(scammer_chain_path, "a") as f:
            for _ in range(save_file_freq):
                current_address = scammers_remaining.pop()
                chain = chain_pattern_detection(current_address)
                string_to_write = '{}, {}\n'.format(len(chain), ', '.join(chain))
                f.write(string_to_write)
                overall_scammers_written = overall_scammers_written + 1
                for scammer in chain:
                    scammers_remaining.discard(scammer)


if __name__ == '__main__':
    run_chain_on_scammers()
    # print(*chain_pattern_detection("0x48f0fc8dfc672dd45e53b6c53cd5b09c71d9fbd6"), sep='\n')
    # print(chain_pattern_detection("0x48f0fc8dfc672dd45e53b6c53cd5b09c71d9fbd6"))
