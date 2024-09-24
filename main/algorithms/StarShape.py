import os

from data_collection.AccountCollector import TransactionCollector
from utils.DataLoader import DataLoader
from utils.Path import Path

dataloader = DataLoader()
path = Path()
transaction_collector = TransactionCollector()

REMOVE_LIQUIDITY_SUBSTRING = "removeLiquidity"
ADD_LIQUIDITY_SUBSTRING = "addLiquidity"
PERCENTAGE_THRESHOLD = 0.9
END_NODES = (
        dataloader.bridge_addresses | dataloader.defi_addresses | dataloader.cex_addresses | dataloader.MEV_addresses
        | dataloader.mixer_addresses | dataloader.wallet_addresses | dataloader.other_addresses)


def find_start_shape(scammer_address):
    # TODO implement
    list = [scammer_address]
    return list


def get_address_after_remove_liquidity(scammer_address: str, normal_txs=None):
    if normal_txs is None:
        normal_txs = transaction_collector.get_transactions(scammer_address)
    return get_largest_address(normal_txs, scammer_address, REMOVE_LIQUIDITY_SUBSTRING, True, len(normal_txs) - 1,
                               -1, -1)


def get_address_before_add_liquidity(scammer_address: str, normal_txs=None):
    if normal_txs is None:
        normal_txs = transaction_collector.get_transactions(scammer_address)
    return get_largest_address(normal_txs, scammer_address, ADD_LIQUIDITY_SUBSTRING, False, 0, len(normal_txs), 1)


def is_valid_address(is_out, transaction, scammer_address):
    if is_out:
        return transaction.is_to_eoa(scammer_address) and transaction.to not in END_NODES
    elif not is_out:
        return transaction.is_in_tx(scammer_address) and transaction.sender not in END_NODES
    return False


def get_largest_address(normal_txs, scammer_address, liquidity_function_name: str, is_out, *range_loop_args):
    liquidity_transaction = None
    exists_duplicate_amount = False
    largest_transaction = None
    for index in range(range_loop_args[0], range_loop_args[1], range_loop_args[2]):
        # if liquidity is added but no respective IN/OUT transaction is found, just exit
        if not liquidity_transaction and liquidity_function_name in str(normal_txs[index].functionName):
            if largest_transaction is None:
                return None
            liquidity_transaction = normal_txs[index]
            break
        # if it's an IN or an OUT transaction
        elif is_valid_address(is_out, normal_txs[index], scammer_address):
            # for first find, nothing to compare to so set largest as first one
            # TODO also verify that it's not an end node
            if largest_transaction is None:
                largest_transaction = normal_txs[index]
            # candidate transaction is larger
            elif normal_txs[index].get_transaction_amount() >= largest_transaction.get_transaction_amount():
                # there exists an equivalent transaction, so mark as duplicate
                if normal_txs[index].get_transaction_amount() == largest_transaction.get_transaction_amount():
                    exists_duplicate_amount = True
                else:
                    exists_duplicate_amount = False
                    largest_transaction = normal_txs[index]

    # TODO liquidity_transaction.get_transaction_amount() doesn't work
    if liquidity_transaction and largest_transaction and liquidity_transaction.get_transaction_amount() / largest_transaction.get_transaction_amount() >= PERCENTAGE_THRESHOLD and not exists_duplicate_amount:
        return largest_transaction.to if is_out else largest_transaction.sender
    return None


def read_from_csv(input_path):
    file = open(input_path)
    read_addresses = set()
    for line in file:
        row = line.rstrip('\n').split(', ')
        read_addresses.add(row[0])

    # remove from first line of reading csv
    read_addresses.remove('address')
    file.close()

    return read_addresses


def write_scammer_funders_and_beneficiary():
    input_path = os.path.join(path.univ2_scammer_chain_path, "scammer_in_out_addresses.txt")
    processed_addresses = read_from_csv()
    scammers_remaining = set(dataloader.scammers)
    # remove already written scammers from remaining
    for processed_address in processed_addresses:
        scammers_remaining.remove(processed_address)

    # lower means will write to file more frequently, but lower performance
    # higher means less file writes, but better performance
    save_file_freq = 1000
    num_scammers_to_run = 100000
    overall_scammers_written = 0

    while overall_scammers_written <= num_scammers_to_run or len(scammers_remaining) > 0:
        with open(input_path, "a") as f:
            for _ in range(save_file_freq):
                current_address = scammers_remaining.pop()
                funder = get_address_before_add_liquidity(current_address)
                beneficiary = get_address_after_remove_liquidity(current_address)
                string_to_write = '{}, {}, {}\n'.format(current_address, funder, beneficiary)
                f.write(string_to_write)
                overall_scammers_written += 1


if __name__ == '__main__':
    address_before = get_address_before_add_liquidity('0xb561d5186917dfea49cd092c5f33995dd837776f')
    address_after = get_address_after_remove_liquidity('0xb561d5186917dfea49cd092c5f33995dd837776f')
    # write_scammer_funders_and_beneficiary()
