import os
from enum import Enum

from data_collection.AccountCollector import TransactionCollector
from entity.blockchain.Transaction import Transaction, NormalTransaction
from utils.DataLoader import DataLoader, load_pool
from utils.Path import Path

dataloader = DataLoader()
path = Path()
transaction_collector = TransactionCollector()

REMOVE_LIQUIDITY_SUBSTRING = "removeLiquidity"
ADD_LIQUIDITY_SUBSTRING = "addLiquidity"
OUT_PERCENTAGE_THRESHOLD = 0.9
IN_PERCENTAGE_THRESHOLD = 1.0
END_NODES = (
        dataloader.bridge_addresses | dataloader.defi_addresses | dataloader.cex_addresses | dataloader.MEV_addresses
        | dataloader.mixer_addresses | dataloader.wallet_addresses | dataloader.other_addresses)


class StarShape(Enum):
    IN = 1  # satellites to center
    OUT = 2  # center to satellites
    IN_OUT = 3  # mix of IN and OUT


# TODO
# Given a scammer address, you need to return any IN, OUT and IN_OUT stars that they're part of.
# Suppose for a given scammer, it's a candidate of type IN (there is an OUT after its last scam),
# then you must go back to the center, verify if there exists other satellite nodes that would satisfy the IN star requirement.

# If some other satellite nodes are possibly part of another star, we don't care. Just return ALL the stars
# this current scammer_address is part of.

# TODO you also need to validate that an address is either exclusively IN, OUT or IN_OUT.
#  E.g., if you verify an IN node, you need to make sure that it isn't an IN_OUT
def find_start_shape(scammer_address):
    # TODO implement
    list = [scammer_address]
    return list


def find_liquidity_transactions_in_pool(scammer_address):
    def calc_liquidity_amount(event, use_value):
        return event.amount0 / 10 ** 18 if use_value == 0 else event.amount1 / 10 ** 18

    # key: transaction hash, value: liquidity added/removed
    liquidity_transactions_pool = {}
    scammer_pool = load_pool(scammer_address, dataloader)
    for pool_index in range(len(scammer_pool)):
        eth_pos = scammer_pool[pool_index].get_high_value_position()
        # add liquidity
        for mint in scammer_pool[pool_index].mints:
            liquidity_amount = calc_liquidity_amount(mint, eth_pos)
            liquidity_transactions_pool[mint.transactionHash] = liquidity_amount
        # remove liquidity
        for burn in scammer_pool[pool_index].burns:
            liquidity_amount = calc_liquidity_amount(burn, eth_pos)
            liquidity_transactions_pool[burn.transactionHash] = liquidity_amount

    return liquidity_transactions_pool


def get_funder_and_beneficiary(scammer_address):
    liquidity_transactions_dict = find_liquidity_transactions_in_pool(scammer_address)
    address_before = get_address_before_add_liquidity(scammer_address, liquidity_transactions_dict)
    address_after = get_address_after_remove_liquidity(scammer_address, liquidity_transactions_dict)

    return address_before, address_after


def get_address_after_remove_liquidity(scammer_address: str, liquidity_transactions_dict):
    return get_largest_address(scammer_address, liquidity_transactions_dict, REMOVE_LIQUIDITY_SUBSTRING, True)


def get_address_before_add_liquidity(scammer_address: str, liquidity_transactions_dict):
    return get_largest_address(scammer_address, liquidity_transactions_dict, ADD_LIQUIDITY_SUBSTRING, False)


def is_valid_address(is_out, transaction, scammer_address):
    if is_out:
        return transaction.is_to_eoa(scammer_address) and transaction.to not in END_NODES
    elif not is_out:
        return transaction.is_in_tx(scammer_address) and transaction.sender not in END_NODES
    return False


def get_largest_address(scammer_address, liquidity_transactions_dict, liquidity_function_name: str, is_out):
    normal_txs = transaction_collector.get_transactions(scammer_address)
    if is_out:
        range_loop_args = [len(normal_txs) - 1, -1, -1]
    else:
        range_loop_args = [0, len(normal_txs), 1]
    liquidity_transaction_found = False
    liquidity_amount = 0
    exists_duplicate_amount = False
    largest_transaction = None
    for index in range(range_loop_args[0], range_loop_args[1], range_loop_args[2]):
        # if liquidity is added but no respective IN/OUT transaction is found, just exit
        if not liquidity_transaction_found and liquidity_function_name in str(normal_txs[index].functionName) and not normal_txs[index].isError:
            candidate_liquidity_amount = liquidity_transactions_dict.get(normal_txs[index].hash)
            # only consider an add/remove liquidity if it's in the pool
            if candidate_liquidity_amount is not None:
                if largest_transaction is None:
                    return ''
                liquidity_amount = liquidity_transactions_dict[normal_txs[index].hash]
                liquidity_transaction_found = True
                break
        # if it's an IN or an OUT transaction
        elif is_valid_address(is_out, normal_txs[index], scammer_address):
            # for first find and not empty, nothing to compare to so set largest as first one
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

    if liquidity_transaction_found and largest_transaction:
        passed_threshold = largest_transaction.get_transaction_amount() / liquidity_amount >= (
            OUT_PERCENTAGE_THRESHOLD if is_out else IN_PERCENTAGE_THRESHOLD)
        if passed_threshold and not exists_duplicate_amount:
            return largest_transaction.to if is_out else largest_transaction.sender
    return ''


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
    processed_addresses = read_from_csv(input_path)
    scammers_remaining = set(dataloader.scammers)
    # remove already written scammers from remaining
    for processed_address in processed_addresses:
        scammers_remaining.remove(processed_address)

    # lower means will write to file more frequently, but lower performance
    # higher means less file writes, but better performance
    save_file_freq = 20
    num_scammers_to_run = 100
    overall_scammers_written = 0

    while overall_scammers_written <= num_scammers_to_run and len(scammers_remaining) > 0:
        with open(input_path, "a") as f:
            for _ in range(save_file_freq):
                current_address = scammers_remaining.pop()
                # print('Checking address {} now'.format(current_address))
                # TODO update this
                funder = get_address_before_add_liquidity(current_address)
                beneficiary = get_address_after_remove_liquidity(current_address)
                string_to_write = '{}, {}, {}\n'.format(current_address, funder, beneficiary)
                f.write(string_to_write)
                overall_scammers_written += 1


if __name__ == '__main__':
    funder, beneficiary = get_funder_and_beneficiary('0xb89c501e28acd743a577b94fc66fb6f2bfd75186')
    print('before address: {}, after address: {}'.format(funder, beneficiary))
    # write_scammer_funders_and_beneficiary()
