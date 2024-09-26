import itertools
import json
import os
from enum import Enum

from data_collection.AccountCollector import TransactionCollector
from utils.DataLoader import DataLoader, load_pool
from utils.Path import Path

dataloader = DataLoader()
path = Path()
transaction_collector = TransactionCollector()

REMOVE_LIQUIDITY_SUBSTRING = "removeLiquidity"
ADD_LIQUIDITY_SUBSTRING = "addLiquidity"
OUT_PERCENTAGE_THRESHOLD = 0.9
IN_PERCENTAGE_THRESHOLD = 1.0
MIN_NUMBER_OF_SATELLITES = 5
ALL_SCAMMERS = set(dataloader.scammers)
END_NODES = (
        dataloader.bridge_addresses | dataloader.defi_addresses | dataloader.cex_addresses | dataloader.MEV_addresses
        | dataloader.mixer_addresses | dataloader.wallet_addresses | dataloader.other_addresses)


class StarShape(Enum):
    IN = 1  # satellites to center
    OUT = 2  # center to satellites
    IN_OUT = 3  # mix of IN and OUT


def is_not_blank(s):
    return bool(s and not s.isspace())


def determine_assigned_star_shape(scammer_address, scammer_dict):
    f_b_tuple = scammer_dict.get(scammer_address)
    if f_b_tuple is None:
        f_b_tuple = get_funder_and_beneficiary(scammer_address)
    star_shapes = set()
    # if the funder and beneficiary are the same given they're not blank, then it can only be an IN_OUT star
    if f_b_tuple[0] == f_b_tuple[1] and is_not_blank(f_b_tuple[0]):
        star_shapes.add(StarShape.IN_OUT)
    else:
        # if there is a funder, then it's an OUT satellite
        if is_not_blank(f_b_tuple[0]):
            star_shapes.add(StarShape.OUT)
        # if there is a beneficiary, then it's an IN satellite
        if is_not_blank(f_b_tuple[1]):
            star_shapes.add(StarShape.IN)

    return star_shapes


def find_star_shapes(scammer_address):
    json_dict = {"processed_address": scammer_address,
                 "star_shapes": []}

    input_path = os.path.join(path.univ2_star_shape_path, "scammer_in_out_addresses.txt")
    scammer_dict = read_from_in_out_scammer_as_dict(input_path)

    possible_star_shapes = determine_assigned_star_shape(scammer_address, scammer_dict)

    for star_shape in possible_star_shapes:
        satellite_nodes = set()
        # find satellite node for an IN/OUT star shape
        center_address = scammer_dict[scammer_address][1 if star_shape == StarShape.IN else 0]
        normal_txs = transaction_collector.get_transactions(center_address)
        for transaction in normal_txs:
            # TODO may not work for IN_OUT. The idea is we don't have to check again for IN_OUT
            # TODO be careful for the IN_OUT case when getting the right .to or .sender
            if is_valid_address(star_shape == StarShape.OUT, transaction, center_address):
                transaction_address = transaction.to if star_shape == StarShape.OUT else transaction.sender
                if transaction_address in ALL_SCAMMERS:
                    satellite_star_shapes = determine_assigned_star_shape(transaction_address, scammer_dict)
                    if star_shape in satellite_star_shapes:
                        satellite_nodes.add(transaction_address)

        # TODO check back to constant that is 5
        # MIN_NUMBER_OF_Satellite
        if len(satellite_nodes) >= 1:
            json_dict["star_shapes"].append({
                star_shape.name: {
                    "satellite_size": len(satellite_nodes),
                    "center_address": center_address,
                    "satellites": list(satellite_nodes)
                }
            })

    # TODO move writing outside since we don't want to write every single time
    # TODO and also add to processed_star_scammers
    input_path = os.path.join(path.univ2_star_shape_path, "{}.json".format(scammer_address))

    with open(input_path, "w") as outfile:
        json.dump(json_dict, outfile, indent=2)

    return json_dict


def find_liquidity_transactions_in_pool(scammer_address):
    def calc_liquidity_amount(event, use_value):
        return event.amount0 / 10 ** 18 if use_value == 0 else event.amount1 / 10 ** 18

    # key: transaction hash, value: liquidity added/removed
    liquidity_transactions_pool = {}
    scammer_pool = load_pool(scammer_address, dataloader)
    for pool_index in range(len(scammer_pool)):
        eth_pos = scammer_pool[pool_index].get_high_value_position()
        for liquidity_trans in itertools.chain(scammer_pool[pool_index].mints, scammer_pool[pool_index].burns):
            liquidity_amount = calc_liquidity_amount(liquidity_trans, eth_pos)
            liquidity_transactions_pool[liquidity_trans.transactionHash] = liquidity_amount

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


def read_from_in_out_scammer_as_dict(input_path):
    file = open(input_path)
    funder_beneficiary_dict = {}

    for line in file:
        row = line.rstrip('\n').split(', ')
        funder_beneficiary_dict.update({row[0]: (row[1], row[2])})

    # remove the first line in the csv
    funder_beneficiary_dict.pop('address')
    file.close()

    return funder_beneficiary_dict


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
    input_path = os.path.join(path.univ2_star_shape_path, "scammer_in_out_addresses.txt")
    processed_addresses = read_from_csv(input_path)
    scammers_remaining = set(dataloader.scammers)
    # remove already written scammers from remaining
    for processed_address in processed_addresses:
        scammers_remaining.remove(processed_address)

    # lower means will write to file more frequently, but lower performance
    # higher means less file writes, but better performance
    save_file_freq = 1000
    num_scammers_to_run = 200000
    overall_scammers_written = 0

    while overall_scammers_written <= num_scammers_to_run and len(scammers_remaining) > 0:
        with open(input_path, "a") as f:
            for _ in range(save_file_freq):
                current_address = scammers_remaining.pop()
                # print('Checking address {} now'.format(current_address))
                funder, beneficiary = get_funder_and_beneficiary(current_address)
                string_to_write = '{}, {}, {}\n'.format(current_address, funder, beneficiary)
                f.write(string_to_write)
                overall_scammers_written += 1


if __name__ == '__main__':
    # in_funder, out_beneficiary = get_funder_and_beneficiary('0xb89c501e28acd743a577b94fc66fb6f2bfd75186')
    # print('before address: {}, after address: {}'.format(in_funder, out_beneficiary))
    # write_scammer_funders_and_beneficiary()
    find_star_shapes('0xc7df5da2cf8dcaa8858c06dada7cf9eba3c71fbf')
