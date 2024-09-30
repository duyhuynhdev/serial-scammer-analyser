import itertools
import os
from enum import Enum

from data_collection.AccountCollector import TransactionCollector
from utils.DataLoader import DataLoader, load_pool
from utils.ProjectPath import ProjectPath
from utils.Utils import is_contract_address

dataloader = DataLoader()
path = ProjectPath()
transaction_collector = TransactionCollector()

REMOVE_LIQUIDITY_SUBSTRING = "removeLiquidity"
ADD_LIQUIDITY_SUBSTRING = "addLiquidity"
OUT_PERCENTAGE_THRESHOLD = 0.9
IN_PERCENTAGE_THRESHOLD = 1.0
MIN_NUMBER_OF_SATELLITES = 5
ALL_SCAMMERS = frozenset(dataloader.scammers)
END_NODES = (
        dataloader.bridge_addresses | dataloader.defi_addresses | dataloader.cex_addresses | dataloader.MEV_addresses
        | dataloader.mixer_addresses | dataloader.wallet_addresses | dataloader.other_addresses)


class StarShape(Enum):
    IN = 1  # satellites to center
    OUT = 2  # center to satellites
    IN_OUT = 3  # mix of IN and OUT


def is_not_blank(s):
    return bool(s and not s.isspace())


def determine_assigned_star_shape_and_f_b(scammer_address, scammer_dict):
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

    return star_shapes, f_b_tuple


def find_star_shapes(scammer_address):
    stars = []

    input_path = os.path.join(path.univ2_star_shape_path, "scammer_in_out_addresses.csv")
    scammer_dict = read_from_in_out_scammer_as_dict(input_path)

    possible_star_shapes = determine_assigned_star_shape_and_f_b(scammer_address, scammer_dict)[0]

    for star_shape in possible_star_shapes:
        satellite_nodes = set()
        # find satellite node for an IN/OUT star shape
        center_address = scammer_dict[scammer_address][1 if star_shape == StarShape.IN else 0]
        normal_txs = transaction_collector.get_transactions(center_address)
        for transaction in normal_txs:
            if is_valid_address(star_shape == StarShape.OUT, transaction, center_address):
                transaction_address = transaction.to if star_shape == StarShape.OUT else transaction.sender
                if transaction_address in ALL_SCAMMERS:
                    satellite_star_shapes, fb_tuple = determine_assigned_star_shape_and_f_b(transaction_address, scammer_dict)
                    same_funder_or_beneficiary = center_address == (fb_tuple[0] if star_shape == StarShape.OUT else fb_tuple[1])
                    if star_shape in satellite_star_shapes and same_funder_or_beneficiary:
                        satellite_nodes.add(transaction_address)

        if len(satellite_nodes) >= MIN_NUMBER_OF_SATELLITES:
            list_to_add = [star_shape, center_address, len(satellite_nodes), satellite_nodes]
            stars.append(list_to_add)

    return stars


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


# TODO need to update
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
            if is_out and not is_contract_address(largest_transaction.to):
                return largest_transaction.to
            elif not is_out:
                return largest_transaction.sender
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


# TODO make both work at once. For a given IN/OUT star,
#  you CANNOT exclude it from the list of scammers to process, we still need to process them
# IN/OUT you definitely can -> even if the size < 5, running it again will be counted as IN/OUT and therefore fail again.
def process_stars_on_all_scammers():
    def remove_from_set(file_path, set_to_remove):
        with open(file_path, 'r') as f:
            for line in f:
                row = line.rstrip('\n').split(', ')
                for index in range(2, len(row)):
                    if is_not_blank(row[index]):
                        set_to_remove.remove(row)

    in_stars_path = os.path.join(path.univ2_star_shape_path, "in_stars.csv")
    out_stars_path = os.path.join(path.univ2_star_shape_path, "out_stars.csv")
    in_out_stars_path = os.path.join(path.univ2_star_shape_path, "in_out_stars.csv")
    no_stars_path = os.path.join(path.univ2_star_shape_path, "no_star.csv")

    in_scammers_remaining = set(dataloader.scammers)

    # remove the scammers with no stars
    with open(no_stars_path, "r") as file:
        for c_line in file:
            c_row = c_line.rstrip('\n')
            if is_not_blank(c_row):
                in_scammers_remaining.remove(c_row)

    # remove the scammers that have an in_out star since the satellites cannot belong to another star
    remove_from_set(in_out_stars_path, in_scammers_remaining)

    in_scammers_remaining.remove('scammer_address')
    out_scammers_remaining = in_scammers_remaining.copy()

    remove_from_set(in_stars_path, in_scammers_remaining)
    remove_from_set(out_stars_path, out_scammers_remaining)

    in_scammers_remaining.remove('satellites')
    out_scammers_remaining.remove('satellites')

    # start processing the writing
    save_file_freq = 1000
    scammers_to_run = 120000
    scammers_ran = 0

    pop_from_in = True

    # TODO update this logic to alternate from a pop() from each
    while scammers_ran < scammers_to_run and len(in_scammers_remaining) + len(out_scammers_remaining) > 0:
        print("Scammers ran {} and scammers left {}".format(scammers_ran, len(in_scammers_remaining) + len(out_scammers_remaining)))
        for _ in range(save_file_freq):
            with (open(in_stars_path, "a") as in_file, open(out_stars_path, "a") as out_file,
                  open(in_out_stars_path, "a") as in_out_file, open(no_stars_path, "a") as no_star_file):
                current_scammer_to_run = in_scammers_remaining.pop if pop_from_in else out_scammers_remaining.pop()
                all_stars_result = find_star_shapes(current_scammer_to_run)
                pop_from_in = not pop_from_in

                # DEBUG LOGGING
                # print("Result for scammer {}: {}".format(current_scammer_to_run, all_stars_result))

                # if no stars are found, write to the no_stars.csv
                # and remove the popped element from the other set
                if len(all_stars_result) == 0:
                    no_star_file.write(current_scammer_to_run + '\n')
                    out_scammers_remaining.discard(current_scammer_to_run)
                    in_scammers_remaining.discard(current_scammer_to_run)
                else:
                    for star in all_stars_result:
                        star_type = star[0]
                        file_to_write_to = None
                        # remove the remaining scammers from the list for respective IN/OUT file or both if it's an IN_OUT star
                        if star_type == StarShape.IN:
                            file_to_write_to = in_file
                            for satellite_scammer in star[3]:
                                in_scammers_remaining.discard(satellite_scammer)
                        elif star_type == StarShape.OUT:
                            file_to_write_to = out_file
                            for satellite_scammer in star[3]:
                                out_scammers_remaining.discard(satellite_scammer)
                        elif star_type == StarShape.IN_OUT:
                            file_to_write_to = in_out_file
                            # an IN_OUT satellites cannot be part of another star so remove from both
                            for satellite_scammer in star[3]:
                                out_scammers_remaining.discard(satellite_scammer)
                                in_scammers_remaining.discard(satellite_scammer)

                        string_to_write = '{}, {}, {}\n'.format(star[1], star[2], ', '.join(star[3]))
                        file_to_write_to.write(string_to_write)

                scammers_ran += 1


# no need to call this anymore
def write_scammer_funders_and_beneficiary():
    input_path = os.path.join(path.univ2_star_shape_path, "scammer_in_out_addresses.csv")

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

    while overall_scammers_written < num_scammers_to_run and len(scammers_remaining) > 0:
        with open(input_path, "a") as f:
            for _ in range(save_file_freq):
                current_address = scammers_remaining.pop()
                # print('Checking address {} now'.format(current_address))
                funder, beneficiary = get_funder_and_beneficiary(current_address)
                string_to_write = '{}, {}, {}\n'.format(current_address, funder, beneficiary)
                f.write(string_to_write)
                overall_scammers_written += 1


if __name__ == '__main__':
    # process_stars_on_all_scammers()
    # result = find_star_shapes('0x94f5628f2ab2efbb60d71400ad71be27fd91fe20')
    result = get_funder_and_beneficiary('0x94f5628f2ab2efbb60d71400ad71be27fd91fe20')
    [print(a) for a in result]
