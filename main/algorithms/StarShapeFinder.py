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

SCAMMER_IN_OUT_PATH = os.path.join(path.univ2_star_shape_path, "scammer_in_out_addresses.csv")

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
        with open(SCAMMER_IN_OUT_PATH, "a") as file:
            string_to_write = '{}, {}, {}\n'.format(scammer_address, f_b_tuple[0], f_b_tuple[1])
            file.write(string_to_write)
            scammer_dict[scammer_address] = f_b_tuple
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


# TODO This needs to be rework
# additional param exclude_star_shape flag which won't look at IN/OUT
def find_star_shapes(scammer_address, star_to_ignore=None):
    stars = []

    input_path = os.path.join(path.univ2_star_shape_path, "scammer_in_out_addresses.csv")
    scammer_dict = read_from_in_out_scammer_as_dict(input_path)

    possible_star_shapes = determine_assigned_star_shape_and_f_b(scammer_address, scammer_dict)[0]
    if star_to_ignore:
        possible_star_shapes.remove(star_to_ignore)

    for star_shape in possible_star_shapes:
        satellite_nodes = set()
        # LOGIC find satellite node for an IN/OUT star shape
        center_address = scammer_dict[scammer_address][1 if star_shape == StarShape.IN else 0]
        normal_txs = transaction_collector.get_transactions(center_address)
        for transaction in normal_txs:
            if is_valid_address(star_shape == StarShape.OUT, transaction, center_address):
                scammer_address_dest = transaction.to if star_shape == StarShape.OUT else transaction.sender
                if scammer_address_dest in ALL_SCAMMERS:
                    satellite_star_shapes, fb_tuple = determine_assigned_star_shape_and_f_b(scammer_address_dest, scammer_dict)
                    same_funder_or_beneficiary = center_address == (fb_tuple[0] if star_shape == StarShape.OUT else fb_tuple[1])
                    if star_shape in satellite_star_shapes and same_funder_or_beneficiary:
                        satellite_nodes.add(scammer_address_dest)

        if len(satellite_nodes) >= MIN_NUMBER_OF_SATELLITES:
            list_to_add = [star_shape, center_address, len(satellite_nodes), satellite_nodes]
            stars.append(list_to_add)

    return stars


def find_liquidity_transactions_in_pool(scammer_address):
    def calc_liquidity_amount(event, use_value):
        return event.amount0 / 10 ** 18 if use_value == 0 else event.amount1 / 10 ** 18

    # key: transaction hash
    # value: liquidity added/removed
    liquidity_transactions_pool = {}
    scammer_pool = load_pool(scammer_address, dataloader)
    for pool_index in range(len(scammer_pool)):
        eth_pos = scammer_pool[pool_index].get_high_value_position()
        for liquidity_trans in itertools.chain(scammer_pool[pool_index].mints, scammer_pool[pool_index].burns):
            liquidity_amount = calc_liquidity_amount(liquidity_trans, eth_pos)
            liquidity_transactions_pool[liquidity_trans.transactionHash] = liquidity_amount

    return liquidity_transactions_pool


# TODO this needs to be reworked
# A funder cannot also be sent money back from the center except if it's an IN/OUT star
# A beneficiary cannot also fund the money to the satellite except if it's an IN/OUT star
# You can use a flag to determine if it's passed the same address but don't discard it until the end.
def get_funder_and_beneficiary(scammer_address):
    '''Return a dictionary object containg the following
    { scammer: (addresss, scams_performed),
      funder: (address, timestamp, raw eth amount) (can be `None`)
      beneficiary: (address, timestamp, raw eth amount) (can be `None`)
    }
    '''
    # used to determine if a funder/beneficiary doesn't perform opposite transaction for an IN/OUT star -
    # if so, the funder/beneficary will become ineligible but not for an IN_OUT star
    largest_in_transaction = largest_out_transaction = None
    add_liquidity_amt = remove_liquidity_amt = 0
    out_addresses = in_addresses = set()
    num_remove_liquidities = 0
    passed_add_liquidity = passed_remove_liquidity = False
    duplicate_in_amt = duplicate_out_amt = False
    liquidity_transactions_dict = find_liquidity_transactions_in_pool(scammer_address)
    normal_txs = transaction_collector.get_transactions(scammer_address)

    for transaction in normal_txs:
        if transaction.is_not_error():
            # LOGIC upon passing the first add liquidity, mark down the amount and don't check any more add liquidites
            if not passed_add_liquidity and ADD_LIQUIDITY_SUBSTRING in str(transaction.functionName):
                passed_add_liquidity = True
                candidate_liq_amt = liquidity_transactions_dict.get(transaction.hash)
                if candidate_liq_amt:
                    add_liquidity_amt = candidate_liq_amt
            # LOGIC upon passing a remove liquidity - current largest_out becomes ineligible
            elif REMOVE_LIQUIDITY_SUBSTRING in str(transaction.functionName):
                passed_remove_liquidity = True
                largest_out_transaction = None
                candidate_liq_amt = liquidity_transactions_dict.get(transaction.hash)
                if candidate_liq_amt:
                    num_remove_liquidities += 1
                    remove_liquidity_amt = candidate_liq_amt

            # LOGIC transaction before we encounter the first add liquidity, find the largest IN transaction
            if not passed_add_liquidity and is_valid_address(False, transaction, scammer_address):
                in_addresses.add(transaction.sender)
                # LOGIC assume first IN is largest
                if not largest_in_transaction:
                    largest_in_transaction = transaction
                elif transaction.get_transaction_amount() >= largest_in_transaction.get_transaction_amount():
                    # LOGIC if amount is the same, mark as duplicate, otherwise this becomes new largest transaction
                    if transaction.get_transaction_amount() == largest_in_transaction.get_transaction_amount():
                        duplicate_in_amt = True
                    else:
                        duplicate_in_amt = False
                        largest_in_transaction = transaction
            # OUT transaction logic
            elif is_valid_address(True, transaction, scammer_address):
                out_addresses.add(transaction.to)
                # LOGIC set largest_out when none is a candidate
                if not largest_out_transaction:
                    largest_out_transaction = transaction
                elif transaction.get_transaction_amount() >= largest_out_transaction.get_transaction_amount():
                    # LOGIC if out is the same, duplicate amount so this becomes invalid, otherwise becomes new largest out
                    if transaction.get_transaction_amount() == largest_out_transaction.get_transaction_amount():
                        duplicate_out_amt = True
                    else:
                        duplicate_out_amt = False
                        largest_out_transaction = transaction

    results_dict = {
        'scammer_details': {'address': scammer_address, 'num_scams': num_remove_liquidities}
    }
    funder_dict = beneficiary_dict = None

    def get_dict_info(normal_tx, address):
        return {'address': address, 'timestamp': normal_tx.timeStamp, 'amount': normal_tx.get_transaction_amount()}

    if passed_add_liquidity and passed_remove_liquidity:
        # LOGIC case where the in sender and out receiver are the same for IN_OUT star
        if largest_in_transaction and largest_out_transaction and not duplicate_out_amt and not duplicate_in_amt and largest_in_transaction.sender == largest_out_transaction.to:
            funder_dict = get_dict_info(largest_in_transaction, largest_in_transaction.sender)
            beneficiary_dict = get_dict_info(largest_out_transaction, largest_out_transaction.to)
        else:
            # LOGIC for funder, if it didn't perform any out transcations, no duplicate, passed the threshold then add
            if largest_in_transaction:
                passed_threshold = largest_in_transaction.get_transaction_amount_and_fee() / add_liquidity_amt >= IN_PERCENTAGE_THRESHOLD
                if passed_threshold and not duplicate_in_amt and largest_in_transaction.sender not in out_addresses:
                    funder_dict = get_dict_info(largest_in_transaction, largest_in_transaction.sender)

            # LOGIC for beneficiary, if it didn't perform any in transactions,
            # LOGIC no duplicate, and passed the threshold and is not an
            if largest_out_transaction:
                passed_threshold = largest_out_transaction.get_transaction_amount_and_fee() / remove_liquidity_amt >= OUT_PERCENTAGE_THRESHOLD
                if passed_threshold and not duplicate_out_amt and largest_out_transaction.to not in in_addresses and not is_contract_address(largest_transaction.to):
                    beneficiary_dict = get_dict_info(largest_out_transaction, largest_out_transaction.to)

    if funder_dict:
        results_dict.update({'funder': funder_dict})
    if beneficiary_dict:
        results_dict.update({'beneficiary': beneficiary_dict})

    return results_dict

# REFERENCE unusued
@deprecated("old function to get the largest address - this is no longer used")
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


def process_stars_on_all_scammers():
    def remove_from_set(file_path, set_to_remove):
        with open(file_path, 'r') as f:
            for line in f:
                row = line.rstrip('\n').split(', ')
                for index in range(2, len(row)):
                    if is_not_blank(row[index]) and row[index] != 'satellites':
                        # change back to remove
                        set_to_remove.remove(row[index])

    in_stars_path = os.path.join(path.univ2_star_shape_path, "in_stars.csv")
    out_stars_path = os.path.join(path.univ2_star_shape_path, "out_stars.csv")
    in_out_stars_path = os.path.join(path.univ2_star_shape_path, "in_out_stars.csv")
    no_stars_path = os.path.join(path.univ2_star_shape_path, "no_star.csv")

    in_scammers_remaining = set(dataloader.scammers)

    # remove the scammers with no stars
    with open(no_stars_path, "r") as file:
        for c_line in file:
            c_row = c_line.rstrip('\n')
            if is_not_blank(c_row) and c_row != 'scammer_address':
                # we will copy this to the out list later
                in_scammers_remaining.remove(c_row)

    # remove the scammers that have an in_out star since the satellites cannot belong to another star
    remove_from_set(in_out_stars_path, in_scammers_remaining)
    out_scammers_remaining = in_scammers_remaining.copy()
    remove_from_set(in_stars_path, in_scammers_remaining)
    remove_from_set(out_stars_path, out_scammers_remaining)

    # start processing the writing
    save_file_freq = 1000
    scammers_to_run = 200000
    scammers_ran = 0

    pop_from_in = True

    while scammers_ran < scammers_to_run and len(in_scammers_remaining) + len(out_scammers_remaining) > 0:
        print("Scammers ran {} and scammers left {}".format(scammers_ran, len(in_scammers_remaining) + len(out_scammers_remaining)))
        # TODO this needs to be changed to use the other way of opening/writing to csv
        with (open(in_stars_path, "a") as in_file, open(out_stars_path, "a") as out_file, open(in_out_stars_path, "a") as in_out_file, open(no_stars_path, "a") as no_star_file):
            for _ in range(save_file_freq):
                current_scammer_to_run = in_scammers_remaining.pop() if pop_from_in else out_scammers_remaining.pop()
                all_stars_result = find_star_shapes(current_scammer_to_run)

                # DEBUG LOGGING
                # print("Result for scammer {}: {}".format(current_scammer_to_run, all_stars_result))

                # if no stars are found, write to the no_stars.csv
                if len(all_stars_result) == 0:
                    no_star_file.write(current_scammer_to_run + '\n')
                else:
                    for star in all_stars_result:
                        star_type = star[0]
                        file_to_write_to = None
                        # remove the satellites from the respective set
                        if star_type == StarShape.IN and bool(star[3] & in_scammers_remaining):
                            file_to_write_to = in_file
                            for satellite_scammer in star[3]:
                                in_scammers_remaining.discard(satellite_scammer)

                        elif star_type == StarShape.OUT and bool(star[3] & out_scammers_remaining):
                            file_to_write_to = out_file
                            for satellite_scammer in star[3]:
                                out_scammers_remaining.discard(satellite_scammer)
                        elif star_type == StarShape.IN_OUT:
                            file_to_write_to = in_out_file
                            # an IN_OUT satellites cannot be part of another star so remove from both
                            for satellite_scammer in star[3]:
                                in_scammers_remaining.discard(satellite_scammer)
                                out_scammers_remaining.discard(satellite_scammer)

                        if file_to_write_to:
                            string_to_write = '{}, {}, {}\n'.format(star[1], star[2], ', '.join(star[3]))
                            # DEBUG LOGGING
                            # print('string_to_write is {} with the current_scammer={} and pop_from_in={}\n'.format(string_to_write, current_scammer_to_run, str(pop_from_in)))
                            file_to_write_to.write(string_to_write)
                # since we just wrote all the results including IN/OUT, can just remove from other stack
                in_scammers_remaining.discard(current_scammer_to_run)
                out_scammers_remaining.discard(current_scammer_to_run)
                pop_from_in = not pop_from_in
                scammers_ran += 1


@deprecated("No point writing beforehand but can still use")
def write_scammer_funders_and_beneficiary():
    processed_addresses = read_from_csv(SCAMMER_IN_OUT_PATH)
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
        print("Scammers ran {} and scammers left {}".format(overall_scammers_written, len(scammers_remaining)))
        with open(SCAMMER_IN_OUT_PATH, "a") as f:
            for _ in range(save_file_freq):
                current_address = scammers_remaining.pop()
                # print('Checking address {} now'.format(current_address))
                funder, beneficiary = get_funder_and_beneficiary(current_address)
                string_to_write = '{}, {}, {}\n'.format(current_address, funder, beneficiary)
                f.write(string_to_write)
                overall_scammers_written += 1


if __name__ == '__main__':
    # result = find_star_shapes('0x5d65491fa3f9422e8bb75dbd57f675b562029a36')
    # print(result)
    process_stars_on_all_scammers()
    # result = get_funder_and_beneficiary('0x94f5628f2ab2efbb60d71400ad71be27fd91fe20')
    # write_scammer_funders_and_beneficiary()
