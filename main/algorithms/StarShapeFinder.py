import ast
import csv
import itertools

import os
import statistics
import sys

sys.path.append(os.path.join(os.path.dirname(sys.path[0])))
from enum import Enum

from data_collection.AccountCollector import TransactionCollector
from utils.DataLoader import DataLoader, load_light_pool
from utils.ProjectPath import ProjectPath

dex = 'panv2'
dataloader = DataLoader(dex=dex)
path = ProjectPath()
transaction_collector = TransactionCollector()

SCAMMER_F_AND_B_PATH = os.path.join(path.panv2_star_shape_path, "scammer_funder_and_beneficiary.csv")

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


def get_and_save_f_and_b(scammer_address, scammer_dict):
    f_b_result = get_funder_and_beneficiary(scammer_address)
    scammer_dict[scammer_address] = f_b_result

    with open(SCAMMER_F_AND_B_PATH, "a", newline='') as file:
        csv_writer = csv.writer(file, quotechar='"', delimiter='|', quoting=csv.QUOTE_ALL)
        scammer_details = f_b_result.get('scammer_details')
        col_1 = (scammer_details['address'], scammer_details['num_scams'])

        funder_details = f_b_result.get('funder')
        col_2 = col_3 = ''
        if funder_details:
            col_2 = (funder_details['address'], funder_details['timestamp'], funder_details['amount'])

        beneficiary_details = f_b_result.get('beneficiary')
        if beneficiary_details:
            col_3 = (beneficiary_details['address'], beneficiary_details['timestamp'], beneficiary_details['amount'])
        csv_writer.writerow([col_1, col_2, col_3])


def determine_assigned_star_shape_and_f_b(scammer_address, scammer_dict):
    f_b_dict = scammer_dict.get(scammer_address)
    if f_b_dict is None:
        get_and_save_f_and_b(scammer_address, scammer_dict)
        f_b_dict = scammer_dict[scammer_address]
    star_shapes = set()
    funder_details = f_b_dict.get('funder')
    beneficiary_details = f_b_dict.get('beneficiary')
    if funder_details and beneficiary_details and funder_details['address'] == beneficiary_details['address']:
        star_shapes.add(StarShape.IN_OUT)
    else:
        if funder_details:
            star_shapes.add(StarShape.OUT)
        if beneficiary_details:
            star_shapes.add(StarShape.IN)

    return star_shapes, f_b_dict


def find_star_shape_for_scammer(scammer_address, scammer_dict=None, star_to_ignore=None):
    stars = []

    if not scammer_dict:
        scammer_dict = read_from_in_out_scammer_as_dict()

    possible_star_shapes = determine_assigned_star_shape_and_f_b(scammer_address, scammer_dict)[0]
    if star_to_ignore:
        possible_star_shapes.remove(star_to_ignore)

    for star_shape in possible_star_shapes:
        satellite_nodes = set()
        # LOGIC if it's an IN or IN_OUT star, get the beneficiary. For IN_OUT, we only need to look at half of the transactions
        if star_shape == StarShape.IN or star_shape == StarShape.IN_OUT:
            center_address = scammer_dict[scammer_address]['beneficiary']['address']
            is_out = False
        # LOGIC for an out star, get the funder address
        elif star_shape == StarShape.OUT:
            center_address = scammer_dict[scammer_address]['funder']['address']
            is_out = True
        else:
            raise Exception("There was no star detected in the possible_star_shapes")

        normal_txs, _ = transaction_collector.get_transactions(center_address, dex)
        for transaction in normal_txs:
            if is_valid_address(is_out, transaction, center_address):
                scammer_address_dest = transaction.to if is_out else transaction.sender
                if scammer_address_dest in ALL_SCAMMERS:
                    satellite_star_shapes, f_b_dict = determine_assigned_star_shape_and_f_b(scammer_address_dest, scammer_dict)
                    if star_shape in satellite_star_shapes:
                        # LOGIC for an OUT star, center sends to satellites so check the best funder of the satellite is same as center
                        use_funder_or_beneficiary = None
                        if is_out:
                            use_funder_or_beneficiary = 'funder'
                        # LOGIC, otherwise for an IN or IN_OUT star, we need to see who the satellites send money to which will match if same center address
                        else:
                            use_funder_or_beneficiary = 'beneficiary'
                        if center_address == f_b_dict[use_funder_or_beneficiary]['address']:
                            num_scams = f_b_dict['scammer_details']['num_scams']
                            timestamp = f_b_dict[use_funder_or_beneficiary]['timestamp']
                            eth_amount = f_b_dict[use_funder_or_beneficiary]['amount']
                            # LOGIC add to satellite if match
                            satellite_nodes.add((scammer_address_dest, num_scams, int(timestamp), eth_amount))

        if len(satellite_nodes) >= MIN_NUMBER_OF_SATELLITES:
            sorted_satellites = sorted(satellite_nodes, key=lambda x: x[2])
            list_to_add = [star_shape, center_address, sorted_satellites]
            stars.append(list_to_add)

    return stars


def find_liquidity_transactions_in_pool(scammer_address):
    def calc_liquidity_amount(event, use_value):
        return event.amount0 / 10 ** 18 if use_value == 0 else event.amount1 / 10 ** 18

    # key: transaction hash
    # value: liquidity added/removed
    liquidity_transactions_pool = {}
    scammer_pool = load_light_pool(scammer_address, dataloader, dex=dex)
    for pool_index in range(len(scammer_pool)):
        eth_pos = scammer_pool[pool_index].get_high_value_position()
        for liquidity_trans in itertools.chain(scammer_pool[pool_index].mints, scammer_pool[pool_index].burns):
            liquidity_amount = calc_liquidity_amount(liquidity_trans, eth_pos)
            liquidity_transactions_pool[liquidity_trans.transactionHash] = liquidity_amount

    return liquidity_transactions_pool


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
    largest_in_transaction = largest_out_transaction = None
    add_liquidity_amt = remove_liquidity_amt = 0
    out_addresses = set()
    in_addresses = set()
    num_remove_liquidities = 0
    passed_add_liquidity = passed_remove_liquidity = False
    duplicate_in_amt = duplicate_out_amt = False
    normal_txs, internal_txs = transaction_collector.get_transactions(scammer_address, dex)
    liq_trans_dict = find_liquidity_transactions_in_pool(scammer_address)

    for transaction in normal_txs:
        if transaction.is_not_error():
            # LOGIC upon passing the first add liquidity, mark down the amount and don't check any more add liquidaties
            if not passed_add_liquidity and ADD_LIQUIDITY_SUBSTRING in str(transaction.functionName):
                decode_valid_add = False
                candidate_add_liquidity_amt = liq_trans_dict.get(transaction.hash)
                if candidate_add_liquidity_amt:
                    decode_valid_add = True
                if decode_valid_add:
                    if candidate_add_liquidity_amt > 0.0:
                        passed_add_liquidity = True
                        add_liquidity_amt = candidate_add_liquidity_amt
            # LOGIC upon passing a remove liquidity - current largest_out becomes ineligible
            elif REMOVE_LIQUIDITY_SUBSTRING in str(transaction.functionName):
                decode_valid_remove = False
                candidate_rmv_amt = liq_trans_dict.get(transaction.hash)
                if candidate_rmv_amt:
                    decode_valid_remove = True
                if decode_valid_remove:
                    if candidate_rmv_amt > 0.0:
                        passed_remove_liquidity = True
                        largest_out_transaction = None
                        duplicate_out_amt = False
                        num_remove_liquidities += 1
                        remove_liquidity_amt = candidate_rmv_amt
            # LOGIC transaction before we encounter the first add liquidity, find the largest IN transaction
            if is_valid_address(False, transaction, scammer_address):
                # LOGIC still track all the IN addresses even if we passed add liqudity
                in_addresses.add(transaction.sender)
                # LOGIC assume first IN is largest
                if not passed_add_liquidity:
                    if not largest_in_transaction:
                        largest_in_transaction = transaction
                    elif transaction.get_transaction_amount() >= largest_in_transaction.get_transaction_amount():
                        # LOGIC if amount is the same, mark as duplicate, otherwise this becomes new largest transaction
                        if transaction.get_transaction_amount() == largest_in_transaction.get_transaction_amount():
                            duplicate_in_amt = True
                        else:
                            duplicate_in_amt = False
                            largest_in_transaction = transaction
            # LOGIC OUT transaction
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
        passed_in_threshold = False
        passed_out_threshold = False
        valid_out_address = False
        if largest_in_transaction:
            passed_in_threshold = largest_in_transaction.get_transaction_amount_and_fee() / add_liquidity_amt >= IN_PERCENTAGE_THRESHOLD
        if largest_out_transaction:
            passed_out_threshold = largest_out_transaction.get_transaction_amount_and_fee() / remove_liquidity_amt >= OUT_PERCENTAGE_THRESHOLD
            valid_out_address = transaction_collector.ensure_valid_eoa_address(largest_out_transaction.to, dex)

        # LOGIC case where the in sender and out receiver are the same for IN_OUT star
        if valid_out_address and passed_in_threshold and passed_out_threshold and largest_in_transaction and largest_out_transaction and not duplicate_out_amt and not duplicate_in_amt and largest_in_transaction.sender == largest_out_transaction.to:
            funder_dict = get_dict_info(largest_in_transaction, largest_in_transaction.sender)
            beneficiary_dict = get_dict_info(largest_out_transaction, largest_out_transaction.to)
        else:
            # LOGIC for funder, if it didn't perform any out transactions, no duplicate, passed the threshold then add
            if largest_in_transaction:
                if passed_in_threshold and not duplicate_in_amt and largest_in_transaction.sender not in out_addresses and transaction_collector.ensure_valid_eoa_address(
                        largest_in_transaction.sender, dex):
                    funder_dict = get_dict_info(largest_in_transaction, largest_in_transaction.sender)

            # LOGIC for beneficiary, if it didn't perform any in transactions, no duplicate, and passed the threshold and is not a contract address
            if largest_out_transaction:
                if passed_out_threshold and not duplicate_out_amt and largest_out_transaction.to not in in_addresses and valid_out_address:
                    beneficiary_dict = get_dict_info(largest_out_transaction, largest_out_transaction.to)

    if funder_dict:
        results_dict.update({'funder': funder_dict})
    if beneficiary_dict:
        results_dict.update({'beneficiary': beneficiary_dict})

    return results_dict


def is_valid_address(is_out, transaction, scammer_address):
    if is_out:
        return transaction.is_to_eoa(scammer_address) and transaction.to not in END_NODES
    elif not is_out:
        return transaction.is_in_tx(scammer_address) and transaction.sender not in END_NODES
    return False


def read_from_in_out_scammer_as_dict():
    funder_beneficiary_dict = {}
    with open(SCAMMER_F_AND_B_PATH, "r", newline='') as file:
        reader = csv.reader(file, quotechar='"', delimiter='|', quoting=csv.QUOTE_ALL)
        next(reader)
        for line in reader:
            scammer_details = ast.literal_eval(line[0])
            funder_details = ast.literal_eval(line[1]) if is_not_blank(line[1]) else None
            beneficiary_details = ast.literal_eval(line[2]) if is_not_blank(line[2]) else None
            dict_to_add = {
                'scammer_details': {
                    'address': scammer_details[0],
                    'num_scams': scammer_details[1]
                }}
            if funder_details:
                dict_to_add.update({'funder': {
                    'address': funder_details[0],
                    'timestamp': funder_details[1],
                    'amount': funder_details[2],
                }})
            if beneficiary_details:
                dict_to_add.update({'beneficiary': {
                    'address': beneficiary_details[0],
                    'timestamp': beneficiary_details[1],
                    'amount': beneficiary_details[2],
                }})

            funder_beneficiary_dict[scammer_details[0]] = dict_to_add

    return funder_beneficiary_dict


def process_stars_on_all_scammers():
    def remove_from_set(file_path, set_to_remove):
        with open(file_path, 'r', newline='') as f:
            reader_f = csv.reader(f, quotechar='"', delimiter='|', quoting=csv.QUOTE_ALL)
            next(reader_f)
            for line_r in reader_f:
                scammer_chain = ast.literal_eval(line_r[2])
                for scammer in scammer_chain:
                    set_to_remove.discard(scammer[0])

    in_stars_path = os.path.join(path.panv2_star_shape_path, "in_stars.csv")
    out_stars_path = os.path.join(path.panv2_star_shape_path, "out_stars.csv")
    in_out_stars_path = os.path.join(path.panv2_star_shape_path, "in_out_stars.csv")
    no_stars_path = os.path.join(path.panv2_star_shape_path, "no_star.csv")

    in_scammers_remaining = set(dataloader.scammers)

    # remove the scammers with no stars
    with open(no_stars_path, "r", newline='') as file:
        reader = csv.reader(file, quotechar='"', delimiter='|', quoting=csv.QUOTE_ALL)
        next(reader)
        for line in reader:
            in_scammers_remaining.discard(line[0])

    # LOGIC remove the scammers that have an in_out star since the satellites cannot belong to another star
    remove_from_set(in_out_stars_path, in_scammers_remaining)
    out_scammers_remaining = in_scammers_remaining.copy()
    # LOGIC remove IN scammers from IN set and OUT scammers from OUT set
    remove_from_set(in_stars_path, in_scammers_remaining)
    remove_from_set(out_stars_path, out_scammers_remaining)

    # start processing the writing
    save_file_freq = 500
    scammers_to_run = 1_000_000
    scammers_ran = 0

    scammer_dict = read_from_in_out_scammer_as_dict()
    pop_from_in = True

    while scammers_ran < scammers_to_run and len(in_scammers_remaining) + len(out_scammers_remaining) > 0:
        print("Scammers ran {} and scammers left {}".format(scammers_ran, len(in_scammers_remaining) + len(out_scammers_remaining)))
        with (open(in_stars_path, "a", newline='') as in_file, open(out_stars_path, "a", newline='') as out_file, open(in_out_stars_path, "a", newline='') as in_out_file, open(no_stars_path, "a",
                                                                                                                                                                                newline='') as no_star_file):
            in_star_writer = csv.writer(in_file, quotechar='"', delimiter='|', quoting=csv.QUOTE_ALL)
            out_star_writer = csv.writer(out_file, quotechar='"', delimiter='|', quoting=csv.QUOTE_ALL)
            in_out_star_writer = csv.writer(in_out_file, quotechar='"', delimiter='|', quoting=csv.QUOTE_ALL)
            no_star_writer = csv.writer(no_star_file, quotechar='"', delimiter='|', quoting=csv.QUOTE_ALL)
            for _ in range(save_file_freq):
                current_scammer_to_run = ''
                if pop_from_in and len(in_scammers_remaining) > 0:
                    current_scammer_to_run = in_scammers_remaining.pop()
                elif not pop_from_in and len(out_scammers_remaining) > 0:
                    current_scammer_to_run = out_scammers_remaining.pop()
                else:
                    if len(in_scammers_remaining) > 0:
                        current_scammer_to_run = in_scammers_remaining.pop()
                        pop_from_in = True
                    elif len(out_scammers_remaining) > 0:
                        current_scammer_to_run = out_scammers_remaining.pop()
                        pop_from_in = False
                    else:
                        print('no more scammers remaining')
                        break

                print("Current_scammer={}, scammers_ran={}, in_scammers_left={}, out_scammers_left={}".format(current_scammer_to_run, scammers_ran, len(in_scammers_remaining),
                                                                                                              len(out_scammers_remaining)))
                # LOGIC if the removed scammer address doesn't exist in the other set, don't look for that star again
                star_to_ignore = None
                if pop_from_in and current_scammer_to_run not in out_scammers_remaining:
                    star_to_ignore = StarShape.OUT
                elif not pop_from_in and current_scammer_to_run not in in_scammers_remaining:
                    star_to_ignore = StarShape.IN
                all_stars_result = find_star_shape_for_scammer(current_scammer_to_run, scammer_dict, star_to_ignore)

                # LOGIC if no stars are found and we didn't ignore a star, write to the no_stars.csv
                if len(all_stars_result) == 0 and star_to_ignore is None:
                    no_star_writer.writerow([current_scammer_to_run])
                else:
                    for star in all_stars_result:
                        star_type = star[0]
                        csv_to_write_to = None
                        # remove the satellites from the respective set
                        if star_type == StarShape.IN:
                            csv_to_write_to = in_star_writer
                            for satellite_scammer in star[2]:
                                in_scammers_remaining.discard(satellite_scammer[0])
                        elif star_type == StarShape.OUT:
                            csv_to_write_to = out_star_writer
                            for satellite_scammer in star[2]:
                                out_scammers_remaining.discard(satellite_scammer[0])
                        elif star_type == StarShape.IN_OUT:
                            csv_to_write_to = in_out_star_writer
                            # an IN_OUT satellites cannot be part of another star so remove from both
                            for satellite_scammer in star[2]:
                                in_scammers_remaining.discard(satellite_scammer[0])
                                out_scammers_remaining.discard(satellite_scammer[0])

                        if csv_to_write_to:
                            csv_to_write_to.writerow([star[1], len(star[2]), star[2]])
                # since we just wrote all the results including IN/OUT, can just remove from other stack
                in_scammers_remaining.discard(current_scammer_to_run)
                out_scammers_remaining.discard(current_scammer_to_run)
                pop_from_in = not pop_from_in
                scammers_ran += 1


def write_chain_stats_on_data():
    in_stars_path = os.path.join(path.panv2_star_shape_path, "in_stars.csv")
    out_stars_path = os.path.join(path.panv2_star_shape_path, "out_stars.csv")
    in_out_stars_path = os.path.join(path.panv2_star_shape_path, "in_out_stars.csv")

    def create_reader_and_skip(file):
        reader = csv.reader(file, quotechar='"', delimiter='|', quoting=csv.QUOTE_ALL)
        next(reader)
        return reader

    all_stars = []

    with open(in_stars_path, 'r', newline='') as in_file, open(out_stars_path, 'r', newline='') as out_file, open(in_out_stars_path, 'r', newline='') as in_out_file:
        in_reader = create_reader_and_skip(in_file)
        out_reader = create_reader_and_skip(out_file)
        in_out_reader = create_reader_and_skip(in_out_file)

        # LOGIC Read in data
        for line in in_reader:
            all_stars.append([StarShape.IN, line[0], line[1], ast.literal_eval(line[2])])
        for line in out_reader:
            all_stars.append([StarShape.OUT, line[0], line[1], ast.literal_eval(line[2])])
        for line in in_out_reader:
            all_stars.append([StarShape.IN_OUT, line[0], line[1], ast.literal_eval(line[2])])

    scammer_f_b_dict = read_from_in_out_scammer_as_dict()
    star_stats_path = os.path.join(path.panv2_star_shape_path, "star_stats.csv")
    star_stats_header = ["star_type", "center_address", "star_size", "funds_in_to_center_avg", "funds_in_to_center_total", "funds_out_from_center_avg", "funds_out_from_center_total", "scam_duration",
                         "num_scams_total"]
    with open(star_stats_path, "w", newline='') as star_stats_file:
        csv_writer = csv.writer(star_stats_file, quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL)
        csv_writer.writerow(star_stats_header)
        # LOGIC for all stars
        for star in all_stars:
            funds_in = []
            funds_out = []
            num_scams_total = []
            transfer_timestamps = []
            for satellite in star[3]:
                num_scams_total.append(satellite[1])
                transfer_timestamps.append(satellite[2])
                if star[0] == StarShape.IN:
                    funds_in.append(satellite[3])
                elif star[0] == StarShape.OUT:
                    funds_out.append(satellite[3])
                elif star[0] == StarShape.IN_OUT:
                    funds_in.append(satellite[3])
                    funder_from_dict = scammer_f_b_dict[satellite[0]]['funder']
                    funds_out.append(funder_from_dict['amount'])
                    transfer_timestamps.append(funder_from_dict['timestamp'])

            funds_in_avg = ''
            funds_in_total = ''
            if len(funds_in) > 0:
                funds_in_avg = statistics.mean(funds_in)
                funds_in_total = sum(funds_in)
            funds_out_avg = ''
            funds_out_total = ''
            if len(funds_out) > 0:
                funds_out_avg = statistics.mean(funds_out)
                funds_out_total = sum(funds_out)
            scam_duration = max(transfer_timestamps) - min(transfer_timestamps)
            csv_writer.writerow([star[0].name, star[1], star[2], funds_in_avg, funds_in_total, funds_out_avg, funds_out_total, scam_duration, sum(num_scams_total)])


if __name__ == '__main__':
    # process_stars_on_all_scammers()
    write_chain_stats_on_data()
    # get_and_save_f_and_b()
    # find_star_shape_for_scammer("0x346a75e69b77d0b6f128b34014485366cd88f7f8")
    # print(get_funder_and_beneficiary("0x396a47f56f1c46dabd3a789918bfeb7f8da9534a"))
