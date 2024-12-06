import ast
import csv
import itertools
import os
import statistics

# from algorithms.ScammerNetworkBuilder import dataloader
from data_collection.AccountCollector import TransactionCollector
from utils.DataLoader import DataLoader, load_pool, load_light_pool
from utils.ProjectPath import ProjectPath
from utils.Utils import TransactionUtils
dex="panv2"
dataloader = DataLoader(dex=dex)
path = ProjectPath()
transaction_collector = TransactionCollector()

REMOVE_LIQUIDITY_SUBSTRING = "removeLiquidity"
ADD_LIQUIDITY_SUBSTRING = "addLiquidity"
MIN_CHAIN_LENGTH = 2


def chain_pattern_detection(starter_address):
    """Function that returns a single chain list scam from a scammer address. Includes the entire chain the address
    is involved in"""
    fwd_chain = []
    valid_address_fwd = valid_address_bwd = starter_address in dataloader.scammers
    if valid_address_fwd:
        fwd_chain.append([starter_address])

    starter_transaction_history, _ = transaction_collector.get_transactions(starter_address,dex)
    current_transaction_history = starter_transaction_history
    current_address = starter_address

    # chain forward
    while valid_address_fwd:
        valid_address_fwd = False
        largest_out_transaction, _, num_remove_calls = get_largest_out_after_remove_liquidity(current_address, current_transaction_history)
        if largest_out_transaction and largest_out_transaction.to in dataloader.scammers:
            largest_in_transaction, next_transaction_history, _ = get_largest_in_before_add_liquidity(largest_out_transaction.to)
            if largest_in_transaction:
                valid_address_fwd = largest_out_transaction.hash == largest_in_transaction.hash

        if valid_address_fwd:
            # we now have data of the current node, append that
            fwd_chain[-1].extend([num_remove_calls, largest_out_transaction.timeStamp, largest_out_transaction.get_transaction_amount()])
            current_address = largest_in_transaction.to
            current_transaction_history = next_transaction_history
            fwd_chain.append([current_address])

    if len(fwd_chain) > 0:
        num_remove_calls = count_number_of_remove_liquidity_calls(current_address, current_transaction_history)
        fwd_chain[-1].append(num_remove_calls)

    current_address = starter_address
    current_transaction_history = starter_transaction_history
    bwd_chain = []
    while valid_address_bwd:
        valid_address_bwd = False
        largest_in_transaction = get_largest_in_before_add_liquidity(current_address, current_transaction_history)[0]
        if largest_in_transaction and largest_in_transaction.sender in dataloader.scammers:
            largest_out_transaction, prev_transaction_history, num_remove_calls = get_largest_out_after_remove_liquidity(largest_in_transaction.sender)
            if largest_out_transaction:
                valid_address_bwd = largest_out_transaction.hash == largest_in_transaction.hash

        if valid_address_bwd:
            current_address = largest_out_transaction.sender
            current_transaction_history = prev_transaction_history
            bwd_chain.append([current_address, num_remove_calls, largest_in_transaction.timeStamp, largest_in_transaction.get_transaction_amount()])

    bwd_chain = bwd_chain[::-1]
    complete_chain = bwd_chain + fwd_chain
    return complete_chain if len(complete_chain) >= MIN_CHAIN_LENGTH else []


def count_number_of_remove_liquidity_calls(scammer_address, normal_txs=None):
    num_remove_liquidity_calls = 0
    valid_liquidity_set = return_valid_liquidity_transactions_bool(scammer_address)
    if normal_txs is None:
        normal_txs, _ = transaction_collector.get_transactions(scammer_address, dex)

    for transaction in normal_txs:
        if REMOVE_LIQUIDITY_SUBSTRING in str(transaction.functionName) and not transaction.isError:
            if TransactionUtils.is_scam_remove_liq(transaction, dataloader) or transaction.hash in valid_liquidity_set:
                num_remove_liquidity_calls += 1

    return num_remove_liquidity_calls


def get_largest_out_after_remove_liquidity(scammer_address: str, normal_txs=None):
    if normal_txs is None:
        normal_txs, _ = transaction_collector.get_transactions(scammer_address,dex=dex)
    return get_largest_transaction(normal_txs, scammer_address, REMOVE_LIQUIDITY_SUBSTRING, True, len(normal_txs) - 1, -1, -1)


def get_largest_in_before_add_liquidity(scammer_address: str, normal_txs=None):
    if normal_txs is None:
        normal_txs, _ = transaction_collector.get_transactions(scammer_address, dex)
    return get_largest_transaction(normal_txs, scammer_address, ADD_LIQUIDITY_SUBSTRING, False, 0, len(normal_txs), 1)


def return_valid_liquidity_transactions_bool(scammer_address):
    valid_liquidity_transactions = set()
    scammer_pool = load_light_pool(scammer_address, dataloader, dex=dex)
    for pool in scammer_pool:
        for liq_trans in itertools.chain(pool.burns, pool.mints):
            valid_liquidity_transactions.add(liq_trans.transactionHash)

    return valid_liquidity_transactions


def get_largest_transaction(normal_txs, scammer_address, liquidity_function_name: str, is_out, *range_loop_args):
    num_remove_liquidities_found = 0
    passed_liquidity_function = False
    exists_duplicate_amount = False
    largest_transaction = None
    set_of_valid_liq_trans = return_valid_liquidity_transactions_bool(scammer_address)

    for index in range(range_loop_args[0], range_loop_args[1], range_loop_args[2]):
        if liquidity_function_name in str(normal_txs[index].functionName) and not normal_txs[index].isError:
            is_valid_liq_trans = TransactionUtils.is_scam_add_liq(normal_txs[index], dataloader) or TransactionUtils.is_scam_remove_liq(normal_txs[index], dataloader)
            if not is_valid_liq_trans:
                is_valid_liq_trans = normal_txs[index].hash in set_of_valid_liq_trans
            if is_valid_liq_trans:
                passed_liquidity_function = True
                if liquidity_function_name == REMOVE_LIQUIDITY_SUBSTRING:
                    num_remove_liquidities_found += 1
                if largest_transaction is None:
                    return None, normal_txs, num_remove_liquidities_found
        elif (is_out and normal_txs[index].is_to_eoa(scammer_address)) or (not is_out and normal_txs[index].is_in_tx(scammer_address)):
            # just set the largest_transaction for the first find
            if largest_transaction is None:
                largest_transaction = normal_txs[index]
            elif normal_txs[index].get_transaction_amount() >= largest_transaction.get_transaction_amount():
                # >= amount found then current largest, therefore is not the sole funder
                if passed_liquidity_function:
                    return None, normal_txs, num_remove_liquidities_found

                # if this new transaction is the same amount already, indicate that there is a duplicate
                if normal_txs[index].get_transaction_amount() == largest_transaction.get_transaction_amount():
                    exists_duplicate_amount = True
                else:
                    exists_duplicate_amount = False
                    largest_transaction = normal_txs[index]

    valid_to_return = not exists_duplicate_amount and passed_liquidity_function
    if valid_to_return and largest_transaction:
        valid_to_return = False
        # needs to pass one of these
        if is_out and largest_transaction.to in dataloader.scammers:
            valid_to_return = transaction_collector.ensure_valid_eoa_address(largest_transaction.to, dex)
        elif not is_out and largest_transaction.sender in dataloader.scammers:
            valid_to_return = transaction_collector.ensure_valid_eoa_address(largest_transaction.sender, dex)

    return largest_transaction if valid_to_return else None, normal_txs, num_remove_liquidities_found


def run_chain_on_scammers():
    simple_chain_path = os.path.join(path.panv2_scammer_chain_path, "simple_chain.csv")
    no_chain_path = os.path.join(path.panv2_scammer_chain_path, "no_chain.csv")

    # remove scammers that don't belong in a chain
    scammers_remaining = set(dataloader.scammers)
    with open(no_chain_path, 'r') as file:
        reader = csv.reader(file, quotechar='"', delimiter='|', quoting=csv.QUOTE_ALL)
        next(reader)
        for line in reader:
            scammers_remaining.discard(line[0])

    # remove scammers already processed in the chain
    with open(simple_chain_path, 'r') as file:
        reader = csv.reader(file, quotechar='"', delimiter='|', quoting=csv.QUOTE_ALL)
        next(reader)
        for line in reader:
            scammer_chain = ast.literal_eval(line[1])
            for scammer in scammer_chain:
                scammers_remaining.discard(scammer[0])

    # lower means will write to file more frequently, but lower performance
    # higher means less file writes, but better performance
    save_file_freq = 125
    num_scammers_to_run = 1_000_000
    overall_scammers_written = 0

    # save to file
    while overall_scammers_written <= num_scammers_to_run and len(scammers_remaining) > 0:
        with open(simple_chain_path, "a", newline='') as chain_file, open(no_chain_path, "a", newline='') as no_chain_file:
            chain_writer = csv.writer(chain_file, quotechar='"', delimiter='|', quoting=csv.QUOTE_ALL)
            no_chain_writer = csv.writer(no_chain_file, quotechar='"', delimiter='|', quoting=csv.QUOTE_ALL)
            for _ in range(save_file_freq):
                current_address = scammers_remaining.pop()
                print('Scammers processed={} scammers left={}'.format(overall_scammers_written, len(scammers_remaining)))
                chain = chain_pattern_detection(current_address)
                if len(chain) == 0:
                    no_chain_writer.writerow([current_address])
                else:
                    chain_writer.writerow([len(chain), chain])
                    for scammer_data in chain:
                        scammer_to_remove = scammer_data[0]
                        if scammer_to_remove != current_address:
                            scammers_remaining.remove(scammer_to_remove)
                overall_scammers_written = overall_scammers_written + 1

    print('Finished writing all scammers')


def write_chain_stats_on_data():
    simple_chain_path = os.path.join(path.panv2_scammer_chain_path, "simple_chain.csv")

    all_chains = []

    # read in all the data
    with open(simple_chain_path, 'r') as file:
        reader = csv.reader(file, quotechar='"', delimiter='|', quoting=csv.QUOTE_ALL)
        next(reader)
        for line in reader:
            chain_array = ast.literal_eval(line[1])
            all_chains.append([len(chain_array), chain_array])

    chain_stats_path = os.path.join(path.panv2_scammer_chain_path, "chain_stats.csv")
    chain_stats_headers = ["start_address", "end_address", "chain_length", "num_scams_avg", "trans_amt_avg", "trans_time_diff_avg"]
    with open(chain_stats_path, "w", newline='') as chain_stats_file:
        csv_writer = csv.writer(chain_stats_file, quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL)
        csv_writer.writerow(chain_stats_headers)

        for chain in all_chains:
            scams_performed_values = []
            transfer_amount_values = []
            transfer_time_values = []
            started_address = chain[1][0][0]
            end_address = chain[1][-1][0]
            for scammer_index in range(len(chain[1])):
                # liquidity scams performed stats
                scams_performed_values.append(chain[1][scammer_index][1])

                # transfer amount stats (no data in last scammer)
                if scammer_index < len(chain[1]) - 1:
                    transfer_amount_values.append(chain[1][scammer_index][3])

                # time difference
                if scammer_index < len(chain[1]) - 1:
                    time_difference = chain[1][scammer_index][2]
                    transfer_time_values.append(time_difference)

            transfer_time_mean = max(transfer_time_values) - min(transfer_time_values) if len(transfer_time_values) >= 2 else ""
            csv_writer.writerow([started_address, end_address, chain[0], statistics.mean(scams_performed_values), statistics.mean(transfer_amount_values), transfer_time_mean])


# REFERENCE unused atm
def convert_seconds_to_hms_string(time_difference: int) -> str:
    days = time_difference // 86400
    hours = (time_difference % 86400) // 3600
    minutes = (time_difference % 3600) // 60
    seconds = time_difference % 60

    output = []

    if days > 0:
        output.append(f"{days} days")
    if hours > 0:
        output.append(f"{hours} hours")
    if minutes > 0:
        output.append(f"{minutes} minutes")
    output.append(f"{seconds} seconds")

    return ", ".join(output)


if __name__ == '__main__':
    # run_chain_on_scammers()
    write_chain_stats_on_data()
    # print(*chain_pattern_detection("0x9d143bcbf058553ddd86e13a6ed7c3b38b6c73c1"), sep='\n')
    # print(chain_pattern_detection("0x7edda39fd502cb71aa577452f1cc7e83fda9c5c7"))