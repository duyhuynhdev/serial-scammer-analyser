import ast
import csv
import os
import statistics
import sys

import numpy as np

from algorithms.ScammerNetworkBuilder import dataloader
from data_collection.AccountCollector import TransactionCollector
from utils.DataLoader import DataLoader
from utils.ProjectPath import ProjectPath

dataloader = DataLoader()
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

    starter_transaction_history = transaction_collector.get_transactions(starter_address)
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
    if normal_txs is None:
        normal_txs = transaction_collector.get_transactions(scammer_address)

    for transaction in normal_txs:
        if REMOVE_LIQUIDITY_SUBSTRING in str(transaction.functionName) and not transaction.isError:
            num_remove_liquidity_calls += 1

    return num_remove_liquidity_calls


def get_largest_out_after_remove_liquidity(scammer_address: str, normal_txs=None):
    if normal_txs is None:
        normal_txs = transaction_collector.get_transactions(scammer_address)
    return get_largest_transaction(normal_txs, scammer_address, REMOVE_LIQUIDITY_SUBSTRING, True, len(normal_txs) - 1, -1, -1)


def get_largest_in_before_add_liquidity(scammer_address: str, normal_txs=None):
    if normal_txs is None:
        normal_txs = transaction_collector.get_transactions(scammer_address)
    return get_largest_transaction(normal_txs, scammer_address, ADD_LIQUIDITY_SUBSTRING, False, 0, len(normal_txs), 1)


def get_largest_transaction(normal_txs, scammer_address, liquidity_function_name: str, is_out, *range_loop_args):
    num_remove_liquidities_found = 0
    passed_liquidity_function = False
    exists_duplicate_amount = False
    largest_transaction = None
    for index in range(range_loop_args[0], range_loop_args[1], range_loop_args[2]):
        if liquidity_function_name in str(normal_txs[index].functionName) and not normal_txs[index].isError:
            passed_liquidity_function = True
            # count the number of remove liquidity functions
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

    return largest_transaction if not exists_duplicate_amount and passed_liquidity_function else None, normal_txs, num_remove_liquidities_found


def run_chain_on_scammers():
    simple_chain_path = os.path.join(path.univ2_scammer_chain_path, "simple_chain.csv")
    no_chain_path = os.path.join(path.univ2_scammer_chain_path, "no_chain.csv")

    # remove scammers that don't belong in a chain
    scammers_remaining = set(dataloader.scammers)
    with open(no_chain_path, 'r') as file:
        reader = csv.reader(file, quotechar='"', delimiter='|', quoting=csv.QUOTE_ALL)
        next(reader)
        for line in reader:
            scammers_remaining.remove(line[0])

    # remove scammers already processed in the chain
    with open(simple_chain_path, 'r') as file:
        reader = csv.reader(file, quotechar='"', delimiter='|', quoting=csv.QUOTE_ALL)
        next(reader)
        for line in reader:
            scammer_chain = ast.literal_eval(line[1])
            for scammer in scammer_chain:
                scammers_remaining.remove(scammer[0])

    # lower means will write to file more frequently, but lower performance
    # higher means less file writes, but better performance
    save_file_freq = 5000
    num_scammers_to_run = 150000
    overall_scammers_written = 0

    # save to file
    while overall_scammers_written <= num_scammers_to_run or len(scammers_remaining) > 0:
        with open(simple_chain_path, "a", newline='') as chain_file, open(no_chain_path, "a", newline='') as no_chain_file:
            chain_writer = csv.writer(chain_file, quotechar='"', delimiter='|', quoting=csv.QUOTE_ALL)
            no_chain_writer = csv.writer(no_chain_file, quotechar='"', delimiter='|', quoting=csv.QUOTE_ALL)
            print('Scammers processed={} scammers left={}'.format(overall_scammers_written, len(scammers_remaining)))
            for _ in range(save_file_freq):
                current_address = scammers_remaining.pop()
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


def write_chain_stats_on_data():
    chain_stats_path = os.path.join(path.univ2_scammer_chain_path, "chain_stats.txt")
    simple_chain_path = os.path.join(path.univ2_scammer_chain_path, "simple_chain.csv")

    all_chains = []

    # read in all the data
    with open(simple_chain_path, 'r') as file:
        reader = csv.reader(file, quotechar='"', delimiter='|', quoting=csv.QUOTE_ALL)
        next(reader)
        for line in reader:
            all_chains.append([int(line[0]), ast.literal_eval(line[1])])

    chain_stats = {"min": sys.maxsize, "max": -1, "average": 0, "median": 0, "num_chains": 0}
    scams_performed_stats = {"min": sys.maxsize, "max": -1, "average": 0, "median": 0, }

    chain_median = []
    scams_performed_median = []
    for chain in all_chains:
        # chain stats
        chain_stats['average'] += chain[0]
        chain_median.append(chain[0])
        if chain[0] > chain_stats['max']:
            chain_stats['max'] = chain[0]
        if chain[0] < chain_stats['min']:
            chain_stats['min'] = chain[0]

        for scammer in chain[1]:
            # liquidity scam stats
            scams_performed_stats['average'] += scammer[1]
            scams_performed_median.append(scammer[1])
            if scammer[1] > scams_performed_stats['max']:
                scams_performed_stats['max'] = scammer[1]
            if scammer[1] < scams_performed_stats['min']:
                scams_performed_stats['min'] = scammer[1]

    chain_stats['average'] = chain_stats['average'] / len(all_chains)
    chain_stats['median'] = statistics.median(chain_median)
    chain_stats['num_chains'] = len(all_chains)

    scams_performed_stats['average'] = scams_performed_stats['average'] / len(scams_performed_median)
    scams_performed_stats['median'] = statistics.median(scams_performed_median)
    # TOD use stasticis.mean, much easier that way
    print(chain_stats)
    print(scams_performed_stats)


if __name__ == '__main__':
    write_chain_stats_on_data()
    # run_chain_on_scammers()
    # print(*chain_pattern_detection("0x48f0fc8dfc672dd45e53b6c53cd5b09c71d9fbd6"), sep='\n')
    # print(chain_pattern_detection("0x7edda39fd502cb71aa577452f1cc7e83fda9c5c7"))
