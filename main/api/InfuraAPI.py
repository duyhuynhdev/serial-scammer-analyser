import math

import requests
from web3.datastructures import AttributeDict
from web3 import Web3
from hexbytes import HexBytes
from utils.Settings import Setting
import pandas as pd
import os
from utils import Utils as ut

setting = Setting()

'''
References:  https://docs.infura.io/infura/networks/ethereum/
'''


def rpc(method, list_params):
    """
    Infura JSON RPC calling
    :param method: JSON-RPC method name (see: https://docs.infura.io/infura/networks/ethereum/json-rpc-methods)
    :param list_params: list of method's params
    :return:
    """
    base_url = setting.INFURA_ETH_NODE_URL + setting.INFURA_API_MAIN_KEY
    data = [{"jsonrpc": "2.0", "method": method, "params": params, "id": 1} for params in list_params]
    response = requests.post(base_url, headers={"Content-Type": "application/json"}, json=data)
    return response.json()


def rpc_single_call(method, params):
    """
    Infura JSON RPC calling
    :param method: JSON-RPC method name (see: https://docs.infura.io/infura/networks/ethereum/json-rpc-methods)
    :param params: method's params
    :return:
    """
    base_url = setting.INFURA_ETH_NODE_URL + setting.INFURA_API_MAIN_KEY
    data = {"jsonrpc": "2.0", "method": method, "params": params, "id": 1}
    response = requests.post(base_url, headers={"Content-Type": "application/json"}, json=data)
    return response.json()


def infura_query_partitioning(from_block, to_block, num_partitions):
    """
    Query partitioning because of Infura 10K results limitation
    :param from_block: start block
    :param to_block:   end block
    :param num_partitions: number of partitions
    :return: a list of partition
    """
    partition_size = math.ceil((to_block - from_block) / num_partitions)
    assert partition_size >= 1, "Number of block is not enough for partitioning!!!"
    partitions = [{"fromBlock": from_block + i * partition_size, "toBlock": from_block + (i + 1) * partition_size - 1} for i in range(0, num_partitions)]
    return partitions


def hex_to_dec(hex_val):
    return int(hex_val, 16)


def get_event_args(contract, event_name, log):
    parsed_log = log.copy()
    parsed_log['blockHash'] = HexBytes(parsed_log['blockHash'])
    parsed_log['blockNumber'] = hex_to_dec(parsed_log['blockNumber'])
    parsed_log['logIndex'] = hex_to_dec(parsed_log['logIndex'])
    for i in range(len(parsed_log['topics'])):
        parsed_log['topics'][i] = HexBytes(parsed_log['topics'][i])
    parsed_log['transactionHash'] = HexBytes(parsed_log['transactionHash'])
    parsed_log['transactionIndex'] = hex_to_dec(parsed_log['transactionIndex'])
    receipt = AttributeDict({'logs': [AttributeDict(parsed_log)]})
    args = eval('contract.events.{}().process_receipt({})'.format(event_name, receipt))[0]
    return args


def check_event_exist(contract, event_name, event_hash, input_names, from_block, to_block, num_partitions=5):
    partitions = infura_query_partitioning(from_block, to_block, num_partitions)
    print("Check logs from block ", from_block, " to block ", to_block, "(", num_partitions, " partitions )")
    list_params = [[{"address": contract.address,
                     "fromBlock": hex(partitions[i]["fromBlock"]),
                     "toBlock": hex(partitions[i]["toBlock"]),
                     "topics": [event_hash]}] for i in range(0, len(partitions))]
    logs_list = rpc("eth_getLogs", list_params)
    for index, logs in enumerate(logs_list):
        p_from_block = hex_to_dec(list_params[index][0]["fromBlock"])
        p_to_block = hex_to_dec(list_params[index][0]["toBlock"])
        print("PROCESS logs from ", p_from_block, "to", p_to_block)
        if list(logs.keys())[-1] == "result" and logs["result"] is not None:
            events = []
            for log in logs['result']:
                parsed_log = get_event_args(contract, event_name, log)
                event_info = {'transaction_hash': parsed_log.transactionHash.hex(),
                              'block_number': parsed_log.blockNumber}
                arg_dict = dict(parsed_log.args)
                for input in input_names:
                    event_info[input] = arg_dict[input]
                events.append(event_info)
            print(events)
            print("GOT ", len(events), " LOGS FROM BLOCK ", p_from_block, " TO BLOCK ", p_to_block)
            if len(events) > 0:
                return True
        else:  # over 10k items -> continue partition and get logs
            if p_to_block == p_from_block:
                print("ERROR still occurs in ", p_to_block, " - address:", contract.address)
                raise Exception("ERROR still occurs within one block ")
            if p_to_block - p_from_block <= num_partitions:
                num_partitions = (p_to_block - p_from_block) + 1
            check_result = check_event_exist(contract, event_name, event_hash, input_names, p_from_block, p_to_block, num_partitions)
            if check_result:
                return True
    return False


def eth_getLogs(contract, event_name, event_hash, input_names, from_block, to_block, output_path, num_partitions=10):
    partitions = infura_query_partitioning(from_block, to_block, num_partitions)
    print("Getting logs from block ", from_block, " to block ", to_block, "(", num_partitions, " partitions )")
    list_params = [[{"address": contract.address,
                     "fromBlock": hex(partitions[i]["fromBlock"]),
                     "toBlock": hex(partitions[i]["toBlock"]),
                     "topics": [event_hash]}] for i in range(0, len(partitions))]
    logs_list = rpc("eth_getLogs", list_params)
    count = 0
    for index, logs in enumerate(logs_list):
        p_from_block = hex_to_dec(list_params[index][0]["fromBlock"])
        p_to_block = hex_to_dec(list_params[index][0]["toBlock"])
        if list(logs.keys())[-1] == "result" and logs["result"] is not None:  # check if result exist
            events = []
            # print(logs)
            print("PROCESS logs from ", p_from_block, "to", p_to_block)
            for log in logs['result']:
                parsed_log = get_event_args(contract, event_name, log)
                event_info = {'transaction_hash': parsed_log.transactionHash.hex(),
                              'block_number': parsed_log.blockNumber}
                arg_dict = dict(parsed_log.args)
                for input in input_names[event_name]:
                    event_info[input] = arg_dict[input]
                events.append(event_info)
            print("GOT ", len(events), " LOGS FROM BLOCK ", p_from_block, " TO BLOCK ", p_to_block)
            count += len(events)
            if len(events) > 0:
                df = pd.DataFrame.from_records(events)
                if os.path.isfile(output_path):
                    df.to_csv(output_path, mode='a', header=False, index=False)
                else:
                    df.to_csv(output_path, index=False)
                print("SAVED ", len(events), " LOGS FROM BLOCK ", p_from_block, " TO BLOCK ", p_to_block, " OF ", contract.address)
        else:  # over 10k items -> continue partition and get logs
            if p_to_block == p_from_block:
                print("ERROR still occurs in ", p_to_block, " - address:", contract.address)
                raise Exception("ERROR still occurs within one block ")
            print("Over 10K items -> continue partitioning and getting logs")
            if p_to_block - p_from_block <= num_partitions:
                num_partitions = (p_to_block - p_from_block) + 1
            count += eth_getLogs(contract, event_name, event_hash, input_names, p_from_block, p_to_block, output_path, num_partitions)
    return count


def eth_getCode(address):
    response = rpc_single_call("eth_getCode", [address, "latest"])
    return response


def eth_getTxByHash(hash):
    response = rpc_single_call("eth_getTransactionByHash", [hash])
    return response


def eth_getBlockByNumber(block):
    response = rpc_single_call("eth_getBlockByNumber", [hex(block), False])
    return response


if __name__ == '__main__':
    print(eth_getBlockByNumber(9999)["result"])
