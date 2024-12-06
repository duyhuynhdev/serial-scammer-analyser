import math
from Crypto.Hash import keccak
import os
import json
import pandas as pd
import numpy as np
from web3 import Web3

from data_collection.DataDecoder import FunctionInputDecoder
from entity.blockchain.Transaction import NormalTransaction, InternalTransaction
from utils import Constant
from utils.ProjectPath import ProjectPath
from utils.Settings import Setting
from more_itertools import one
from typing import List

setting = Setting()
path = ProjectPath()

class TransactionUtils:

    @staticmethod
    def is_scam_token(parsed_result, dataloader):
        if parsed_result is None:
            return False
        if "token" in parsed_result:
            token = str(parsed_result["token"]).lower()
            is_scam =  token in dataloader.scam_token_pool.keys()
            # print("\tFOUND TOKEN", token, " - is scam? :", is_scam)
            if not is_scam:
                print("\tFOUND TOKEN IS NOT SCAM ", token)
            return is_scam
        if "tokenA" in parsed_result or "tokenB" in parsed_result:
            tokenA = str(parsed_result["tokenA"]).lower()
            tokenB = str(parsed_result["tokenB"]).lower()
            is_scam = tokenA in dataloader.scam_token_pool.keys() or tokenB in dataloader.scam_token_pool.keys()
            # print("\tFOUND TOKEN_A", tokenA, " TOKEN_B", tokenB, " - is scam? :", is_scam)
            if not is_scam:
                print("\tBOTH TOKENS ARE NOT SCAM ", tokenA, tokenB)
            return is_scam
        return False

    @staticmethod
    def is_scam_add_liq(txs: NormalTransaction, dataloader):
        if int(txs.isError) == 1:
            return False
        if "addLiquidity" in str(txs.functionName) or "openTrading" in str(txs.functionName):
            if txs.to.lower() in dataloader.scam_token_pool.keys():
                return True
            function_decoder = FunctionInputDecoder()
            result = function_decoder.decode_add_liq_function_input(txs.input)
            # print("\tDECODED LIQ ADDING DATA", result)
            return TransactionUtils.is_scam_token(result, dataloader)
        return False

    @staticmethod
    def is_scam_remove_liq(txs: NormalTransaction, dataloader):
        if int(txs.isError) == 1:
            return False
        if "removeLiquidity" in str(txs.functionName):
            function_decoder = FunctionInputDecoder()
            result = function_decoder.decode_remove_liq_function_input(txs.input)
            # print("\tDECODED LIQ REMOVING DATA", result)
            if result is None:
                print("\tCANNOT DECODE FUNCTION ", txs.functionName, "(",txs.methodId,")")
            return TransactionUtils.is_scam_token(result, dataloader)
        return False

    @staticmethod
    def get_add_liq_amount(txs: NormalTransaction, normal_txs: [NormalTransaction], dataloader):
        if txs.get_transaction_amount() > 0:
            return txs.get_transaction_amount()
        for transfer_tx in normal_txs:
            if int(transfer_tx.blockNumber) <= int(txs.blockNumber):
                if transfer_tx.is_function_empty() and transfer_tx.get_transaction_amount() > 0 and not transfer_tx.is_to_empty() and transfer_tx.to.lower() in dataloader.scam_token_pool.keys():
                    return transfer_tx.get_transaction_amount()
        return 0

    @staticmethod
    def find_withdraw_txs(txs:NormalTransaction, normal_txs:[NormalTransaction], internals: [InternalTransaction]):
        for withdraw_tx in normal_txs:
            if int(withdraw_tx.blockNumber) >= int(txs.blockNumber):
                if "withdraw" in str(withdraw_tx.functionName).lower() or str(withdraw_tx.methodId) == "0x2e1a7d4d":
                    for itx in internals:
                        if withdraw_tx.hash == itx.hash:
                            return float(itx.value) / 10 ** Constant.WETH_BNB_DECIMALS
        return 0

    @staticmethod
    def get_related_amount_from_internal_txs(txs:NormalTransaction, normal_txs:[NormalTransaction], internals: [InternalTransaction]):
        for itx in internals:
            if txs.hash == itx.hash:
                return float(itx.value) /  10 ** Constant.WETH_BNB_DECIMALS
        # cannot find related internal_txs
        return TransactionUtils.find_withdraw_txs(txs, normal_txs, internals)

# class TransactionUtils:
#
#     @staticmethod
#     def get_related_amount_from_internal_txs(txs: NormalTransaction, internals: [InternalTransaction]):
#         for itx in internals:
#             if txs.hash == itx.hash:
#                 return float(itx.value) / 10 ** Constant.WETH_BNB_DECIMALS
#         return 0
#
#     @staticmethod
#     def is_scam_token(parsed_result, dataloader):
#         if parsed_result is None:
#             return False
#         if "token" in parsed_result:
#             token = str(parsed_result["token"]).lower()
#             return token in dataloader.scam_token_pool.keys()
#         if "tokenA" in parsed_result:
#             token = str(parsed_result["tokenA"]).lower()
#             return token in dataloader.scam_token_pool.keys()
#         if "tokenB" in parsed_result:
#             token = str(parsed_result["tokenB"]).lower()
#             return token in dataloader.scam_token_pool.keys()
#         return False
#
#     @staticmethod
#     def is_scam_add_liq(txs: NormalTransaction, dataloader):
#         if "addLiquidity" in str(txs.functionName):
#             function_decoder = FunctionInputDecoder()
#             result = function_decoder.decode_add_liq_function_input(txs.input)
#             return TransactionUtils.is_scam_token(result, dataloader)
#         return False
#
#     @staticmethod
#     def is_scam_remove_liq(txs: NormalTransaction, dataloader):
#         if "removeLiquidity" in str(txs.functionName):
#             function_decoder = FunctionInputDecoder()
#             result = function_decoder.decode_remove_liq_function_input(txs.input)
#             return TransactionUtils.is_scam_token(result, dataloader)
#         return False
#

class Utils:
    def __init__(self):
        is_contract_address_path = os.path.join(path.panv2_star_shape_path, "is_contract_address.csv")
        if os.path.exists(is_contract_address_path):
            self.contract_address_dict = {}
            self.contract_address_path = is_contract_address_path

            with open(is_contract_address_path, "r") as file:
                for line in file:
                    row = line.rstrip('\n').split(', ')
                    self.contract_address_dict[row[0]] = row[1].lower() == 'true'

            self.contract_address_dict.pop('address')


def keccak_hash(value):
    """
    Hash function
    :param value: original value
    :return: hash of value
    """
    hash_func = keccak.new(digest_bits=256)
    hash_func.update(bytes(value, encoding="utf-8"))
    return "0x" + hash_func.hexdigest()


utils = Utils()


def is_contract_address(address, key_idx=0):
    if address is None or address == "":
        return False
    address_is_contract = utils.contract_address_dict.get(address)
    if address_is_contract:
        return address_is_contract

    key_idx = key_idx % len(setting.INFURA_API_KEYS)
    web3 = Web3(Web3.HTTPProvider(setting.INFURA_ETH_NODE_URL + setting.INFURA_API_KEYS[key_idx]))
    code = web3.eth.get_code(Web3.to_checksum_address(address))

    result = len(code) > 0
    utils.contract_address_dict.update({address: result})
    with open(utils.contract_address_path, "a") as file:
        file.write("{}, {}\n".format(address, result))

    return result


def get_functions_from_ABI(abi, function_type="event"):
    """
    Get function list of contract in ABI
    :param abi: ABI
    :param function_type: type of function we want to get
    :return: extracted functions
    """
    func_dict = {}
    for item in abi:
        if item["type"] == function_type:
            func = item["name"] + "("
            for count, element in enumerate(item["inputs"]):
                if count == 0:
                    func += element["type"]
                else:
                    func += "," + element["type"]
                count += 1
            func += ")"
            func_dict.update({func: keccak_hash(func)})
    return func_dict


def partitioning(from_idx, to_idx, chunk_size):
    """
    Query partitioning because of results limitation (e.g Infura 10K )
    :param from_idx: start idx
    :param to_idx:   end idx
    :param chunk_size: size of chunk
    :return: a list of partition
    """
    num_partitions = math.ceil((to_idx - from_idx) / chunk_size)
    partitions = [
        {"from": from_idx + i * chunk_size, "to": from_idx + (i + 1) * chunk_size - 1}
        for i in range(0, num_partitions)
    ]
    partitions[-1]["to"] = to_idx
    return partitions


def is_df_cell_is_empty(cell):
    return not cell or (isinstance(cell, float) and math.isnan(cell))


def last_index(arr, value):
    return len(arr) - arr[::-1].index(value) - 1


def find_min_max_indexes(arr):
    acc_max = np.maximum.accumulate(arr) - arr
    idx_min = np.argmax(acc_max)
    if idx_min == 0:
        return 0, 0
    idx_max = last_index(arr[:idx_min], max(arr[:idx_min]))

    return idx_min, idx_max


def try_except_assigning(func, failure_value):
    try:
        return func()
    except:
        return failure_value


def write_list_to_file(file_path, list):
    with open(file_path, "w") as f:
        for item in list:
            f.write("%s\n" % item)
        f.close()


def append_item_to_file(file_path, item):
    with open(file_path, "a") as f:
        f.write("%s\n" % item)
        f.close()


def read_list_from_file(file_path):
    list = []
    with open(file_path, "r") as f:
        for line in f:
            if not line.isspace():
                item = line[:-1]
                list.append(item)
        f.close()
    return list


def save_dict_as_csv(dict, key_label, value_label, output_path):
    records = []
    for key, value in dict.items():
        records.append({key_label: key, value_label: value})
    pd.DataFrame.from_records(records).to_csv(output_path, index=False)


def write_json(file_path, data):
    json_object = json.dumps(data, indent=4)
    with open(file_path, "w") as f:
        f.write(json_object)
        f.close()


def read_json(file_path):
    f = open(file_path)
    json_object = json.load(f)
    f.close()
    return json_object


def read_file_to_string(file_path):
    with open(file_path, 'r') as file:
        content = file.read()
        file.close()
    return content


def write_file_from_string(file_path, content):
    with open(file_path, 'w') as file:
        file.write(content)
        file.close()


def save_or_append_if_exist(data, output_path):
    save_df = pd.DataFrame.from_records(data)
    if os.path.isfile(output_path):
        print(data)
        save_df.to_csv(output_path, mode="a", header=False, index=False)
    else:
        # print("SAVE", len(data), "RECORDS")
        save_df.to_csv(output_path, index=False)


def save_overwrite_if_exist(data, output_path):
    save_df = pd.DataFrame.from_records(data)
    # print("SAVE", len(data), "RECORDS")
    save_df.to_csv(output_path, index=False)


def get_abi_function_signatures(abi, type):
    functions = []
    for function in abi:
        if function["type"] == type:
            input_string = ",".join(
                [str(input["type"]) for input in function["inputs"]]
            )
            functions.append(function["name"] + "(" + input_string + ")")
    return functions


def get_abi_function_inputs(abi, type):
    functions = {}
    for function in abi:
        if function["type"] == type:
            input_names = [str(input["name"]) for input in function["inputs"]]
            functions[function["name"]] = input_names
    return functions


def hex_to_dec(hex_val):
    return int(hex_val, 16)


def get_transaction_by_hash(transactions: List[NormalTransaction], expected_hash: str) -> NormalTransaction:
    """Retrieve the transaction with the specified hash from a list of transactions."""
    return one(
        tx for tx in transactions if tx.hash.lower() == expected_hash.lower()
    )
