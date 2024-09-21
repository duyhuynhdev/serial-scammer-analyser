import math
from Crypto.Hash import keccak
import os
import json
import re
import pandas as pd
import numpy as np
from web3 import Web3
from utils.Settings import Setting
from pathlib import Path

from boto3.s3.transfer import TransferConfig

setting = Setting()
path = Path()


def keccak_hash(value):
    """
    Hash function
    :param value: original value
    :return: hash of value
    """
    hash_func = keccak.new(digest_bits=256)
    hash_func.update(bytes(value, encoding="utf-8"))
    return "0x" + hash_func.hexdigest()


def is_contract_address(address):
    if address is None or address == "":
        return False
    code = setting.infura_web3.eth.get_code(Web3.to_checksum_address(address))
    return len(code) > 0


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


def save_or_append_if_exist(data, output_path):
    save_df = pd.DataFrame.from_records(data)
    if os.path.isfile(output_path):
        # print("APPEND ", len(data), "RECORDS")
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
