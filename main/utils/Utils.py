import math
from Crypto.Hash import keccak
import os
import json
import re
import pandas as pd
import numpy as np
from utils.Path import Path
from utils.Settings import Setting
setting = Setting()
path = Path()

def keccak_hash(value):
    """
    Hash function
    :param value: original value
    :return: hash of value
    """
    hash_func = keccak.new(digest_bits=256)
    hash_func.update(bytes(value, encoding='utf-8'))
    return '0x' + hash_func.hexdigest()


def get_functions_from_ABI(abi, function_type='event'):
    """
    Get function list of contract in ABI
    :param abi: ABI
    :param function_type: type of function we want to get
    :return: extracted functions
    """
    func_dict = {}
    for item in abi:
        if item['type'] == function_type:
            func = item['name'] + '('
            for count, element in enumerate(item['inputs']):
                if count == 0:
                    func += element['type']
                else:
                    func += ',' + element['type']
                count += 1
            func += ')'
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
    partitions = [{"from": from_idx + i * chunk_size, "to": from_idx + (i + 1) * chunk_size - 1} for i in range(0, num_partitions)]
    return partitions


def write_list_to_file(file_path, list):
    with open(file_path, 'w') as f:
        for item in list:
            f.write("%s\n" % item)
        f.close()


def append_item_to_file(file_path, item):
    with open(file_path, 'a') as f:
        f.write("%s\n" % item)
        f.close()


def read_list_from_file(file_path):
    list = []
    with open(file_path, 'r') as f:
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


def get_abi_function_signatures(abi, type):
    functions = []
    for function in abi:
        if function["type"] == type:
            input_string = ",".join([str(input["type"]) for input in function['inputs']])
            functions.append(function["name"] + "(" + input_string + ")")
    return functions


def get_abi_function_inputs(abi, type):
    functions = {}
    for function in abi:
        if function["type"] == type:
            input_names = [str(input["name"]) for input in function['inputs']]
            functions[function["name"]] = input_names
    return functions


def count_uppercase(words):
    count = 0
    for w in words:
        if w.isupper():
            count += 1
    return count


def smart_split_words(str):
    results = []
    words = str.split()
    for word in words:
        # case ABC, abc or Abc => no split more
        if len(word) == count_uppercase(word) or count_uppercase(word) < 2:
            results.append(word)
        else:
            # case AbcDef => Abc, Def
            results.extend(re.findall('[A-Z][^A-Z]*', word))
    return results


def last_index(list, value):
    return len(list) - list[::-1].index(value) - 1


def find_min_max_indexes(list):
    distances = np.maximum.accumulate(list) - list
    idx_min = np.argmax(distances)
    if idx_min == 0:
        return 0, 0
    idx_max = last_index(list[:idx_min], max(list[:idx_min]))

    return idx_min, idx_max

def contract_code_line_numbering(token_name, start_number=1):
    path = Path()
    input_file = os.path.join(path.example_tokens_path, token_name + ".sol")
    output_file = os.path.join(path.example_tokens_after_numbering_path, token_name + ".txt")

    with open(input_file, 'r') as f_in:
        lines = f_in.readlines()

    with open(output_file, 'w') as f_out:
        for i, line in enumerate(lines, start=start_number):
            f_out.write(f"{i}: {line}")

    print(f"Line numbers added successfully. Output written to {output_file}")


def hex_to_dec(hex_val):
    return int(hex_val, 16)


def get_origin_address(address: str):
    pth = os.path.join(path.univ2_base_path, "address_sensitive_case_mapping.csv")
    if os.path.exists(pth):
        addresses = pd.read_csv(pth)
        return addresses[addresses["lower"] == address.lower()]["origin"].values[0]
    else:
        return address


if __name__ == '__main__':
    # ROOT_FOLDER = os.path.dirname(os.path.dirname(__file__))
    # ETH_TOKEN_ABI = json.load(open(ROOT_FOLDER + "/abi/eth_token_abi.json"))
    # UNIV2_FACTORY_ABI = json.load(open(ROOT_FOLDER + "/abi/uniswap_v2_factory_abi.json"))
    # UNIV2_POOL_ABI = json.load(open(ROOT_FOLDER + "/abi/uniswap_v2_pool_abi.json"))
    # print(get_abi_function_signatures(UNIV2_POOL_ABI, "event"))
    # print(get_abi_function_signatures(ETH_TOKEN_ABI, "event"))
    # print(smart_split_words("abc"))
    # print(smart_split_words("Abc"))
    # print(smart_split_words("ABC"))
    # print(smart_split_words("AbcDef"))
    # print(smart_split_words("abc ABC Def ZZab ZxyBceDgh"))
    # print(find_min_max_indexes([5, 7, 5, 2, 5]))
    tokens = ["AquaDrop", "CPP4U", "DollarMillionaire", "LGT", "Sepuku", "SquidX"]
    for token_name in tokens:
        contract_code_line_numbering(token_name)
