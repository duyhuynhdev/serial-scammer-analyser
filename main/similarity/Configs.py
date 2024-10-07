import os
import json
from Crypto.Hash import keccak
from utils.Settings import Setting
from utils.ProjectPath import ProjectPath
from utils import Utils as ut
from utils import Constant

path = ProjectPath()
setting = Setting()

# Tokenization Variables

default_data_dir = "data"
default_ast_dir = os.path.join(default_data_dir, "ast_lowercase")
default_contract_tokenization_file = "tokenized_data_unhashed.json"
default_hashed_contract_tokenization_file = "tokenized_data.json"
default_filter_list_file = "contract_filters.json"
contract_filter_key = "common_contracts"
function_filter_key = "common_functions"


# Similarity and Grouping Variables

default_similarities_file = "similarities.json"
default_groups_file = "contract_groups.csv"
default_minimum_similarity_score = 0.5
default_comparison_similarity = 0.95


# Remote API Variables

default_api_key_location = ".api.key"
default_network = "eth"
blockchain_api_key = ""

# Other Variables

default_creators_file = "token_creation_infos.csv"


def ls_dir(path):
    return os.listdir(path)


def read_list_from_file(file_path):
    list = []
    with open(file_path, 'r') as f:
        for line in f:
            if not line.isspace():
                item = line.strip()
                list.append(item)
        f.close()
    return list


def save_json_file(location, data, indent='\t', sort_keys=True):
    with open(location, "w") as f:
        json.dump(data, f, indent=indent, sort_keys=sort_keys)


def load_json_file(location):
    file_data = {}
    if location is None:
        return file_data
    try:
        with open(location, "r") as f:
            file_data = json.load(f)
    # except FileNotFoundError:
    #     with open(location, "w") as f:
    #         json.dump(file_data, f)
    except (json.decoder.JSONDecodeError, FileNotFoundError):
        with open("Failed JSON Import Files", "a") as f:
            f.write(str(location) + "\n")
    return file_data


def get_value_by_key(d, key):
    if isinstance(d, dict):
        for k, v in d.items():
            if k == key:
                yield v
            yield from list(get_value_by_key(v, key))
    elif isinstance(d, list):
        for o in d:
            yield from list(get_value_by_key(o, key))


def get_node_by_key(d, key):
    if isinstance(d, dict):
        for k, v in d.items():
            if isinstance(v, dict) and key in v.keys():
                yield v
            yield from list(get_node_by_key(v, key))
    elif isinstance(d, list):
        for o in d:
            yield from list(get_node_by_key(o, key))


def get_node_by_key_and_value(d, key, value):
    if isinstance(d, dict):
        for k, v in d.items():
            if k == key and v == value:
                yield d
            yield from list(get_node_by_key_and_value(v, key, value))
    elif isinstance(d, list):
        for o in d:
            yield from list(get_node_by_key_and_value(o, key, value))


def keccak_hash(value):
    hash_func = keccak.new(digest_bits=256)
    hash_func.update(bytes(value, encoding='utf-8'))
    return '0x' + hash_func.hexdigest()


if __name__ == '__main__':
    pass
