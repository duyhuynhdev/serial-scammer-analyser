import argparse
from pprint import pprint
import sys
import os
import pandas as pd
import solcx
sys.path.append(os.path.join(os.path.dirname(sys.path[0])))
import utils.Utils
from main.similarity.Configs import *
import re
from tqdm import tqdm
from main.similarity.ContractRemoteUltils import get_ast_from_remote
from main.similarity.SimilaritiesFromTokens import compare_similarities, compare_similarities_between_sets

# Check dir for AST files.
# Load file containing previous tokenized contracts.
# If file not in loaded data: (Force optional)
# - Load AST for contract.
# - Tokenize contract AST.
# - Update list and save when complete.
# Load file containing previous tokenized contract similarities.
# If tokenized contract loaded data does not have entry for similarities: (Force optional)
# - Calculate jaccard similarity for tokenized contract and each other contract in list.
# - Update entries in both directions (contract a -> b, contract b -> a).
# - Save similarities.

# Tokenization flow.

# Accept AST file for contract to be tokenized.
# Load file containing filter criteria. (Optional)
# Extract all contract definitions from AST file.
# For each contract definition in AST file:
# - If contract name appears in filter list, skip this node. (name or canonicalName fields)
# - For each node within the contract definition:
# - - If nodeType is FunctionDefinition and name field is in filter list for functions, skip this node.
# - - If nodeType is EventDefinition or UsingForDirective, skip this node.
# TODO
# - - For function definitions, obtain data for tokenization using fields x,y,z.
# - - For variable declarations, obtain data for tokenization using fields x,y,z.
# - - If any data fields contain "(u?int[0-9]{0-3})" rename to "number".
# - - Sort data fields so order of appearance in node (parameter list) is not relevant.
# - - Convert data to string to be tokenized.

# TODO - Compare wrapped.
# WBTC - https://etherscan.io/token/0x2260fac5e5542a773aa44fbcfedf7c193bc2c599#code
# WBNB - https://etherscan.io/token/0xB8c77482e45F1F44dE1745F52C74426C631bDD52#code
# Link - https://etherscan.io/token/0x514910771af9ca656af840dff83e8264ecf986ca#code
# TODO Ext. Similarity Analysis
# - - Implement threshold value, storing only similarities exceeding the threshold.
# - - Generate frequency analysis histogram for each similarity percentage across contracts,
# - - gather averages and similarity data.

parser = argparse.ArgumentParser(description="Analyse Smart Contracts for Similarities")
parser.add_argument("-n", "--network", default="eth", help="The network to connect to, default: eth")
parser.add_argument("-a", "--addresses", default=[], help="CSV list of contract addresses to check similarities")

# TODO: Improve filtering, filter for contract names in type defs - get node by key, sub nodes in functs etc
sanitize_regex_string = "|".join([
    "(_\$[0-9]+)",
    "(_[0-9a-f]{5,})",
    "(_by_[0-9]+)",
    "(_\$bound_to)",
    "(_memory)",
    "(_storage)",
    "(_calldata)",
    "(_pure)",
    "(_view)",
    "(_ptr)",
    "(_internal)",
    "(_external)",
    "(_public)"
])
sanitize_regex = re.compile(sanitize_regex_string)
rename_regex = re.compile("(t_u?((int)|(bytes)|(rational))[0-9]{0,3})")

testing_dictionary = {}  # TODO: Remove - testing


def compare_address_similarities_between_sets(address_list, address_list2, min_required_similarity=default_minimum_similarity_score):
    contract_tokens = tokenize_contracts(address_list, force=False)
    save_json_file(default_contract_tokenization_file, contract_tokens)

    contract_tokens = hash_tokenized_contracts_from_file(force=False)
    save_json_file(default_hashed_contract_tokenization_file, contract_tokens)

    address_list_tokens = {}

    for address in address_list:
        address_list_tokens[address] = contract_tokens[address]

    contract_tokens2 = tokenize_contracts(address_list2, force=False)
    save_json_file(default_contract_tokenization_file, contract_tokens2)

    contract_tokens2 = hash_tokenized_contracts_from_file(force=False)
    save_json_file(default_hashed_contract_tokenization_file, contract_tokens2)

    address_list_tokens2 = {}
    for address in address_list2:
        address_list_tokens2[address] = contract_tokens2[address]
    similarities = compare_similarities_between_sets(address_list_tokens, address_list_tokens2,
                                                     min_required_similarity=min_required_similarity)
    # save_json_file(default_similarities_file, similarities, sort_keys=False)
    return similarities


def compare_address_similarities(address_list, min_required_similarity=default_minimum_similarity_score):
    contract_tokens = tokenize_contracts(address_list, force=False)
    save_json_file(default_contract_tokenization_file, contract_tokens)

    contract_tokens = hash_tokenized_contracts_from_file(force=False)
    save_json_file(default_hashed_contract_tokenization_file, contract_tokens)

    address_list_tokens = {}
    for address in address_list:
        address_list_tokens[address] = contract_tokens[address]

    similarities = compare_similarities(address_list_tokens, min_required_similarity=min_required_similarity)
    # save_json_file(default_similarities_file, similarities, sort_keys=False)
    return similarities


def run_tokenization_test():
    # address_list = get_list_of_contract_addresses_from_ast_dir()
    address_list = load_json_file("trapdoor_list.txt")["addresses"]

    # contract_tokens = tokenize_contracts(address_list, force=True)  # TODO: Force set True for testing
    contract_tokens = tokenize_contracts(address_list, force=False)
    save_json_file(default_contract_tokenization_file, contract_tokens)

    contract_tokens = hash_tokenized_contracts_from_file(force=True)
    save_json_file(default_hashed_contract_tokenization_file, contract_tokens)

    similarities = compare_similarities(contract_tokens, min_required_similarity=default_minimum_similarity_score)
    save_json_file(default_similarities_file, similarities, sort_keys=False)


def read_filter_list(filter_list_file=default_filter_list_file):
    if filter_list_file is None:
        return {}
    return load_json_file(filter_list_file)


def load_ast(address, ast_location=default_ast_dir):
    file = address + ".json"
    toke_ast = {}
    try:
        with open(os.path.join(ast_location, file), 'r') as f:
            toke_ast = json.load(f)
            f.close()
    except (json.decoder.JSONDecodeError, FileNotFoundError):
        with open("Failed JSON Import Files", "a") as f:
            f.write(str(address) + "\n")
    if toke_ast == {}:
        toke_ast = get_ast_from_remote(address, api_key=blockchain_api_key)
    return toke_ast


def get_list_of_contract_addresses_from_ast_dir(ast_dir=default_ast_dir):
    file_list = ls_dir(ast_dir)
    address_list = []
    while len(file_list) > 0:
        this_file = file_list.pop()
        if len(this_file) > 5 and this_file[-5:] == ".json":
            address_list.append(this_file[:-5])
    return address_list


def get_all_contracts_from_ast(ast):
    key = "nodeType"
    value = "ContractDefinition"
    contracts = list(get_node_by_key_and_value(ast, key, value))
    return contracts


def tokenize_contracts(address_list, location=default_ast_dir, raw_token_file=default_contract_tokenization_file,
                       filter_list_file=default_filter_list_file, force=False):
    tokens_by_contract = load_json_file(raw_token_file)
    filter_list = read_filter_list(filter_list_file)
    for address in tqdm(address_list, "Tokenizing Contract Addresses"):
        if not force and address in tokens_by_contract:
            continue
        tokens_by_contract[address] = tokenize_contract(address, ast_location=location, filter_list=filter_list)

    return tokens_by_contract
filter_list = {
  "common_contracts": {
    "safemath": 0,
    "ierc20": 0,
    "ibep20": 0,
    "context": 0,
    "address": 0,
    "ownable": 0,
    "ownableupgradeable": 0,
    "iuniswapv2factory": 0,
    "iuniswapv2router02": 0,
    "iuniswapv2router01": 0,
    "iuniswapv2router": 0,
    "iuniswapv2pair": 0,
    "ierc20metadata": 0,
    "ibep20metadata": 0,
    "uniswapexchange": 0,
    "owned": 0,
    "safemathint": 0,
    "safemathuint": 0,
    "ierc20interface": 0,
    "ibep20interface": 0,
    "idexrouter": 0,
    "idexfactory": 0,
    "reentrancyguard": 0,
    "pausable": 0,
    "idexpair": 0,
    "ipancakefactory": 0,
    "ipancakepair": 0,
    "ipancakerouter01": 0,
    "ipancakerouter02": 0,
    "enumerableset": 0,
    "math": 0,
    "arrays": 0,
    "counters": 0,
    "iweth": 0,
    "iwbnb": 0,
    "ierc165": 0,
    "upgradeableproxy": 0,
    "erc1967proxy": 0,
    "erc1967upgrade": 0,
    "transparentupgradeableproxy": 0,
    "proxyadmin": 0,
    "beaconproxy": 0,
    "ibeacon": 0,
    "upgradeablebeacon": 0,
    "initializable": 0,
    "uupsupgradeable": 0,
    "console": 0,
    "strings": 0
  },
  "common_functions": {
    "name": 0,
    "symbol": 0,
    "decimals": 0,
    "totalsupply": 0,
    "allowance": 0,
    "approve": 0,
    "_name": 1213,
	"_symbol": 1210,
	"_approve": 1208,
	"_msgsender": 1202,
	"_decimals": 1077,
	"_owner": 1067,
    "increaseallowance": 936,
	"decreaseallowance": 936,
    "owner": 770,
    "approval": 0,
    "ownershiptransferred": 0
  }
}
def tokenize_ast(contract_ast):
    # filter_list = utils.Utils.read_json("contract_filters.json")
    contracts = get_all_contracts_from_ast(contract_ast)

    tokens = []

    for contract in contracts:
        # If contract name is in filter, skip.
        if ("name" in contract and contract["name"].lower() in filter_list[contract_filter_key]) or (
                "canonicalName" in contract and contract["canonicalName"].lower in filter_list[contract_filter_key]):
            continue

        for node in contract["nodes"]:
            # If node is event or 'using for directive', skip.
            if node["nodeType"].lower() == "eventdefinition" or node["nodeType"].lower() == "usingfordirective":
                continue

            # If node is function and function is in filter, skip.
            if (node["nodeType"].lower() == "functiondefinition" and
                    "name" in node and node["name"].lower() in filter_list[function_filter_key]):
                continue

            tokens.append(tokenize_node(node))
    return tokens

def tokenize_contract(address, ast_location=default_ast_dir, filter_list=None):
    if filter_list is None or filter_list == {}:
        filter_list = {
            contract_filter_key: {},
            function_filter_key: {}
        }
    contract_ast = load_ast(address, ast_location)
    contracts = get_all_contracts_from_ast(contract_ast)

    tokens = []

    for contract in contracts:

        # If contract name is in filter, skip.
        if ("name" in contract and contract["name"].lower() in filter_list[contract_filter_key]) or (
                "canonicalName" in contract and contract["canonicalName"].lower in filter_list[contract_filter_key]):
            continue

        for node in contract["nodes"]:
            # If node is event or 'using for directive', skip.
            if node["nodeType"].lower() == "eventdefinition" or node["nodeType"].lower() == "usingfordirective":
                continue

            # If node is function and function is in filter, skip.
            if (node["nodeType"].lower() == "functiondefinition" and
                    "name" in node and node["name"].lower() in filter_list[function_filter_key]):
                continue

            tokens.append(tokenize_node(node))
    return tokens


def tokenize_node(node):
    # TODO: Testing start
    # if node["nodeType"] not in testing_dictionary:
    #     testing_dictionary[node["nodeType"]] = []
    # testing_dictionary[node["nodeType"]].append(node)
    # TODO: Testing end

    if node["nodeType"] in contract_nodes_to_tokenize:
        return contract_nodes_to_tokenize[node["nodeType"]](node)

    return tokenize_node_original(node)


def rename_uints(input_string):
    return re.sub(rename_regex, 't_number', input_string)


def sanitize_types(input_string):
    return re.sub(sanitize_regex, "", input_string)


def default_tokenize_node(node):
    return tokenize_catchall(node)


def tokenize_catchall(node):
    type_identifier = []
    # type_identifier = str(list(get_value_by_key(node, "typeIdentifier")))
    type_identifier_raw = list(get_value_by_key(node, "typeIdentifier"))
    for item in type_identifier_raw:
        if item is not None:
            type_identifier.append(item)
    try:

        type_identifier.sort()
    except:
        print(type_identifier)
        print(node)
        exit()
    return sanitize_types(rename_uints("".join(type_identifier)))


def tokenize_node_original(node):
    return "".join(list(get_value_by_key(node, "nodeType")))
    # return [node["nodeType"]]


def tokenize_variable(node):
    type_identifier = node["typeDescriptions"]["typeIdentifier"]
    # type_identifier = str(list(get_value_by_key(node["typeDescriptions"], "typeIdentifier")))  # TODO: Testing
    return sanitize_types(rename_uints(type_identifier))


def tokenize_modifier(conde):
    pass


def tokenize_struct(conde):
    pass


def tokenize_user_defined(conde):
    pass


def tokenize_function(conde):
    pass


def hash_tokenized_contracts_from_file(tokenized_contract_file=default_contract_tokenization_file,
                                       hashed_contract_file=default_hashed_contract_tokenization_file,
                                       force=False):
    contracts = load_json_file(tokenized_contract_file)
    hashed_tokenized_contracts = load_json_file(hashed_contract_file)
    for address in tqdm(contracts.keys(), "Tokenizing Contract Addresses"):
        if not force and address in hashed_tokenized_contracts:
            continue
        tokens = []
        for element in contracts[address]:
            tokens.append(keccak_hash("".join(element)))
        hashed_tokenized_contracts[address] = tokens

    return hashed_tokenized_contracts


def save_test_data():
    save_json_file("Testing Dictionary.json", testing_dictionary)


def print_test_data_keys():
    print(list(testing_dictionary.keys()))


def print_test_data_adv():
    for key in testing_dictionary.keys():
        for item in testing_dictionary[key]:
            print(str(key) + "\t" + str(list(item.keys())))


def load_test_data():
    return load_json_file("Testing Dictionary.json")


# TODO: Update dictionary to reflect implemented functions.
contract_nodes_to_tokenize = {
    "EnumDefinition": default_tokenize_node,
    'ErrorDefinition': default_tokenize_node,
    'EventDefinition': default_tokenize_node,
    'FunctionDefinition': default_tokenize_node,
    'ModifierDefinition': default_tokenize_node,
    'StructDefinition': default_tokenize_node,
    'UserDefinedValueTypeDefinition': default_tokenize_node,
    'UsingForDirective': default_tokenize_node,
    'VariableDeclaration': tokenize_variable
}

if __name__ == '__main__':
    args = vars(parser.parse_args())
    # pass
    # run_tokenization_test()
    # TODO: Testing function start
    addresses_to_compare = str(args["addresses"]).split(",")
    compare_address_similarities(addresses_to_compare, min_required_similarity=0)
    # save_test_data()
    # testing_dictionary = load_test_data()

    # print_test_data_keys()
    # print_test_data_adv()

    # TODO: Testing function end
