import asyncio
from main.similarity.Configs import *
from aioetherscan import Client
import solcx


# Requires Solc and dependencies to be installed.
# See: https://docs.soliditylang.org/en/latest/installing-solidity.html#
#
# - Linux:
#     sudo add-apt-repository ppa:ethereum/ethereum
#     sudo apt-get update
#     sudo apt-get install solc


def load_api_key(api_key_location=default_api_key_location):
    try:
        with open(api_key_location, "r") as api_file:
            api = api_file.readline().strip()
    except FileNotFoundError:
        print("API Key File not found at given location: ", api_key_location)
        quit()
    except Exception as e:
        print(e)
        quit()
    return api


def get_ast_from_remote(address_to_check, api_key=None, api_key_location=default_api_key_location):
    if api_key is None or api_key == "":
        api_key = load_api_key(api_key_location)

    remote_contract_data = retrieve_block_explorer_data(address_to_check=address_to_check,
                                                        api_key=api_key)
    remote_ast = generate_ast_from_string(remote_contract_data["source_code"],
                                          compiler_version=str(remote_contract_data["compiler_version"]))
    return remote_ast


def generate_ast_from_string(contract_string, compiler_version=None):
    if compiler_version is not None:
        compiler_version = compiler_version.split("+")[0]
        print("compiler version: ", compiler_version)
        solcx.install_solc(compiler_version, show_progress=True)
    # compiled_data = solcx.compile_source(contract_string, solc_version=compiler_version, output_values=["ast"])
    compiled_data = solcx.compile_source(contract_string, solc_version=compiler_version, base_path=None,
                                         allow_paths=None, allow_empty=True)
    # return compiled_data
    ast = ""
    for contract_entry in compiled_data.values():
        # ast.append({"ast": solcast.from_ast(contract_entry["ast"])})
        if len(str(contract_entry["ast"])) > len(str(ast)):
            ast = contract_entry["ast"]
        # ast.append({"ast": contract_entry["ast"]})
        # print(len(str(contract_entry["ast"])))  # Simplify to single ast based on dict size.
    return [{"ast": ast}]


def retrieve_block_explorer_data(address_to_check=None, api_key=None, blockchain_network=default_network):
    return asyncio.run(retrieve_block_explorer_data_async(address_to_check, api_key, blockchain_network))


async def retrieve_block_explorer_data_async(address_to_check=None, api_key=None, blockchain_network=default_network):
    if address_to_check is None or api_key is None:
        return None

    # TODO : Update response dict for return
    response_dict = {
        "is_proxy": False,
        "is_verified": True,
        "contract_address": "",
        "contract_creator": "",
        "contract_name": "",
        "source_code": None,
        # "token_name": "",
        # "contract_symbol": "",
        # "contract_decimals": 0,
        "abi": None,
        "contract_creation_tx_hash": "",
        "deployed_bytecode": "",
        "contract_creation_code": "",
        "constructor_arguments": "",
        "contract_opcode": "",
        # "functions": [],
        # "events": [],
        # "abi_misc": [],
        # "variables": [],
        "compiler_version": 0,
        "optimization_used": 0,
        "license_type": "",
        "proxy_implementation_data": {},
    }
    # response = None
    # Client(api_key=api_key, api_kind=args["network"], network="main",
    #        throttler=throttler, retry_options=retry_options)
    blockscan = Client(api_key=api_key, api_kind=blockchain_network, network="main")
    try:
        # 'SourceCode', 'ABI', 'ContractName', 'CompilerVersion', 'OptimizationUsed', 'Runs', 'ConstructorArguments',
        # 'EVMVersion', 'Library', 'LicenseType', 'Proxy', 'Implementation', 'SwarmSource'
        response = await blockscan.contract.contract_source_code(address_to_check)

        if response[0]["ABI"] != "Contract source code not verified":

            # for resp_keys in response[0].keys():
            #     print(type(response[0][resp_keys]))
            #     print(response[0][resp_keys])

            response_dict["contract_name"] = response[0]["ContractName"]
            # response_dict["source_code"] = parse_source_code(response[0]["SourceCode"], response[0]["ContractName"])
            # if response[0]["SourceCode"][:2] == "{{":
            #     source_json = json.loads(response[0]["SourceCode"][1:-1])
            #     source_concat = ""
            #     for source_value in source_json["sources"].values():
            #         source_concat += re.sub(multi_file_re_pattern_for_formatting,
            #                                 "//  import \"", str(source_value["content"]))
            #
            #     print(source_concat)
            # response_dict["abi"] = jsonify_abi(response[0]["ABI"])
            response_dict["abi"] = response[0]["ABI"]
            response_dict["source_code"] = response[0]["SourceCode"]
            response_dict["compiler_version"] = response[0]["CompilerVersion"]
        else:
            response["is_verified"] = False
            # print("Failed to return")

        creator_data = await blockscan.contract.contract_creation([address_to_check])
        response_dict["contract_creator"] = creator_data[0]["contractCreator"]
        response_dict["contract_creation_tx_hash"] = creator_data[0]["txHash"]
        creation_tx = await blockscan.proxy.tx_by_hash(response_dict["contract_creation_tx_hash"])
        response_dict["contract_creation_code"] = creation_tx["input"]
        if response[0]["Proxy"] == "1":
            response_dict["is_proxy"] = True
            response_dict["proxy_implementation_data"][response[0]["Implementation"]] = \
                await retrieve_block_explorer_data_async(response[0]["Implementation"], api_key, blockchain_network)

        # w3 = Web3(Web3.HTTPProvider("https://eth-pokt.nodies.app"))
        # contract = w3.eth.contract(address=Web3.to_checksum_address(address_to_check), abi=response_dict["abi"])
        # response_dict["token_name"] = contract.functions.name().call()
        # response_dict["contract_symbol"] = contract.functions.symbol().call()

    except Exception as err:
        print("*** Error occurred ***\n", err)
        # pprint(response)
    finally:
        await blockscan.close()
        return response_dict


if __name__ == '__main__':
    # api_key = ""
    blockchain_api_key = load_api_key()

    contract_data = retrieve_block_explorer_data(address_to_check="0x0ae7b7588de17fcb4eef73a5b2438ab90aa26345",
                                                 api_key=blockchain_api_key)
    # print(contract_data)

    ast_response = generate_ast_from_string(contract_data["source_code"],
                                            compiler_version=str(contract_data["compiler_version"]))

    save_json_file("test.json", ast_response)
