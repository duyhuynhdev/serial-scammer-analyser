import sys
import pandas as pd
import solcx
import os

from tqdm import tqdm

from similarity import ContractTokenization

sys.path.append(os.path.join(os.path.dirname(sys.path[0])))
from utils.Settings import Setting
from utils.ProjectPath import ProjectPath
from utils import Utils as ut

path = ProjectPath()
setting = Setting()

def tokenize_contracts(addresses, dex='univ2'):
    ast_path = eval(f"path.{dex}_token_ast_path")
    tokenization_path = eval(f"path.{dex}_tokenization_path")
    for address in tqdm(addresses, "Tokenizing Contract Addresses"):
        try:
            address = address.lower()
            ast_file = os.path.join(ast_path, f"{address}.json")
            if not os.path.exists(ast_file):
                print(f"Cannot find AST  >> SKIP ({address})")
                continue
            ast = ut.read_json(ast_file)
            tokenization_file = os.path.join(tokenization_path, f"{address}.token")
            hash_file = os.path.join(tokenization_path, f"{address}.hash")
            if os.path.exists(tokenization_file):
                print(f"Tokenization file was generated already >> SKIP ({address})")
                continue
            tokens = ContractTokenization.tokenize_ast(ast)
            if len(tokens) == 0:
                print(f"Empty token list >> SKIP ({address})")
                continue
            ut.write_list_to_file(tokenization_file, tokens)
            hashes = []
            for element in tokens:
                hashes.append(ut.keccak_hash(element))
            ut.write_list_to_file(hash_file, hashes)
        except Exception as e:
            print("ERROR OCCUR:", address)
            print(e)


def load_scam_token_address(dex='univ2'):
    rp_pools = pd.read_csv(
        os.path.join(
            eval("path.{}_processed_path".format(dex)), "filtered_simple_rp_pool.csv"
        )
    )
    rp_pools.fillna("", inplace=True)
    rp_pools = rp_pools[rp_pools["is_rp"] != 0]
    rp_pools["scam_token"] = rp_pools["scam_token"].str.lower()
    addresses = rp_pools["scam_token"].values.tolist()
    return addresses


def generate_ast_for_scam_tokens(job, size=20, dex='univ2'):
    addresses = load_scam_token_address(dex)
    print("TOKEN LEN ", len(addresses))
    chunks = ut.partitioning(0, len(addresses), int(len(addresses) / size))
    chunk = chunks[job]
    chunk_addresses = addresses[chunk["from"]:(chunk["to"] + 1)]
    print(f'START DOWNLOADING DATA (JOB {job}/ {len(chunks)} CHUNKS):{chunk["from"]}_{chunk["to"]} (size: {len(chunk_addresses)})')
    generate_asts_and_tokens(chunk_addresses, dex)

def tokenize_ast_for_scam_tokens(job, size=20, dex='univ2'):
    addresses = load_scam_token_address(dex)
    chunks = ut.partitioning(0, len(addresses), int(len(addresses) / size))
    chunk = chunks[job]
    chunk_addresses = addresses[chunk["from"]:(chunk["to"] + 1)]
    print(f'START DOWNLOADING DATA (JOB {job}/ {len(chunks)} CHUNKS):{chunk["from"]}_{chunk["to"]} (size: {len(chunk_addresses)})')
    try:
        tokenize_contracts(chunk_addresses, dex)
    except Exception as e:
        print(e)

def generate_asts_and_tokens(addresses, dex='univ2'):
    contract_path = eval(f"path.{dex}_token_source_code_path")
    ast_path = eval(f"path.{dex}_token_ast_path")
    df = pd.read_csv(os.path.join(contract_path, 'solidity_version.csv'))
    compiler_versions = dict(zip(df["address"].str.lower(), df["solidity_version"]))
    for address in tqdm(addresses):
        address = address.lower()
        source_code_file = os.path.join(contract_path, f"{address}.sol")
        if not os.path.exists(source_code_file):
            print(f"Source code is unavailable >> SKIP ({address})")
            continue
        ast_file = os.path.join(ast_path, f"{address}.json")
        if not os.path.exists(ast_file):
            version = compiler_versions[address]
            print("Solidity Version", version)
            source_code = ut.read_file_to_string(source_code_file)
            try:
                ast = generate_ast_from_contract(source_code, version)
            except Exception as e:
                print("Cannot generate AST for {} with solidity version {}".format(address, version))
                print("\tError: ", e)
                continue
            ut.write_json(ast_file, ast)
            print(f"AST has been generated and saved ({address})")
        else:
            print(f"AST was already generated >> SKIP ({address})")


def generate_ast_from_contract(source_codes, compiler_version):
    if compiler_version is not None:
        compiler_version = compiler_version.split("+")[0]
        print("compiler version: ", compiler_version)
        solcx.install_solc(compiler_version, show_progress=True)
    compiled_data = solcx.compile_source(source_codes,
                                         solc_version=compiler_version,
                                         base_path=None,
                                         allow_paths=None,
                                         allow_empty=True)
    ast = ""
    for contract_entry in compiled_data.values():
        if len(str(contract_entry["ast"])) > len(str(ast)):
            ast = contract_entry["ast"]
    return ast


if __name__ == '__main__':
    job = 20
    # generate_ast_for_scam_tokens(job, dex='panv2')
    tokenize_ast_for_scam_tokens(job, dex='panv2')
