import sys
import os
from collections import Counter

import numpy as np
import pandas as pd
from tqdm import tqdm
import random

from data_collection.AccountCollector import TransactionCollector
from data_collection.ContractCollector import ContractSourceCodeCollector
from utils import Utils as ut
from similarity import ContractTokenization
from utils import DataLoader

sys.path.append(os.path.join(os.path.dirname(sys.path[0])))
from utils.Settings import Setting
from utils.ProjectPath import ProjectPath

path = ProjectPath()
setting = Setting()

endnodes = DataLoader.load_full_end_nodes(dex='panv2')

zero_txs_addresses = ["0x00c8357eebba2021ee4d8cbd2946145bcc7aa2fb",
                      "0x55cc3a3a89f480e6515e26ebab9a672a6ca43bd8",
                      "0x4d8ba761dc5307f905b3397d697ef898cddf5c2b",
                      "0x484916b5e52dc13bfd42fa55f25dd1d16378a492",
                      "0x0b58aa7ae69eecec4f3807a988fd179012e02b30",
                      "0xc1a2e5beaeb7a682bcdac2133590592959be6536",
                      "0x61329d9b6fa4c5537e1355fdee268eb0c92eadc4",
                      "0x9c8ef15a470e94555810799da82d2f77055b243e",
                      "0xa3490feefc1263139da31b01261d0ab88441b5e2",
                      "0xadfe6b78afe7ffe9f548ffdacbcaf1969bba5f5e",
                      "0xe83334f292888ed34d1a570d553319ab5cdd0278",
                      "0x43ac9dc950d58110f1a5d6b98e283b0b0a5094d2",
                      "0x10a7fb9ce9418597d13a1fdeaf61b99f0e2409a2",
                      "0x53d7a75bc04e6ee1de684fb1e42e2e6e25ff52c4",
                      "0xcb2362cc29a3fd6eea1b40f8b9797cb5fd22740b",
                      "0xa41c14f98574cd79a8c29fdbc0b66870da3477f6",
                      "0x5dab314850f26383a2082a678e691c05edb38b1e",
                      "0xa80fb505c4215d825caca47b9f61773784b10610",
                      "0x24d4de0e79dcbd9987cf3b1eeff435224d7fcfae",
                      "0x3eb84ceb1dca45e505e82e9429ba04a432581f0b",
                      "0xb3736b9b5dd8a2267c948cabd1cce3e40f79ab4e",
                      "0xb4b39d19258df8f39d0d855acad9902512bd1721",
                      "0xa85a885976ce5b1b7e8b5dea7e3042425d5f13f4",
                      "0x3a45d49f6128c0f5b6ecebe01ec90e405ec8da7c",
                      "0x789b0210d833695863a37973966a6008a5ad0496",
                      "0xe69849eb544cdbfc6b720ff09fc4ad1c4dedf90b",
                      "0x7c060c8001235d8f98d7ab8502077ea6da1555aa",
                      "0x423642d039b659e95e67e6b4c1e589cd7f998044",
                      "0xd84aae3c9f373eeab629e425145ed63e854619f0",
                      "0xa5d766dd9f0de3a3bee64bc1b0a9408d4920a808",
                      "0x4fa5316f264805a791a2b49f9191a8de08eb7a5b",
                      "0xe3f92abb8bc37f72bcff6beb9906fe7341adce9f",
                      "0xce8779177857d586419f3c0da8518832c88cff8b",
                      "0x297160a11c87599922fa953fc7b029210e53e3aa",
                      "0xb79c44945e5ddfde51e3d4f55663158c29071d46",
                      "0x9e7292514bdb02e40e0272f5191556ff22ec27e3",
                      "0x8cdc0eb3c9f94694dae870ad39cb2193b25985f6"]


def extract_simple_rp(dex='univ2'):
    scammer_df = pd.read_csv(os.path.join(eval("path.{}_processed_path".format(dex)), "simple_rp_scammers.csv"))
    rp_pools = pd.read_csv(os.path.join(eval("path.{}_processed_path".format(dex)), "simple_rp_pool_labels.csv"))
    rp_pools.fillna("", inplace=True)
    print("All pool:", len(rp_pools))
    print("All scammer:", len(scammer_df["scammer"].unique()))
    simple_rp_df = rp_pools[rp_pools["is_rp"] == 1]
    simple_rp_scammer_df = scammer_df[scammer_df["pool"].isin(simple_rp_df['pool'])]
    print("Simple RP pools:", len(simple_rp_df))
    print("Simple RP scammers", len(simple_rp_scammer_df["scammer"].unique()))
    wrong_scammer_df = simple_rp_scammer_df[simple_rp_scammer_df["scammer"].str.lower().isin(endnodes)]
    print("Wrong RP pools:", len(wrong_scammer_df["pool"].unique()))
    filtered_simple_rp_scammer_df = simple_rp_scammer_df[~simple_rp_scammer_df["pool"].str.lower().isin(wrong_scammer_df["pool"].str.lower().values)]
    filtered_simple_rp_scammer_df = filtered_simple_rp_scammer_df[~filtered_simple_rp_scammer_df["scammer"].str.lower().isin(zero_txs_addresses)]
    simple_rp_df = simple_rp_df[~simple_rp_df["pool"].str.lower().isin(wrong_scammer_df["pool"].str.lower().values)]
    print("Non-endnode Simple RP pools", len(simple_rp_df))
    print("Non-endnode Simple RP scammers", len(filtered_simple_rp_scammer_df["scammer"].unique()))
    filtered_simple_rp_scammer_df.to_csv(os.path.join(eval("path.{}_processed_path".format(dex)), "filtered_simple_rp_scammers.csv"), index=False)
    simple_rp_df.to_csv(os.path.join(eval("path.{}_processed_path".format(dex)), "filtered_simple_rp_pool.csv"), index=False)
    wrong_scammer_df.to_csv(os.path.join(eval("path.{}_processed_path".format(dex)), "invalid_simple_rp_scammers.csv"), index=False)


# def filter_scammer(dex='univ2'):
#     scammer_df = pd.read_csv(os.path.join(eval("path.{}_processed_path".format(dex)), "1_pair_scammers.csv"))
#     rp_pools = pd.read_csv(os.path.join(eval("path.{}_processed_path".format(dex)), "1_pair_pool_labels.csv"))
#     rp_pools.fillna("", inplace=True)
#     print("All pool:", len(rp_pools))
#     print("All scammer:", len(scammer_df["scammer"].unique()))
#     rp_df = rp_pools[rp_pools["is_rp"] != 0]
#     rp_scammer_df = scammer_df[scammer_df["pool"].isin(rp_df['pool'])]
#     filtered_rp_scammer_df = rp_scammer_df[~rp_scammer_df["scammer"].str.lower().isin(endnodes)]
#     wrong_scammer_df = rp_scammer_df[rp_scammer_df["scammer"].str.lower().isin(endnodes)]
#     print(" RP pools:", len(rp_df))
#     print(" RP scammers", len(rp_scammer_df["scammer"].unique()))
#     print("Non-endnode RP scammers", len(filtered_rp_scammer_df["scammer"].unique()))
#     rp_scammer_df.to_csv(os.path.join(eval("path.{}_processed_path".format(dex)), "filtered_full_rp_scammers.csv"), index=False)
#     wrong_scammer_df.to_csv(os.path.join(eval("path.{}_processed_path".format(dex)), "wrong_full_rp_scammers.csv"), index=False)
#     rp_df.to_csv(os.path.join(eval("path.{}_processed_path".format(dex)), "filtered_full_rp_pool.csv"), index=False)
def ensure_valid_eoa_address(address, transaction_collector, cluster_id, dex ):
    normal_txs, internal_txs = transaction_collector.get_transactions(address, dex, cluster_id)
    for ntx in normal_txs:
        if ntx.sender.lower() == address.lower():  # sender of normal txs must be EOA
            return True
        if ntx.is_creation_contract_tx() and ntx.contractAddress.lower() == address.lower():  # address is a contract created by an EOA
            return False
        if ntx.is_contract_call_tx() and ntx.to.lower() == address.lower():  # address is a contract called by other address
            return False
    for itx in internal_txs:
        if itx.sender.lower() == address.lower():  # sender of internal txs must be contract
            return False
        if itx.is_creation_contract_tx() and itx.contractAddress.lower() == address.lower():  # address is a contract created by another contract
            return False
    return True


def filter_non_scammer_addresses(dex='univ2'):
    transaction_collector = TransactionCollector()
    rp_scammer_df = pd.read_csv(os.path.join(eval("path.{}_processed_path".format(dex)), "filtered_simple_rp_scammers.csv"))
    scammers = rp_scammer_df["scammer"].unique()
    print("BEFORE", len(rp_scammer_df))
    eoa = []
    for scammer_address in tqdm(scammers):
        if ensure_valid_eoa_address(scammer_address, transaction_collector,11, dex):
            eoa.append(scammer_address)
        else:
            print("FOUND CONTRACT ADDRESS:", scammer_address)
    rp_scammer_df = rp_scammer_df[rp_scammer_df["scammer"].isin(eoa)]
    print("AFTER", len(rp_scammer_df))
    rp_scammer_df.to_csv(os.path.join(eval("path.{}_processed_path".format(dex)), "filtered_simple_rp_scammers.csv"), index=False)


if __name__ == '__main__':
    dex="panv2"
    extract_simple_rp(dex=dex)
    filter_non_scammer_addresses(dex=dex)
