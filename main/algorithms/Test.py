# from entity.Node import create_node, NodeLabel
import os

import pandas as pd
from tqdm import tqdm

from utils.Utils import  TransactionUtils
from utils.DataLoader import DataLoader
from data_collection.AccountCollector import TransactionCollector
from utils.ProjectPath import ProjectPath

path = ProjectPath()
# Group 2809 coordinator node 0x14f27cec40a190713fa63ec30ee43d86e95cbe8c
# Group 2751 coordinator node 0x2513b299ffd6878e6d9ceafcaadb94acacfa8b23
# Group 2751 coordinator node 0xB1a2a230a02ebC221caC8e0f1efF8f3a2d2cB83D
# Group 34 coordinator node 0x406e8d52fFcC1708bbad849a5541e6C500807298
# dataloader = DataLoader()
#
# def debug ():
#     node = create_node("0x94e247c276CC6046C15d7e44D951b97080D69e13", None, dataloader, dex='univ2')
#     print(node.labels)
#
# def coordinator_test():
#
#     coor_adds = ["0x94e247c276CC6046C15d7e44D951b97080D69e13", "0x14f27cec40a190713fa63ec30ee43d86e95cbe8c", "0x2513b299ffd6878e6d9ceafcaadb94acacfa8b23", "0xB1a2a230a02ebC221caC8e0f1efF8f3a2d2cB83D", "0x406e8d52fFcC1708bbad849a5541e6C500807298"]
#     non_coor_adds = ["0xcac0f1a06d3f02397cfb6d7077321d73b504916e"]
#     print("COORDINATOR", coor_adds)
#     print("NON-COORDINATOR", non_coor_adds)
#     for coor_add in coor_adds:
#         node = create_node(coor_add, None, dataloader, dex='univ2')
#         print(node.labels)
#         assert NodeLabel.COORDINATOR in node.labels
#     for non_coor_add in non_coor_adds:
#         node = create_node(non_coor_add, None, dataloader, dex='univ2')
#         print(node.labels)
#         assert NodeLabel.COORDINATOR not in node.labels

def test_single_scammer(address, collector, dataloader,  dex='univ2'):
    print("*"*100)
    print("CHECK SCAMMER", address)
    normal_txs, internal_txs = collector.get_transactions(address, dex=dex, key_idx=17)
    found_add = False
    found_rev = False
    for normal_tx in normal_txs:
        # print("Function name: ", normal_tx.functionName, f" is matched ({normal_tx.hash})", True if "addLiquidity" in str(normal_tx.functionName) or "removeLiquidity" in str(normal_tx.functionName) else False)
        if TransactionUtils.is_scam_add_liq(normal_tx, dataloader):
            found_add = True
            amount = TransactionUtils.get_add_liq_amount(normal_tx, normal_txs, dataloader)
            # print(f"\tFOUND SCAM LIQ ADDING {normal_tx.hash} WITH ADDED AMOUNT iS {amount}")
            if amount > 0:
                found_add = True

        if TransactionUtils.is_scam_remove_liq(normal_tx, dataloader):
            found_rev = True
            amount = TransactionUtils.get_related_amount_from_internal_txs(normal_tx, normal_txs, internal_txs)
            # print(f"\tFOUND SCAM LIQ REMOVAL {normal_tx.hash} WITH REMOVED AMOUNT iS {amount}")
            if amount > 0:
                found_rev = True
    assert found_add
    assert found_rev
    return found_add, found_rev

def test_all_scammer(collector, dataloader, dex='univ2'):
    ignores = [
        "0xe2772341c3ca68f332ec8d68d13b5ebae3dd26d8", # migration -> non add liq
        "0x2556942f62a9c29147620a3ed4abbb0d1ba7141e", # sell rp -> mis labelled -> non add liq
        "0x1f3e0da640ad4c2f6df33341e282cbcb01a1f067", # sell rp -> mis labelled -> non add liq
        "0x6ff7ab81f907a5201ace41d99f7ba987b1380879", # sell rp -> mis labelled -> non add liq
    ]
    df = pd.read_csv(
        os.path.join(eval("path.{}_processed_path".format(dex)), "filtered_simple_rp_pool.csv")
    )
    scammers = df["creator"].str.lower().values
    no_add = []
    no_rev = []
    for s in tqdm(scammers):
        if s in ignores:
            continue
        found_add, found_rev = test_single_scammer(s, collector, dataloader, dex=dex)
        if not found_add:
            no_add.append(s)
        if not found_rev:
            no_rev.append(s)
    print(len(no_add), len(no_rev))
    print("NO ADD", len(no_add))
    print("NO REV", len(no_rev))

if __name__ == '__main__':
    dex = "univ2"
    collector = TransactionCollector()
    dataloader = DataLoader(dex=dex)
    # address = "0xe7daf02024dfcf0d36ed49a6f9b33beb430edb5b".lower()
    # test_single_scammer(address,collector, dataloader, dex)
    test_all_scammer(collector, dataloader, dex=dex)