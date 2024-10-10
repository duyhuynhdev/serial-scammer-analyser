# from entity.Node import create_node, NodeLabel
from utils.Utils import  TransactionUtils
from utils.DataLoader import DataLoader
from data_collection.AccountCollector import TransactionCollector

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


if __name__ == '__main__':
    dex = "panv2"
    collector = TransactionCollector()
    address = "0x98d3047Fb31b6ED04bB1dFA3558F3AAD1c147b12"
    dataloader = DataLoader(dex=dex)
    normal_txs, internal_txs = collector.get_transactions(address, dex=dex, key_idx=17)
    for normal_tx in normal_txs:
        # print("Function name: ",normal_tx.functionName, " is matched", True if "addLiquidity" in str(normal_tx.functionName) or "removeLiquidity" in str(normal_tx.functionName) else False)
        if TransactionUtils.is_scam_add_liq(normal_tx, dataloader):
            amount = normal_tx.get_transaction_amount()
            print(f"\tFOUND SCAM LIQ ADDING { normal_tx.hash} WITH ADDED AMOUNT iS {amount}")

        if TransactionUtils.is_scam_remove_liq(normal_tx, dataloader):
            amount = TransactionUtils.get_related_amount_from_internal_txs(normal_tx, internal_txs)
            print(f"\tFOUND SCAM LIQ REMOVAL {normal_tx.hash} WITH REMOVED AMOUNT iS {amount}")