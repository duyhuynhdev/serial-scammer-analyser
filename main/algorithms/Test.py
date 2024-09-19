from entity.Node import create_node, NodeLabel
from utils.DataLoader import DataLoader


# Group 2809 coordinator node 0x14f27cec40a190713fa63ec30ee43d86e95cbe8c
# Group 2751 coordinator node 0x2513b299ffd6878e6d9ceafcaadb94acacfa8b23
# Group 2751 coordinator node 0xB1a2a230a02ebC221caC8e0f1efF8f3a2d2cB83D
# Group 34 coordinator node 0x406e8d52fFcC1708bbad849a5541e6C500807298
dataloader = DataLoader()

def debug ():
    node = create_node("0x94e247c276CC6046C15d7e44D951b97080D69e13", None, dataloader, dex='univ2')
    print(node.labels)

def coordinator_test():

    coor_adds = ["0x94e247c276CC6046C15d7e44D951b97080D69e13", "0x14f27cec40a190713fa63ec30ee43d86e95cbe8c", "0x2513b299ffd6878e6d9ceafcaadb94acacfa8b23", "0xB1a2a230a02ebC221caC8e0f1efF8f3a2d2cB83D", "0x406e8d52fFcC1708bbad849a5541e6C500807298"]
    non_coor_adds = ["0xcac0f1a06d3f02397cfb6d7077321d73b504916e"]
    print("COORDINATOR", coor_adds)
    print("NON-COORDINATOR", non_coor_adds)
    for coor_add in coor_adds:
        node = create_node(coor_add, None, dataloader, dex='univ2')
        print(node.labels)
        assert NodeLabel.COORDINATOR in node.labels
    for non_coor_add in non_coor_adds:
        node = create_node(non_coor_add, None, dataloader, dex='univ2')
        print(node.labels)
        assert NodeLabel.COORDINATOR not in node.labels


if __name__ == '__main__':
    debug()
