from marshmallow.orderedset import OrderedSet
import json
import utils.Utils
from data_collection.AccountCollector import TransactionCollector


class NodeLabel:
    U = "unknown"
    S = "scammer"
    D = "deposit"
    C = "coordinator"
    T = "transfer"
    P = "pool"
    SF = "scammer_funder"
    SB = "scammer_beneficiary"
    WT = "wash_trading"
    WTF = "wash_trading_funder"
    DEX = "dex_public_address"
    MEV = "sniper_bots"
    BRIDGE = "bridge"
    CEX = "cex_public_address"
    MIXER = "mixer_public_address"

    def is_scammer(self, node):
        return self.S in node.labels

class Node:
    def __init__(self, address, parent, eoa_next_neighbours=None, eoa_prev_neighbours=None, contract_next_neighbours=None, contract_prev_neighbours=None, in_txs=None, out_txs=None, labels=[],
                 tag_name=""):
        self.address = address
        self.labels = labels
        self.tag_name = tag_name
        self.parent = parent
        # below data field are used in scammer clustering only (network construction)
        self.eoa_next_neighbours = OrderedSet(eoa_next_neighbours) if eoa_next_neighbours is not None else OrderedSet()
        self.eoa_prev_neighbours = OrderedSet(eoa_prev_neighbours) if eoa_prev_neighbours is not None else OrderedSet()
        self.contract_next_neighbours = OrderedSet(contract_next_neighbours) if contract_next_neighbours is not None else OrderedSet()
        self.contract_prev_neighbours = OrderedSet(contract_prev_neighbours) if contract_prev_neighbours is not None else OrderedSet()
        self.normal_txs = []
        self.internal_txs = []
        self.in_txs = in_txs if in_txs is not None else []
        self.out_txs = out_txs if out_txs is not None else []
        self.in_degree = len(self.in_txs) if in_txs is not None else 0
        self.out_degree = len(self.out_txs) if out_txs is not None else 0

    def path(self):
        path = list()
        path.append(self.address)
        parent = self.parent
        while parent is not None:
            path.append(parent.address)
            parent = parent.parent
        path = list(reversed(path))
        return path

def create_end_node(address, parent):
    node = Node(address, parent)
    return node


def create_node(address, parent, dex='univ2'):
    transaction_collector = TransactionCollector()
    normal_txs, internal_txs = transaction_collector.get_transactions(address, dex)
    print("\t CREATE NODE FOR ", address, " WITH NORMAL TX:", len(normal_txs) if normal_txs is not None else 0, "AND INTERNAL TX:", len(internal_txs) if internal_txs is not None else 0)
    if normal_txs is None and internal_txs is None:
        return create_end_node(address, parent)
    (eoa_prev_neighbours,
     eoa_next_neighbours,
     contract_prev_neighbours,
     contract_next_neighbours, in_txs_list, out_txs_list) = get_neighbours_from_transactions(address, normal_txs, internal_txs)
    node = Node(address, parent, eoa_next_neighbours, eoa_prev_neighbours, contract_next_neighbours, contract_prev_neighbours, in_txs_list, out_txs_list)
    return node


def get_neighbours_from_transactions(scammer_address, normal_txs, internal_txs):
    eoa_prev_neighbours, eoa_next_neighbours, contract_prev_neighbours, contract_next_neighbours = [], [], [], []
    in_txs_list, out_txs_list = [], []
    if normal_txs is not None and len(normal_txs) > 0:
        # normal_txs.fillna(None, inplace=True)
        in_txs = normal_txs[normal_txs["to"] == scammer_address]
        if len(in_txs) > 0:
            # all senders in normal txs must be EOA
            eoa_prev_neighbours = in_txs["from"].tolist()
        contract_creation_txs = normal_txs[(normal_txs["from"] == scammer_address) & (normal_txs["to"].isna())]
        if len(contract_creation_txs) > 0:
            # add created contract into contract list
            contract_next_neighbours = contract_creation_txs["contractAddress"].tolist()
        out_txs = normal_txs[(normal_txs["from"] == scammer_address) & (~normal_txs["to"].isna())]
        if len(out_txs) > 0:
            out_txs_to_contract = out_txs[~out_txs["functionName"].isna()]
            out_txs_to_eoa = out_txs[out_txs["functionName"].isna()]
            if len(out_txs_to_contract) > 0:
                contract_next_neighbours.extend(out_txs_to_contract["to"].tolist())
            if len(out_txs_to_eoa) > 0:
                eoa_next_neighbours.extend(out_txs_to_eoa["to"].tolist())
        in_txs_list = in_txs.to_dict(orient="records")
        out_txs_list = out_txs.to_dict(orient="records")
    if internal_txs is not None and len(internal_txs) > 0:
        # receiver in internal txs of an eoa is always itself
        contract_prev_neighbours.extend(internal_txs["from"].tolist())
        in_txs_list.extend(internal_txs.to_dict(orient="records"))
    return OrderedSet(eoa_prev_neighbours), OrderedSet(eoa_next_neighbours), OrderedSet(contract_prev_neighbours), OrderedSet(contract_next_neighbours), in_txs_list, out_txs_list


if __name__ == '__main__':
    create_node("0x48f0fc8dfc672dd45e53b6c53cd5b09c71d9fbd6",None,dex='univ2')