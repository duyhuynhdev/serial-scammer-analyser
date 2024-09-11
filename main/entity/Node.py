from marshmallow.orderedset import OrderedSet
import json
import utils.Utils
from data_collection.AccountCollector import TransactionCollector
from entity.blockchain.Transaction import NormalTransaction, InternalTransaction
import numpy as np


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
    def __init__(self, address, parent, eoa_neighbours=None, contract_neighbours=None, normal_txs=None,
                 internal_txs=None, labels=None,
                 tag_name=""):
        self.address = address
        self.labels = labels if labels is not None else set()
        self.tag_name = tag_name
        self.parent = parent
        self.eoa_neighbours = OrderedSet(eoa_neighbours) if eoa_neighbours is not None else OrderedSet()
        self.contract_neighbours = OrderedSet(contract_neighbours) if contract_neighbours is not None else OrderedSet()
        self.normal_txs = normal_txs if normal_txs is not None else []
        self.internal_txs = internal_txs if internal_txs is not None else []

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
    eoa_neighbours, contract_neighbours = get_neighbours_from_transactions(address, normal_txs, internal_txs)
    node = Node(address, parent, eoa_neighbours, contract_neighbours, normal_txs, internal_txs)
    return node


def classify_transactions(txs, scammer_address):
    in_txs, out_txs, contract_creation_txs = list(), list(), list()
    for tx in txs:
        if tx.to == scammer_address:
            in_txs.append(tx)
        elif tx.sender == scammer_address and tx.to is not np.nan:
            out_txs.append(tx)
        elif tx.sender == scammer_address and tx.to is np.nan:
            contract_creation_txs.append(tx)
    return in_txs, out_txs, contract_creation_txs


def get_neighbours_from_transactions(scammer_address, normal_txs, internal_txs):
    eoa_neighbours, contract_neighbours = [], []
    if normal_txs is not None and len(normal_txs) > 0:
        # normal_txs.fillna(None, inplace=True)
        in_txs, out_txs, contract_creation_txs = classify_transactions(normal_txs, scammer_address)
        if len(in_txs) > 0:
            # all senders in normal txs must be EOA
            eoa_neighbours.extend([tx.sender for tx in in_txs ])
        if len(contract_creation_txs) > 0:
            # add created contract into contract list
            contract_neighbours.extend([tx.contractAddress for tx in contract_creation_txs ])
        if len(out_txs) > 0:
            out_txs_to_contract = [tx for tx in out_txs if tx.functionName is not np.nan]
            out_txs_to_eoa = [tx for tx in out_txs if tx.functionName is np.nan]
            if len(out_txs_to_contract) > 0:
                contract_neighbours.extend([tx.to for tx in out_txs_to_contract])
            if len(out_txs_to_eoa) > 0:
                eoa_neighbours.extend([tx.to for tx in out_txs_to_eoa])
    if internal_txs is not None and len(internal_txs) > 0:
        # receiver in internal txs of an eoa is always itself
        contract_neighbours.extend([tx.sender for tx in internal_txs])
    return OrderedSet(eoa_neighbours), OrderedSet(contract_neighbours)


if __name__ == '__main__':
    create_node("0x48f0fc8dfc672dd45e53b6c53cd5b09c71d9fbd6", None, dex='univ2')
