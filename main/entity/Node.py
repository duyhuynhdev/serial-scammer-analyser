from marshmallow.orderedset import OrderedSet
import json

from web3 import Web3
from data_collection.DataDecoder import FunctionInputDecoder
from utils import Constant
from data_collection.AccountCollector import TransactionCollector
from entity.blockchain.Transaction import NormalTransaction, InternalTransaction
import numpy as np

from utils.DataLoader import DataLoader


class NodeLabel:
    U = "unknown"

    # SCAMMER LABELS
    S = "scammer"  # who is in our scammer set
    D = "deposit"  # who send money to defi node
    W = "withdraw"  # who receive money from defi node
    C = "coordinator"  # ???
    T = "transfer"  # just receive money and transfer
    SP = "scam_pool"  # rug-pull pool
    ST = "scam_token"  # token in rug-pull pool
    SF = "scammer_funder"
    SB = "scammer_beneficiary"
    WT = "wash_trading"
    WTF = "wash_trading_funder"

    # PUBLIC ADDRESSES LABELS
    DEFI = "dex_cex_public_address"
    MEV = "sniper_bots"
    BRIDGE = "bridge"
    MIXER = "mixer_public_address"
    WALLET = "wallet_public_address"
    OTHER = "other_public_address"

    # NODE LABELS
    UC = "unknown_contract"  # contract nodes
    BLANK = "blank"  # no transaction nodes
    EOA = "eoa"  # personal account nodes
    BIG = "big"  # big nodes that contains EOA neighbour > 1K
    GREY = "grey"  # node mix between malicious and benign

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
        path = [self.address]
        parent = self.parent
        while parent is not None:
            path.append(parent.address)
            parent = parent.parent
        path = list(reversed(path))
        return path


def create_end_node(address, parent, label):
    node = Node(address, parent)
    node.labels.add(label)
    return node


def create_node(address, parent, dataloader: DataLoader, label=NodeLabel.EOA, dex='univ2'):
    transaction_collector = TransactionCollector()
    normal_txs, internal_txs = transaction_collector.get_transactions(address, dex)
    print("\t CREATE NODE FOR ", address, " WITH NORMAL TX:", len(normal_txs) if normal_txs is not None else 0, "AND INTERNAL TX:", len(internal_txs) if internal_txs is not None else 0)
    if normal_txs is None and internal_txs is None:
        return create_end_node(address, parent, NodeLabel.BLANK)
    eoa_neighbours, contract_neighbours, is_grey = get_neighbours_from_transactions(address, normal_txs, internal_txs, dataloader)
    if is_grey:
        print("\t FOUND GREY NODE ", address)
        return create_end_node(address, parent, NodeLabel.GREY)
    node = Node(address, parent, eoa_neighbours, contract_neighbours, normal_txs, internal_txs)
    node.labels.add(label)
    # if eoa_neighbours is not None and len(eoa_neighbours) > Constant.BIG_NODE_TXS:
    #     node.labels.add(NodeLabel.BIG)
    return node


def classify_transactions(txs, scammer_address):
    in_txs, out_txs, contract_creation_txs = list(), list(), list()
    for tx in txs:
        if tx.to == scammer_address:
            in_txs.append(tx)
        elif tx.sender == scammer_address and tx.to is not np.nan and tx.to != "":
            out_txs.append(tx)
        elif tx.sender == scammer_address and (tx.to is np.nan or tx.to == ""):
            contract_creation_txs.append(tx)
    return in_txs, out_txs, contract_creation_txs


def get_neighbours_from_transactions(scammer_address, normal_txs, internal_txs, dataloader: DataLoader):
    fdecoder = FunctionInputDecoder()
    eoa_neighbours, contract_neighbours = [], []
    if normal_txs is not None and len(normal_txs) > 0:
        # normal_txs.fillna(None, inplace=True)
        in_txs, out_txs, contract_creation_txs = classify_transactions(normal_txs, scammer_address)
        if len(in_txs) > 0:
            # all senders in normal txs must be EOA
            eoa_neighbours.extend([tx.sender for tx in in_txs if int(tx.value) > 0])
        if len(contract_creation_txs) > 0:
            # add created contract into contract list
            contract_neighbours.extend([tx.contractAddress for tx in contract_creation_txs])
        if len(out_txs) > 0:
            out_txs_to_contract = [tx for tx in out_txs if tx.functionName is not np.nan]
            out_txs_to_eoa = [tx for tx in out_txs if (tx.functionName is np.nan) and (int(tx.value) > 0)]
            if len(out_txs_to_eoa) > 0:
                eoa_neighbours.extend([tx.to for tx in out_txs_to_eoa])
            if len(out_txs_to_contract) > 0:
                # contract_neighbours.extend([tx.to for tx in out_txs_to_contract])
                for contract_call_tx in out_txs_to_contract:
                    paths = list()
                    parsed_inputs = fdecoder.decode_function_input(contract_call_tx.input)
                    for input in parsed_inputs:
                        paths.append(input["path"])
                    for path in paths:
                        scam_count = 0
                        for token in path:
                            if token.lower() in dataloader.scam_token_pool.keys():
                                scam_count += 1
                                scam_pool = dataloader.scam_token_pool[token.lower()]
                                if scam_pool is not None:
                                    contract_neighbours.append(scam_pool)
                                    scammers = dataloader.pool_scammers[scam_pool]
                                    # scam_pool_creator = dataloader.creators[scam_pool]
                                    if scammers is not None:
                                        eoa_neighbours.extend(scammers)
                        if scam_count == 0 and scammer_address.lower() not in dataloader.scammers:
                            print(f"\t\t FOUND BENIGN SWAP TX {contract_call_tx.hash} TOKEN 0 {path[0]} TOKEN 1 {path[1]}")
                            return OrderedSet(), OrderedSet(), True
    if internal_txs is not None and len(internal_txs) > 0:
        # receiver in internal txs of an eoa is always itself
        contract_neighbours.extend([tx.sender for tx in internal_txs])
    return OrderedSet(eoa_neighbours), OrderedSet(contract_neighbours), False


if __name__ == '__main__':
    create_node("0x48f0fc8dfc672dd45e53b6c53cd5b09c71d9fbd6", None, DataLoader(), dex='univ2')
