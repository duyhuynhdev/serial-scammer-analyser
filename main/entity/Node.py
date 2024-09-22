from aiohttp.web_routedef import static
from marshmallow.orderedset import OrderedSet
import json

from web3 import Web3
from data_collection.DataDecoder import FunctionInputDecoder
from utils import Constant
from data_collection.AccountCollector import TransactionCollector
from entity.blockchain.Transaction import NormalTransaction, InternalTransaction
import numpy as np


class NodeLabel:
    # SCAMMER LABELS
    SCAMMER = "scammer"  # who is in our scammer set
    DEPOSITOR = "depositor"  # who send money to defi node
    WITHDRAWER = "withdrawer"  # who receive money from defi node
    COORDINATOR = "coordinator"  # ???
    TRANSFER = "transfer_node"  # just receive money and transfer
    SCAMPOOL = "scam_pool"  # rug-pull pool
    SCAMTOKEN = "scam_token"  # token in rug-pull pool
    WASHTRADER = "wash_trader"
    WASHTRADINGFUNDER = "wash_trading_funder"

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

    @staticmethod
    def is_scammer(node):
        return NodeLabel.SCAMMER in node.labels


class Node:
    def __init__(
        self,
        address,
        path,
        eoa_neighbours=None,
        contract_neighbours=None,
        normal_txs=None,
        internal_txs=None,
        swap_txs=None,
        contract_creation_txs=None,
        labels=None,
        tag_name="",
    ):
        self.address = address
        self.labels = labels if labels is not None else set()
        self.tag_name = tag_name
        self.path = path.copy() if path is not None else []
        self.path.append(self.address)
        self.eoa_neighbours = (
            OrderedSet(eoa_neighbours) if eoa_neighbours is not None else OrderedSet()
        )
        self.contract_neighbours = (
            OrderedSet(contract_neighbours)
            if contract_neighbours is not None
            else OrderedSet()
        )
        self.normal_txs = normal_txs if normal_txs is not None else []
        self.internal_txs = internal_txs if internal_txs is not None else []
        self.swap_txs = swap_txs if swap_txs is not None else []
        self.contract_creation_txs = (
            contract_creation_txs if contract_creation_txs is not None else []
        )


def create_end_node(address, path, label):
    node = Node(address, path)
    node.labels.add(label)
    return node


def create_node(address, path, dataloader, label=NodeLabel.EOA, dex="univ2"):
    transaction_collector = TransactionCollector()
    normal_txs, internal_txs = transaction_collector.get_transactions(address, dex)
    print(
        "\t CREATE NODE FOR ",
        address,
        " WITH NORMAL TX:",
        len(normal_txs) if normal_txs is not None else 0,
        "AND INTERNAL TX:",
        len(internal_txs) if internal_txs is not None else 0,
    )
    if normal_txs is None and internal_txs is None:
        return create_end_node(address, path, NodeLabel.BLANK)
    eoa_neighbours, contract_neighbours, swap_txs, contract_creation_txs, labels = (
        get_neighbours_from_transactions(address, normal_txs, internal_txs, dataloader)
    )
    node = Node(
        address,
        path,
        eoa_neighbours,
        contract_neighbours,
        normal_txs,
        internal_txs,
        swap_txs,
        contract_creation_txs,
    )
    node.labels.add(label)
    node.labels.update(labels)
    if (len(normal_txs) + len(internal_txs)) >= Constant.BIG_NODE_TXS:
        node.labels.add(NodeLabel.BIG)
    return node


def get_scammers_list(scam_token, dataloader):
    scam_pool = dataloader.scam_token_pool[scam_token.lower()]
    scammers = []
    if scam_pool is not None and scam_pool.lower() in dataloader.pool_scammers.keys():
        scammers = dataloader.pool_scammers[scam_pool]
    return scam_pool, scammers


def get_neighbours_from_transactions(
    scammer_address, normal_txs, internal_txs, dataloader
):
    function_decoder = FunctionInputDecoder()
    eoa_neighbours, contract_neighbours, swap_txs, contract_creation_txs = (
        [],
        [],
        [],
        [],
    )
    labels = set()
    scammer_neighbours = set()
    normal_accounts = set()
    if normal_txs is not None and len(normal_txs) > 0:
        for tx in normal_txs:
            if tx.is_in_tx(scammer_address) and float(tx.value) > 0:
                eoa_neighbours.append(tx.sender)
                # check if sender is CEX, MIXER, OR BRIDGE
                if tx.sender in (
                    dataloader.bridge_addresses
                    | dataloader.mixer_addresses
                    | dataloader.cex_addresses
                ):
                    labels.add(NodeLabel.WITHDRAWER)
                else:
                    normal_accounts.add(tx.sender)
                if tx.sender in dataloader.scammers:
                    scammer_neighbours.add(tx.sender)
            elif (
                tx.is_out_tx(scammer_address)
                and tx.is_to_eoa(scammer_address)
                and float(tx.value) > 0
            ):
                eoa_neighbours.append(tx.to)
                # check if receiver is CEX, MIXER, OR BRIDGE
                if tx.to in (
                    dataloader.bridge_addresses
                    | dataloader.mixer_addresses
                    | dataloader.cex_addresses
                ):
                    labels.add(NodeLabel.DEPOSITOR)
                else:
                    normal_accounts.add(tx.to)
                if tx.to in dataloader.scammers:
                    scammer_neighbours.add(tx.to)
            elif tx.is_out_tx(scammer_address) and tx.is_creation_contract(
                scammer_address
            ):
                contract_neighbours.append(tx.contractAddress)
            elif tx.is_out_tx(scammer_address) and tx.is_to_contract(scammer_address):
                parsed_inputs = function_decoder.decode_function_input(tx.input)
                if (parsed_inputs is not None) and (len(parsed_inputs) > 0):
                    swap_txs.append(tx)
                    # get path inputs from parsed inputs
                    paths = [pi["path"] for pi in parsed_inputs if "path" in pi.keys()]
                    # Get all tokens from paths (each path contains 2 tokens)
                    # The first element of path is the input token, the last is the output token
                    # Hence if path [0] is HV token -> the swap is swap in
                    scam_tokens = [
                        path[1]
                        for path in paths
                        if (len(path) == 2)
                        and (path[0].lower() in Constant.HIGH_VALUE_TOKENS)
                        and (path[1].lower() in dataloader.scam_token_pool.keys())
                    ]
                    # scam_tokens = [token for path in paths for token in path if token.lower() in dataloader.scam_token_pool.keys()]
                    if len(scam_tokens) > 0:
                        labels.add(NodeLabel.WASHTRADER)
                    for token in scam_tokens:
                        scam_pool, scammers = get_scammers_list(token, dataloader)
                        if scam_pool is not None:
                            contract_creation_txs.append(scam_pool)
                        if scammers is not None and len(scammers) > 0:
                            eoa_neighbours.extend(scammers)
                            scammer_neighbours.add(scammers[0])  # add presentative only
                            normal_accounts.add(scammers[0])  # add presentative only
            # else:
            # print(f"Transaction {tx.hash} cannot be classified (SKIPPED)")
    if (
        len(scammer_neighbours) >= 5
        and (len(scammer_neighbours) * 1.0 / len(normal_accounts)) > 0.5
    ):
        labels.add(NodeLabel.COORDINATOR)
    if internal_txs is not None and len(internal_txs) > 0:
        # receiver in internal txs of an eoa is always itself
        contract_neighbours.extend([tx.sender for tx in internal_txs])
    return (
        OrderedSet(eoa_neighbours),
        OrderedSet(contract_neighbours),
        swap_txs,
        contract_creation_txs,
        labels,
    )


if __name__ == "__main__":
    create_node("0x0ae5a86ea44c76911deed02e48bc61520e925137", None, None, dex="univ2")
