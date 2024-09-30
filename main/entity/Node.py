from aiohttp.web_routedef import static
from marshmallow.orderedset import OrderedSet
import json

from web3 import Web3
from data_collection.DataDecoder import FunctionInputDecoder
from utils import Constant
from data_collection.AccountCollector import TransactionCollector
from entity.blockchain.Transaction import NormalTransaction, InternalTransaction
import numpy as np

function_decoder = FunctionInputDecoder()
transaction_collector = TransactionCollector()


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
    CEX = "cex"
    MEV = "sniper_bots"
    BRIDGE = "bridge"
    MIXER = "mixer_public_address"
    WALLET = "wallet_public_address"
    OTHER = "other_public_address"

    # NODE LABELS
    CONTRACT = "contract"  # contract nodes
    BLANK = "blank"  # no transaction nodes
    EOA = "eoa"  # personal account nodes
    BIG = "big"  # big nodes that contains EOA neighbour > 500 txs
    BIG_CONNECTOR = "big_connector"  # node connect with > 100 nodes

    @staticmethod
    def is_scammer(node):
        return NodeLabel.SCAMMER in node.labels


class Node:
    def __init__(self, address, path, eoa_neighbours=None, contract_neighbours=None, normal_txs=None,
                 internal_txs=None, labels=None,
                 tag_name=""):
        self.address = address
        self.labels = labels if labels is not None else set()
        self.tag_name = tag_name
        self.path = path.copy() if path is not None else []
        self.path.append(self.address)
        self.eoa_neighbours = eoa_neighbours if eoa_neighbours is not None else []
        self.contract_neighbours = contract_neighbours if contract_neighbours is not None else []
        self.normal_txs = normal_txs if normal_txs is not None else []
        self.internal_txs = internal_txs if internal_txs is not None else []


def create_end_node(address, path, label):
    node = Node(address, path)
    node.labels.add(label)
    return node


def create_node(address, path, dataloader,  existing_groups=None, dex='univ2'):
    if existing_groups is None:
        existing_groups = set()
    normal_txs, internal_txs = transaction_collector.get_transactions(address, dex)
    print("\t CREATE NODE FOR ", address, " WITH NORMAL TX:", len(normal_txs) if normal_txs is not None else 0, "AND INTERNAL TX:", len(internal_txs) if internal_txs is not None else 0)
    if normal_txs is None and internal_txs is None:
        return create_end_node(address, path, NodeLabel.BLANK)
    eoa_neighbours, contract_neighbours, labels = get_neighbours_and_labels(address, normal_txs, internal_txs, dataloader, existing_groups)
    node = Node(address, path, eoa_neighbours, contract_neighbours, normal_txs, internal_txs, labels)
    return node


def get_scammers_list_by_token(scam_token, dataloader, existing_groups=None):
    if existing_groups is None:
        existing_groups = set()
    scam_pool = dataloader.scam_token_pool[scam_token.lower()]
    scammers = []
    if scam_pool is not None and scam_pool.lower() in dataloader.pool_group.keys():
        group_id = dataloader.pool_group[scam_pool]
        if group_id in existing_groups:
            # print(f"\t\tFOUND A SWAP ON POOL {scam_pool} in GROUP {group_id} (SKIP BECAUSE THE GROUP HAS BEEN ADDED)")
            return []
        existing_groups.add(group_id)
        scammers = dataloader.group_scammers[group_id]
        # print(f"\t\tFOUND A SWAP ON POOL {scam_pool} in GROUP {group_id} SIZE {len(scammers)}")
    return scammers


def get_scammers_list_from_swap_tx(tx, dataloader, existing_groups=None):
    if existing_groups is None:
        existing_groups = set()
    parsed_inputs = function_decoder.decode_function_input(tx.input)
    scammers = list()
    if (parsed_inputs is not None) and (len(parsed_inputs) > 0):
        # get path inputs from parsed inputs
        paths = [pi["path"] for pi in parsed_inputs if "path" in pi.keys()]
        # Get all tokens from paths (each path contains 2 tokens)
        # The first element of path is the input token, the last is the output token
        # Hence if path [0] is HV token -> the swap is swap in
        scam_tokens = [path[1] for path in paths if (len(path) == 2) and (path[0].lower() in Constant.HIGH_VALUE_TOKENS) and (path[1].lower() in dataloader.scam_token_pool.keys())]
        # scam_tokens = [token for path in paths for token in path if token.lower() in dataloader.scam_token_pool.keys()]
        for token in scam_tokens:
            scammers.extend([s for s in get_scammers_list_by_token(token, dataloader, existing_groups) if s not in scammers])
    return scammers


def get_neighbours_and_labels(scammer_address, normal_txs, internal_txs, dataloader,  existing_groups=None):
    if existing_groups is None:
        existing_groups = set()
    eoa_neighbours, contract_neighbours = [], []
    labels = set()
    scammer_neighbours = set()
    normal_accounts = set()
    if (len(normal_txs) + len(internal_txs)) >= Constant.BIG_NODE_TXS:
        labels.add(NodeLabel.BIG)
    if scammer_address.lower() in dataloader.scammers:
        labels.add(NodeLabel.SCAMMER)
    else:
        labels.add(NodeLabel.EOA)
    if normal_txs is not None and len(normal_txs) > 0:
        for tx in normal_txs:
            if tx.is_in_tx(scammer_address) and float(tx.value) > 0:
                eoa_neighbours.append(tx.sender)
                # check if sender is CEX, MIXER, OR BRIDGE
                if tx.sender in (dataloader.bridge_addresses | dataloader.mixer_addresses | dataloader.cex_addresses):
                    labels.add(NodeLabel.WITHDRAWER)
                else:
                    normal_accounts.add(tx.sender)
                if tx.sender in dataloader.scammers:
                    scammer_neighbours.add(tx.sender)
            elif tx.is_out_tx(scammer_address) and tx.is_to_eoa(scammer_address) and float(tx.value) > 0:
                eoa_neighbours.append(tx.to)
                # check if receiver is CEX, MIXER, OR BRIDGE
                if tx.to in (dataloader.bridge_addresses | dataloader.mixer_addresses | dataloader.cex_addresses):
                    labels.add(NodeLabel.DEPOSITOR)
                else:
                    normal_accounts.add(tx.to)
                if tx.to in dataloader.scammers:
                    scammer_neighbours.add(tx.to)
            elif tx.is_out_tx(scammer_address) and tx.is_creation_contract():
                contract_neighbours.append(tx.contractAddress)
            elif tx.is_out_tx(scammer_address) and tx.is_to_contract(scammer_address):
                scammers = get_scammers_list_from_swap_tx(tx, dataloader, existing_groups)
                if scammers is not None and len(scammers) > 0:
                    print("\t\t FOUND A SCAM SWAP ON NEW GROUP")
                    labels.add(NodeLabel.WASHTRADER)
                    eoa_neighbours.extend(scammers)
                    scammer_neighbours.add(scammers[0])  # add presentative only
                    normal_accounts.add(scammers[0])  # add presentative only
    if len(scammer_neighbours) >= 5 and (len(scammer_neighbours) * 1.0 / len(normal_accounts)) > 0.5:
        labels.add(NodeLabel.COORDINATOR)
    if internal_txs is not None and len(internal_txs) > 0:
        # receiver in internal txs of an eoa is always itself
        contract_neighbours.extend([tx.sender for tx in internal_txs])
    eoa_neighbours = OrderedSet(eoa_neighbours)
    contract_neighbours = OrderedSet(contract_neighbours)
    if len(eoa_neighbours) >= 100:
        labels.add(NodeLabel.BIG_CONNECTOR)  # add this label for debugging
    return eoa_neighbours, contract_neighbours, labels
