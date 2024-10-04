import sys
import os
from tkinter.font import NORMAL

sys.path.append(os.path.join(os.path.dirname(sys.path[0])))
from data_collection.AccountCollector import TransactionCollector
from data_collection.DataDecoder import FunctionInputDecoder
from utils import Constant
from utils import Utils as ut
from utils.DataLoader import DataLoader


class LightNodeLabel:
    SCAMMER = "scammer"  # 1d rug pull scammer
    DEPOSITOR = "depositor"  # send money to defi node
    WITHDRAWER = "withdrawer"  # receive money from defi node
    COORDINATOR = "coordinator"  # manage scammers
    TRANSFER = "transfer"  # only receive and transfer money
    BIG_TRANSFER = "big_transfer"  # only receive and transfer money
    WASHTRADER = "washtrader"  # washtrade scam token
    BIG_WASHTRADER = "big_washtrader"  # washtrade scam token
    BOUNDARY = "boundary"  # grey node

    ### DEBUGGING LABELS
    BIG_CONNECTOR = "big_connector"
    PHISHING = "PHISHING"
    OVER_LIMIT_1 = "over_limit_1"
    OVER_LIMIT_2 = "over_limit_2"
    OVER_LIMIT_2_BUT_ACCEPT = "over_limit_2_but_accept"
    SKIP_LABELS = [BOUNDARY, OVER_LIMIT_1, OVER_LIMIT_2, BIG_TRANSFER]
    ACCEPT_LIMIT_2_LABELS = [SCAMMER, COORDINATOR, BIG_WASHTRADER]


class LightNode:
    def __init__(self, address, valid_neighbours, normal_txs_len, labels, path, normal_txs=None):
        self.address = address
        self.valid_neighbours = valid_neighbours
        self.normal_txs_len = normal_txs_len
        self.labels = labels
        self.path = path
        self.normal_txs = normal_txs

    @staticmethod
    def from_dict(data):
        address = data['address']
        normal_txs_len = data['normal_txs_len'] if 'normal_txs_len' in data else None
        valid_neighbours = data['valid_neighbours'].split(';') if 'valid_neighbours' in data and not ut.is_df_cell_is_empty(data['valid_neighbours']) else []
        labels = data['labels'].split(';') if 'labels' in data and not ut.is_df_cell_is_empty(data['labels']) else []
        path = data['path'].split(';') if 'path' in data and not ut.is_df_cell_is_empty(data['path']) else []
        return LightNode(address, valid_neighbours, normal_txs_len, labels, path)

    @staticmethod
    def to_sort_dict(node):
        return {
            'address': node.address,
            'valid_neighbours': len(node.valid_neighbours),
            'normal_txs_len': node.normal_txs_len,
            'labels': ';'.join(node.labels),
        }


class LightNodeFactory:
    def __init__(self, dataloader=None, dex="univ2"):
        self.dex = dex
        self.dataloader = dataloader if dataloader is not None else DataLoader(dex)
        self.decoder = FunctionInputDecoder()
        self.transaction_collector = TransactionCollector()
        self.public_exchange_addresses = (dataloader.bridge_addresses | dataloader.cex_addresses | dataloader.mixer_addresses)
        self.bots = dataloader.MEV_addresses
        self.application_address = (dataloader.defi_addresses | dataloader.wallet_addresses | dataloader.other_addresses)

    def is_scammer_address(self, address):
        return address.lower() in self.dataloader.scammers

    def is_public_address(self, address):
        return address.lower() in self.public_exchange_addresses or address.lower() in self.bots or address.lower() in self.application_address

    def ensure_valid_eoa_address(self, address, cluster_id):
        normal_txs, internal_txs = self.transaction_collector.get_transactions(address, self.dex, cluster_id)
        if len(normal_txs) >= Constant.TX_LIMIT_1:
            return False
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

    def get_scammer_if_swap_tx(self, tx):
        is_swap, parsed_inputs = self.decoder.decode_function_input(tx.input)
        scammers = list()
        scam_pool = None
        out_token = None
        if (parsed_inputs is not None) and (len(parsed_inputs) > 0):
            # get path inputs from parsed inputs
            paths = [pi["path"] for pi in parsed_inputs if "path" in pi.keys()]
            # Get all tokens from paths (each path contains 2 tokens)
            # The first element of path is the input token, the last is the output token
            # Hence if path [0] is HV token -> the swap is swap in
            for path in paths:
                if len(path) == 2:
                    in_token, out_token = path[0].lower(), path[1].lower()
                    if in_token in Constant.HIGH_VALUE_TOKENS and out_token in self.dataloader.scam_token_pool.keys():
                        scam_pool = self.dataloader.scam_token_pool[out_token]
                        if scam_pool and scam_pool in self.dataloader.pool_scammers.keys():
                            scammers = self.dataloader.pool_scammers[scam_pool]
                            scammers.extend([s for s in scammers if s not in scammers])
                    else:
                        is_swap = False  # turn of if swap out
        return is_swap, scammers, scam_pool, out_token

    def get_valid_neighbours(self, address, normal_txs, cluster_id):
        valid_neighbours = []
        for tx in normal_txs:
            if (tx.is_in_tx(address)
                    and float(tx.value) > 0
                    and not self.is_public_address(tx.sender)
                    and tx.sender not in valid_neighbours):
                valid_neighbours.append(tx.sender)
            elif (tx.is_to_eoa(address)
                  and float(tx.value) > 0
                  and tx.to not in valid_neighbours
                  and not self.is_public_address(tx.to)
                  and self.ensure_valid_eoa_address(tx.to, cluster_id)):
                valid_neighbours.append(tx.to)
            elif tx.is_out_tx(address) and tx.is_contract_call_tx():
                is_swap, scammers, _, _ = self.get_scammer_if_swap_tx(tx)
                valid_neighbours.extend([s for s in scammers if s not in valid_neighbours])
        return valid_neighbours

    def categorise_normal_transaction(self, address, normal_txs):
        scam_neighbours = []
        eoa_neighbours = []
        contract_neighbours = []
        to_cex_txs = []
        from_cex_txs = []
        swap_in_txs = []
        scam_swap_in_txs = []
        scam_pools = []
        scam_tokens = []
        transfer_txs = []
        true_in_value = 0
        true_out_value = 0
        for tx in normal_txs:
            if tx.is_in_tx(address) and float(tx.value) > 0:
                eoa_neighbours.append(tx.sender)
                if tx.sender.lower() in self.public_exchange_addresses:
                    from_cex_txs.append(tx)
                if self.is_scammer_address(tx.sender):
                    scam_neighbours.append(tx.sender)
                if tx.is_transfer_tx():
                    transfer_txs.append(tx)
                    true_in_value += tx.get_true_transfer_amount(address)
            elif tx.is_out_tx(address):
                if tx.to.lower() in self.public_exchange_addresses:
                    to_cex_txs.append(tx)
                if tx.is_transfer_tx() and float(tx.value) > 0:
                    transfer_txs.append(tx)
                    true_out_value += tx.get_true_transfer_amount(address)
                if tx.is_to_eoa(address) and float(tx.value) > 0:
                    eoa_neighbours.append(tx.to)
                    if self.is_scammer_address(tx.to):
                        scam_neighbours.append(tx.to)
                else:
                    contract_neighbours.append(tx.to)
                    if tx.is_contract_call_tx():
                        contract_neighbours.append(tx.to)
                        is_swap_in, scammers, scam_pool, scam_token = self.get_scammer_if_swap_tx(tx)
                        if is_swap_in:
                            swap_in_txs.append(tx)
                        if len(scammers) > 0:
                            scam_pools.append(scam_pool)
                            scam_tokens.append(scam_token)
                            scam_swap_in_txs.append(tx)
        return (scam_neighbours,
                eoa_neighbours,
                contract_neighbours,
                to_cex_txs,
                from_cex_txs,
                swap_in_txs,
                scam_swap_in_txs,
                scam_pools,
                scam_tokens,
                transfer_txs,
                true_in_value,
                true_out_value)

    def get_node_labels(self, address, normal_txs, internal_txs):
        labels = set()
        (scam_neighbours,
         eoa_neighbours,
         contract_neighbours,
         to_cex_txs,
         from_cex_txs,
         swap_in_txs,
         scam_swap_in_txs,
         scam_pools,
         scam_tokens,
         transfer_txs,
         true_in_value,
         true_out_value) = self.categorise_normal_transaction(address, normal_txs)
        # print("sb", len(scam_neighbours), "sb_rate", len(scam_neighbours) / len(eoa_neighbours), "swap_txs", len(swap_in_txs), "scam_swap_txs", len(scam_swap_in_txs))
        # print("scam_pools", len(scam_pools))
        # print("unique_pools", set(scam_pools))
        # print("scam_tokens", len(scam_tokens))
        # print("unique_tokens", set(scam_tokens))
        ## MAIN LABELS
        if self.is_scammer_address(address):
            labels.add(LightNodeLabel.SCAMMER)
        if len(to_cex_txs) > 0:
            labels.add(LightNodeLabel.DEPOSITOR)
        if len(from_cex_txs) > 0:
            labels.add(LightNodeLabel.WITHDRAWER)
        if len(scam_swap_in_txs) > 0:
            if len(swap_in_txs) >= Constant.BIG_WT_SWAP and len(scam_swap_in_txs) / len(swap_in_txs) >= Constant.BIG_WT_SCAM_SWAP_RATE:
                labels.add(LightNodeLabel.BIG_WASHTRADER)
                # print("scam_swap_in_txs = ", len(scam_swap_in_txs), ", swap_in_txs = ", len(swap_in_txs), "\n")
                # print("scam tokens = ", scam_tokens,"\n")
            else:
                labels.add(LightNodeLabel.WASHTRADER)
        if len(scam_neighbours) >= Constant.COORDINATOR_SCAM_NEIGHBOUR and len(scam_neighbours) / len(eoa_neighbours) > Constant.COORDINATOR_SCAM_NEIGHBOUR_RATE:
            labels.add(LightNodeLabel.COORDINATOR)
        #if (LightNodeLabel.SCAMMER not in labels
        #        and LightNodeLabel.COORDINATOR not in labels
        #        and len(swap_in_txs) >= Constant.BOUNDARY_SWAP
        #        and len(scam_swap_in_txs) / len(swap_in_txs) < Constant.BOUNDARY_SCAM_SWAP_RATE):
        #    labels.add(LightNodeLabel.BOUNDARY)
        if ((len(swap_in_txs) >= Constant.BOUNDARY_SWAP) and (LightNodeLabel.SCAMMER not in labels)
                and (LightNodeLabel.COORDINATOR not in labels)):
            if len(scam_swap_in_txs) / len(swap_in_txs) < Constant.BOUNDARY_SCAM_SWAP_RATE:
                labels.add(LightNodeLabel.BOUNDARY)
                #        and
                #        and len(scam_swap_in_txs) / len(swap_in_txs) < Constant.BOUNDARY_SCAM_SWAP_RATE):
                #    labels.add(LightNodeLabel.BOUNDARY)
        if (len(contract_neighbours) == 0
                and len(internal_txs) == 0
                and len(transfer_txs) == len(normal_txs)
                and true_out_value > 0
                and true_out_value / true_in_value >= Constant.TRANSFER_TRUE_VALUE_RATE):
            if len(normal_txs) > Constant.TRANSFER_LIMIT:
                labels.add(LightNodeLabel.BIG_TRANSFER)
            else:
                labels.add(LightNodeLabel.TRANSFER)

        ## LIMIT LABELS:
        if len(normal_txs) >= Constant.TX_LIMIT_1:
            labels.add(LightNodeLabel.OVER_LIMIT_1)
        elif len(normal_txs) >= Constant.TX_LIMIT_2:
            if any(label in LightNodeLabel.ACCEPT_LIMIT_2_LABELS for label in labels):
                labels.add(LightNodeLabel.OVER_LIMIT_2_BUT_ACCEPT)
            else:
                labels.add(LightNodeLabel.OVER_LIMIT_2)

        return labels

    def createNode(self, address, parent_path, cluster_id):
        normal_txs, internal_txs = self.transaction_collector.get_transactions(address, self.dex, cluster_id)
        print("\t\t CREATE NODE FOR ", address, " WITH NORMAL TX:", len(normal_txs) if normal_txs is not None else 0, "AND INTERNAL TX:", len(internal_txs) if internal_txs is not None else 0)
        labels = self.get_node_labels(address, normal_txs, internal_txs)
        print("\t\t LABELS", labels)
        valid_neighbours = []
        # Skip verify neighbours if the node is boundary node
        if not any(label in LightNodeLabel.SKIP_LABELS for label in labels):
            valid_neighbours = self.get_valid_neighbours(address, normal_txs, cluster_id)
            if len(valid_neighbours) > 50:
                labels.add(LightNodeLabel.BIG_CONNECTOR)
        path = parent_path.copy() if parent_path is not None else []
        path.append(address)
        return LightNode(address, valid_neighbours, len(normal_txs), labels, path, normal_txs)


if __name__ == '__main__':
    dataloader = DataLoader()
    factory = LightNodeFactory(dataloader)
    # address = sys.argv[1]
    # node = factory.create(address, [])
    # addresses = [
    #     "0xcc3337f6fe821a7a654552395e128e2c4fcabe31",
    #     "0x68bec4525eb557f48c30eefec249b1d85706cf0c",
    #     "0xb2cd290b0f0ddfd07fe19f8f5aa1474a51caf922",
    #     "0x4363abdadf700814636b86ac67c4bb487ce6b63c",
    #     "0x4b2b31b7a32a3d3bb894effc31b16abd400bd258",
    #     "0xf3451414eb22c20d4cfaf84ea913f0a958bd169b",
    #     "0x12d23712617b425e1a009a0a5fb7502a9d5a328e",
    #     "0x660b143181cd5667fbd4e2f498adfb7dc61b1c9c",
    #     "0x32967f449e4a8824a02f1b35598a02586e7bb7e6",
    #     "0xf0a5e9fa56e3b4a21bdaddeb80a0287ecfdccd99",
    #     "0x4e756eb534caf866f911117f9c5b9927c11927cf",
    #     "0x015a3c7587e43cd7fc99ace46f8aed1320bdd074",
    #     "0xbdab033b757de1484ce5d566f123f91ae9055484",
    #     "0xfa19e5e2ca003c0195859b10296867a2a10514f9",
    #     "0x4d103648cb1ca7f0b053e388f7ff275a5b4c020b",
    #     "0x527721e999560c8652ada7d6659a54b5a004ea47",
    #     "0xcb6be648529545a9548ef09a0c721c83b01f800f",
    #     "0xd16a4e051f6bed58eb87345dea8729a88e6bc758",
    # ]
    # for address in addresses:
    #     node = factory.create(address, [])
    #     print(address,":",node.labels)

    node = factory.createNode("0xe83d96cc5c7e2c3cb1959a676c7a34f5b535730f", [], 3)
    print("0xe83d96cc5c7e2c3cb1959a676c7a34f5b535730f", ":", node.labels)
