import sys
import os
sys.path.append( os.path.join(os.path.dirname(sys.path[0])))
from data_collection.AccountCollector import TransactionCollector
from data_collection.DataDecoder import FunctionInputDecoder
from utils import Constant
from utils.DataLoader import DataLoader


class LightNodeLabel:
    SCAMMER = "scammer"  # 1d rug pull scammer
    DEPOSITOR = "depositor"  # send money to defi node
    WITHDRAWER = "withdrawer"  # receive money from defi node
    COORDINATOR = "coordinator"  # manage scammers
    TRANSFER = "transfer"  # only receive and transfer money
    WASHTRADER = "washtrader"  # washtrade scam token
    BOUNDARY = "boundary"  # grey node
    BIG_CONNECTOR = "big_connector"


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
        valid_neighbours = data['valid_neighbours'].split(';') if 'valid_neighbours' in data else []
        labels = data['labels'].split(';') if 'labels' in data else []
        path = data['path'].split(';') if 'path' in data else []
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
    def __init__(self, dataloader = None, dex = "univ2"):
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

    def ensure_eoa_address(self, address):
        normal_txs, internal_txs = self.transaction_collector.get_transactions(address, self.dex)
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
                        is_swap = False # turn of if swap out
        return is_swap, scammers

    def get_valid_neighbours(self, address, normal_txs):
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
                  and self.ensure_eoa_address(tx.to)):
                valid_neighbours.append(tx.to)
            elif tx.is_out_tx(address) and tx.is_contract_call_tx():
                is_swap, scammers = self.get_scammer_if_swap_tx(tx)
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
                        is_swap_in, scammers = self.get_scammer_if_swap_tx(tx)
                        if is_swap_in:
                            swap_in_txs.append(tx)
                        if len(scammers) > 0:
                            scam_swap_in_txs.append(tx)
        return (scam_neighbours,
                eoa_neighbours,
                contract_neighbours,
                to_cex_txs,
                from_cex_txs,
                swap_in_txs,
                scam_swap_in_txs,
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
         transfer_txs,
         true_in_value,
         true_out_value) = self.categorise_normal_transaction(address, normal_txs)
        # print("sb", len(scam_neighbours), "swap_txs", len(swap_in_txs), "scam_swap_txs", len(scam_swap_in_txs))
        if self.is_scammer_address(address):
            labels.add(LightNodeLabel.SCAMMER)
        if len(to_cex_txs) > 0:
            labels.add(LightNodeLabel.DEPOSITOR)
        if len(from_cex_txs) > 0:
            labels.add(LightNodeLabel.WITHDRAWER)
        if len(scam_swap_in_txs) > 0:
            labels.add(LightNodeLabel.WASHTRADER)
        if len(eoa_neighbours) > 0 and len(scam_neighbours) / len(eoa_neighbours) > 0.5:
            labels.add(LightNodeLabel.COORDINATOR)
        if (LightNodeLabel.SCAMMER not in labels
                and LightNodeLabel.COORDINATOR not in labels
                and len(swap_in_txs) >= 5
                and len(scam_swap_in_txs) / len(swap_in_txs) < 0.5):
            labels.add(LightNodeLabel.BOUNDARY)
        if (len(contract_neighbours) == 0
                and len(internal_txs) == 0
                and len(transfer_txs) == len(normal_txs)
                and true_out_value > 0
                and true_out_value / true_in_value >= 0.99):
            labels.add(LightNodeLabel.TRANSFER)
        return labels

    def create(self, address, parent_path):
        normal_txs, internal_txs = self.transaction_collector.get_transactions(address, self.dex)
        print("\t\t CREATE NODE FOR ", address, " WITH NORMAL TX:", len(normal_txs) if normal_txs is not None else 0, "AND INTERNAL TX:", len(internal_txs) if internal_txs is not None else 0)
        labels = self.get_node_labels(address, normal_txs, internal_txs)
        valid_neighbours = []
        # Skip verify neighbours if the node is boundary node
        if LightNodeLabel.BOUNDARY not in labels:
            valid_neighbours = self.get_valid_neighbours(address, normal_txs)
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
    addresses = [
        "0x17ef03d1f9b53123aba4a9d67fe579b10dbf59d4",
        "0x68bec4525eb557f48c30eefec249b1d85706cf0c",
        "0x4502f3bca6ec0aa547ffb0f2b166e0d82c01f856",
        "0xb2cd290b0f0ddfd07fe19f8f5aa1474a51caf922",
        "0x50274a83511af15b94c1777fff23ee5818d0b19d",
        "0xb5fceb9eb50f2c95c18600dc1c02232e61068b29",
        "0xf0e2eb33510dd3fd61ad7c449effe1e5215e0345",
        "0x4363abdadf700814636b86ac67c4bb487ce6b63c",
        "0x81111eeef086ed668754bb36d4eed08f5026ce48",
        "0xd75e638fbe9c9c3a3a03c84250ccf17a9ca1e7f6",
        "0x2da7697b6cbcc8c40bb2d267b0bb0835543877b5",
        "0x5736bbb2247546214086993945c3d0e46a285d21",
        "0x1ba8732c2de697854243713ec53995280c5fd369",
        "0x6f7d7efd37d01574980542662c9ac1b4e8dd66f6",
    ]
    # for address in addresses:
    #     node = factory.create(address, [])
    #     print(address,":",node.labels)

    # 0xb2cd290b0f0ddfd07fe19f8f5aa1474a51caf922
    node = factory.create("0xb2cd290b0f0ddfd07fe19f8f5aa1474a51caf922", [])
    print("0xb2cd290b0f0ddfd07fe19f8f5aa1474a51caf922",":",node.labels)