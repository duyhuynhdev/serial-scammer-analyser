from entity.blockchain.Transaction import InternalTransaction, NormalTransaction
from entity.blockchain.DTO import DTO

class AddressType:
    eoa = "EOA"
    contract = "Contract"


class Address(DTO):
    def __init__(self, address=None, type=None):
        super().__init__()
        self.address = address
        self.type = type


class Account(Address):
    def __init__(self, address=None, normal_transactions: [NormalTransaction] = None, internal_transactions: [InternalTransaction] = None):
        super().__init__(address, AddressType.eoa)
        self.normal_transactions = normal_transactions if normal_transactions is not None else []
        self.internal_transactions = internal_transactions if internal_transactions is not None else []


class Contract(Address):
    def __init__(self, address=None):
        super().__init__(address, AddressType.contract)


class ERC20(Contract):
    def __init__(self, address=None, name=None, symbol=None, supply=None, decimals=None, transfers=None):
        super().__init__(address)
        self.name = name
        self.symbol = symbol
        self.supply = supply
        self.decimals = decimals
        self.transfers = transfers if transfers is not None else []


class Pool(ERC20):
    def __init__(self, address=None, token0: ERC20 = None, token1: ERC20 = None, scammers=None, syncs=None, burns=None, swaps=None, transfers=None):
        super().__init__(address, "Uniswap V2", "UNI-V2", None, 18, transfers)
        self.token0 = token0
        self.token1 = token1
        self.scammers = scammers if scammers is not None else []
        self.syncs = syncs if syncs is not None else []
        self.burns = burns if burns is not None else []
        self.swaps = swaps if swaps is not None else []


class Token(ERC20):
    def __init__(self,  address=None, name=None, symbol=None, supply=None, decimals=None, transfers=None ):
        super().__init__(address, name, symbol, supply, decimals, transfers)
