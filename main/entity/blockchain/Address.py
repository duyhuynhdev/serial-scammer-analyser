from entity.blockchain.Event import MintEvent, BurnEvent, SwapEvent, TransferEvent
from entity.blockchain.Transaction import InternalTransaction, NormalTransaction
from entity.blockchain.DTO import DTO
from utils import Constant


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
    def __init__(self, address=None, token0=None, token1=None, scammers=None, mints=None, burns=None, swaps=None, transfers=None):
        super().__init__(address, "Uniswap V2", "UNI-V2", None, 18, transfers)
        self.token0: Token = token0
        self.token1: Token = token1
        self.scammers = scammers if scammers is not None else []
        self.mints = mints if mints is not None else []
        self.burns = burns if burns is not None else []
        self.swaps = swaps if swaps is not None else []

    def get_high_value_position(self):
        if self.token0 is not None and (self.token0.address.lower() in Constant.HIGH_VALUE_TOKENS):
            return 0
        if self.token1 is not None and (self.token1.address.lower() in Constant.HIGH_VALUE_TOKENS):
            return 1
        return -1

    def get_total_mint_value(self, position):
        mint_total, fee_total = 0, 0
        token: Token = eval(f"self.token{position}")
        decimals = int(token.decimals)
        for mint in self.mints:
            mint_total += float(eval(f"mint.amount{position}")) / 10 ** decimals
            fee_total += float(mint.gasUsed * mint.gasPrice) / 10 ** 18
        return mint_total, fee_total

    def get_total_burn_value(self, position):
        burn_total, fee_total = 0, 0
        token: Token = eval(f"self.token{position}")
        decimals = int(token.decimals)
        for burn in self.burns:
            burn_total += float(eval(f"burn.amount{position}")) / 10 ** decimals
            fee_total += float(burn.gasUsed * burn.gasPrice) / 10 ** 18
        return burn_total, fee_total

    def get_max_swap_value(self, position):
        max_swap, swap_fee = 0, 0
        token: Token = eval(f"self.token{position}")
        decimals = int(token.decimals)
        for swap in self.swaps:
            if float(eval(f"swap.amount{position}Out")) > max_swap:
                max_swap = float(eval(f"swap.amount{position}Out")) / 10 ** decimals
                swap_fee = float(swap.gasUsed * swap.gasPrice) / 10 ** 18
        return max_swap, swap_fee

    def get_swap_in_value(self, position, address):
        swap_in_total, fee_total = 0, 0
        token: Token = eval(f"self.token{position}")
        decimals = int(token.decimals)
        for swap in self.swaps:
            if swap.to.lower() == address.lower():
                swap_in_total += float(eval(f"swap.amount{position}In")) / 10 ** decimals
                fee_total += float(swap.gasUsed * swap.gasPrice) / 10 ** 18
        return swap_in_total, fee_total

    def get_swap_out_value(self, position, address):
        swap_out_total, fee_total = 0, 0
        token: Token = eval(f"self.token{position}")
        decimals = int(token.decimals)
        for swap in self.swaps:
            if swap.to.lower() == address.lower():
                swap_out_total += float(eval(f"swap.amount{position}Out")) / 10 ** decimals
                fee_total += float(swap.gasUsed * swap.gasPrice) / 10 ** 18
        return swap_out_total, fee_total


class Token(ERC20):
    def __init__(self, address=None, name=None, symbol=None, supply=None, decimals=None, transfers=None):
        super().__init__(address, name, symbol, supply, decimals, transfers)
