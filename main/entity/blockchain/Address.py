from typing import Set, Tuple

from aiohttp.web_routedef import static
from enum import Enum
from entity.blockchain.Transaction import InternalTransaction, NormalTransaction
from entity.blockchain.DTO import DTO
from utils import Constant

class SwapDirection(Enum):
    IN = "In"
    OUT = "Out"

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
    def __init__(self, address=None, name=None, symbol=None, supply=None, decimals=None, transfers=None, creator=None, creation_tx=None):
        super().__init__(address)
        self.name = name
        self.symbol = symbol
        self.supply = supply
        self.decimals = decimals
        self.transfers = transfers if transfers is not None else []
        self.creator = creator
        self.creation_tx = creation_tx


class Pool(ERC20):
    def __init__(self, address=None, token0=None, token1=None, scammers=None, mints=None, burns=None, swaps=None, transfers=None, creator=None, creation_tx=None):
        super().__init__(address, "Uniswap V2", "UNI-V2", None, 18, transfers, creator, creation_tx)
        self.token0: Token = token0
        self.token1: Token = token1
        self.scammers = scammers if scammers is not None else []
        self.mints = mints if mints is not None else []
        self.burns = burns if burns is not None else []
        self.swaps = swaps if swaps is not None else []
        self.high_value_token_position = self.get_high_value_position()
        self.scam_token_position = 1 - self.high_value_token_position

    def get_scam_token(self):
        return eval(f"self.token{self.scam_token_position}")

    def get_high_value_position(self):
        if self.token0 is not None and (self.token0.address.lower() in Constant.HIGH_VALUE_TOKENS):
            return 0
        if self.token1 is not None and (self.token1.address.lower() in Constant.HIGH_VALUE_TOKENS):
            return 1
        return -1

    def get_total_mint_value(self):
        mint_total, fee_total = 0, 0
        token: Token = eval(f"self.token{self.high_value_token_position}")
        decimals = int(token.decimals)
        for mint in self.mints:
            mint_total += float(eval(f"mint.amount{self.high_value_token_position}")) / 10 ** decimals
            fee_total += float(mint.gasUsed * mint.gasPrice) / 10 ** Constant.WETH_BNB_DECIMALS
        return mint_total, fee_total

    def get_total_burn_value(self):
        burn_total, fee_total = 0, 0
        token: Token = eval(f"self.token{self.high_value_token_position}")
        decimals = int(token.decimals)
        for burn in self.burns:
            burn_total += float(eval(f"burn.amount{self.high_value_token_position}")) / 10 ** decimals
            fee_total += float(burn.gasUsed * burn.gasPrice) / 10 ** Constant.WETH_BNB_DECIMALS
        return burn_total, fee_total

    def get_total_swap_value(self, addresses: Set[str], direction: SwapDirection) -> Tuple[float, float]:
        """
        Return the total swap amount (buy/sell) by the addresses in the cluster within this pool.
        swap_direction should be either 'In' for buy or 'Out' for sell.
        """
        swap_total, fee_total = 0.0, 0.0
        token: Token = eval(f"self.token{self.high_value_token_position}")
        decimals = int(token.decimals)

        for swap in self.swaps:
            if swap.to.lower() in addresses:
                swap_total += float(eval(f"swap.amount{self.high_value_token_position}{direction.value}")) / 10 ** decimals
                fee_total += float(swap.gasUsed * swap.gasPrice) / 10 ** Constant.WETH_BNB_DECIMALS

        return swap_total, fee_total


class Token(ERC20):
    def __init__(self, address=None, name=None, symbol=None, supply=None, decimals=None, transfers=None, creator=None, creation_tx=None):
        super().__init__(address, name, symbol, supply, decimals, transfers, creator, creation_tx)
