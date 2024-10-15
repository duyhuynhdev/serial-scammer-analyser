from typing import Set, Tuple, Optional

from enum import Enum
from entity.blockchain.Transaction import InternalTransaction, NormalTransaction
from entity.blockchain.DTO import DTO
from utils import Constant


class HighValueTokenNotFound(Exception):
    """Custom exception when a high-value token is not found"""
    pass

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
    def __init__(
            self,
            address=None,
            token0=None,
            token1=None,
            scammers=None,
            mints=None,
            burns=None,
            swaps=None,
            transfers=None,
            creator=None,
            creation_tx=None,
    ):
        super().__init__(
            address, "Uniswap V2", "UNI-V2", None, 18, transfers, creator, creation_tx
        )
        self.token0: str = token0
        self.token1: str = token1
        self.scammers = scammers if scammers is not None else []
        self.mints = mints if mints is not None else []
        self.burns = burns if burns is not None else []
        self.swaps = swaps if swaps is not None else []
        # self.high_value_token_position = self.get_high_value_position()
        # self.scam_token_position = 1 - self.high_value_token_position
        self.x: Optional[float] = None
        self.y: Optional[float] = None
        self.z: Optional[float] = None
        self.profit: Optional[float] = None

    def get_scam_token(self):
        return eval(f"self.token{self.scam_token_position}")

    def get_high_value_position(self) -> int:
        if self.token0 is not None and (
                self.token0.lower() in Constant.HIGH_VALUE_TOKENS
        ):
            return 0
        if self.token1 is not None and (
                self.token1.lower() in Constant.HIGH_VALUE_TOKENS
        ):
            return 1
        raise HighValueTokenNotFound(
            "Neither token0 nor token1 are in HIGH_VALUE_TOKENS."
        )

    def calculate_total_value_and_fees(self, items, amount_attr: str) -> Tuple[float, float]:
        """
        Generic method to calculate the total values and fees from a list of transactions.
        :param items: List of transaction objects (mints, burns, swaps).
        :param amount_attr: The attribute of the item to be evaluated for the amount.
        :return: A tuple of total value and total fees.
        """
        total_value, total_fees = 0, 0
        token: Token = eval(f"self.token{self.high_value_token_position}")
        decimals = int(token.decimals) 
        
        for item in items:
            total_value += float(eval(f"item.{amount_attr}")) / 10 ** decimals
            total_fees += float(item.gasUsed * item.gasPrice) / 10 ** Constant.WETH_BNB_DECIMALS

        return total_value, total_fees


    def calculate_total_mint_value_and_fees(self) -> Tuple[float, float]:
        return self.calculate_total_value_and_fees(self.mints, f"amount{self.high_value_token_position}")

    def calculate_total_burn_value_and_fees(self) -> Tuple[float, float]:
        return self.calculate_total_value_and_fees(self.burns, f"amount{self.high_value_token_position}")

    def calculate_total_swap_value_and_fees(self, addresses: Set[str], direction: SwapDirection) -> Tuple[float, float]:
        filtered_swaps = [swap for swap in self.swaps if swap.to.lower() in addresses]
        return self.calculate_total_value_and_fees(filtered_swaps, f"amount{self.high_value_token_position}{direction.value}")


class Token(ERC20):
    def __init__(self, address=None, name=None, symbol=None, supply=None, decimals=None, transfers=None, creator=None, creation_tx=None):
        super().__init__(address, name, symbol, supply, decimals, transfers, creator, creation_tx)
