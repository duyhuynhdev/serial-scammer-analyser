from typing import Set, Tuple, List, Optional
from functools import cached_property
from typing import Set, Tuple, List, Optional

from enum import Enum

from entity.blockchain.Event import Event, SwapEvent
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
    def __init__(
        self,
        address=None,
        normal_transactions: [NormalTransaction] = None,
        internal_transactions: [InternalTransaction] = None,
    ):
        super().__init__(address, AddressType.eoa)
        self.normal_transactions = (
            normal_transactions if normal_transactions is not None else []
        )
        self.internal_transactions = (
            internal_transactions if internal_transactions is not None else []
        )


class Contract(Address):
    def __init__(self, address=None):
        super().__init__(address, AddressType.contract)


class ERC20(Contract):
    def __init__(
        self,
        address=None,
        name=None,
        symbol=None,
        supply=None,
        decimals=None,
        transfers=None,
        creator=None,
        creation_tx=None,
    ):
        super().__init__(address)
        self.name = name
        self.symbol = symbol
        self.supply = supply
        self.decimals = decimals
        self.transfers = transfers if transfers is not None else []
        self.creator = creator
        self.creation_tx = creation_tx


class Token(ERC20):
    def __init__(
        self,
        address=None,
        name=None,
        symbol=None,
        supply=None,
        decimals=None,
        transfers=None,
        creator=None,
        creation_tx=None,
    ):
        super().__init__(
            address, name, symbol, supply, decimals, transfers, creator, creation_tx
        )


class Pool(ERC20):
    POOL_DECIMALS = 18

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
        self.x: Optional[float] = None
        self.y: Optional[float] = None
        self.z: Optional[float] = None
        self.profit: Optional[float] = None

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

    @cached_property
    def high_value_token_position(self) -> int:
        return self.get_high_value_position()

    @cached_property
    def scam_token_position(self) -> int:
        return 1 - self.high_value_token_position

    @cached_property
    def investing_node_addresses(self) -> Set[str]:
        """
        Returns all node addresses that invested the high-value token into the pool,
        including both scam cluster nodes and legitimate investor nodes.
        """

        return set(
            swap.to.lower()
            for swap in self.swaps
            if getattr(swap, self.investing_amount_attr) > 0
        )

    @cached_property
    def investing_swaps(self) -> List[SwapEvent]:
        """
        Returns all swap transactions where the high-value token is used to purchase the scam token,
        including all the purchases made by both scam cluster nodes and legitimate investor nodes.
        """

        return [
            swap for swap in self.swaps if getattr(swap, self.investing_amount_attr) > 0
        ]

    @cached_property
    def investing_amount_attr(self) -> str:
        return f"amount{self.high_value_token_position}{SwapDirection.IN.value}"

    @cached_property
    def divesting_swaps(self) -> List[SwapEvent]:
        """
        Returns all swap transactions where the high-value token is withdrawn, including all the
        withdrawals made by both scam cluster nodes and legitimate investor nodes.
        """

        return [
            swap for swap in self.swaps if getattr(swap, self.divesting_amount_attr) > 0
        ]

    @cached_property
    def divesting_amount_attr(self) -> str:
        return f"amount{self.high_value_token_position}{SwapDirection.OUT.value}"

    @cached_property
    def scam_token_address(self) -> str:
        return eval(f"self.token{self.scam_token_position}").lower()

    @cached_property
    def high_value_token_address(self) -> str:
        return eval(f"self.token{self.high_value_token_position}").lower()

    def calculate_total_value_and_fees(
        self, items: List[Event], amount_attr: str
    ) -> Tuple[float, float]:
        """
        Generic method to calculate the total values and fees from a list of transactions.
        :param items: List of transaction objects (mints, burns, swaps).
        :param amount_attr: The attribute of the item to be evaluated for the amount.
        :return: A tuple of total value and total fees.
        """
        total_value, total_fees = 0, 0

        for item in items:
            total_value += float(getattr(item, amount_attr)) / 10**self.POOL_DECIMALS
            total_fees += (
                float(item.gasUsed)
                * float(item.gasPrice)
                / 10**Constant.WETH_BNB_DECIMALS
            )

        return total_value, total_fees

    def calculate_total_mint_value_and_fees(self) -> Tuple[float, float]:
        return self.calculate_total_value_and_fees(
            self.mints, f"amount" f"{self.high_value_token_position}"
        )

    def calculate_total_burn_value_and_fees(self) -> Tuple[float, float]:
        return self.calculate_total_value_and_fees(
            self.burns, f"amount" f"{self.high_value_token_position}"
        )

    def calculate_total_investing_value_and_fees_by_addressees(
        self, addresses: Set[str]
    ) -> Tuple[float, float]:
        filtered_investing_swaps = [
            swap for swap in self.investing_swaps if swap.to.lower() in addresses
        ]
        return self.calculate_total_value_and_fees(
            filtered_investing_swaps, self.investing_amount_attr
        )

    def calculate_total_divesting_value_and_fees_by_addressees(
        self, addresses: Set[str]
    ) -> Tuple[float, float]:
        filtered_divesting_swaps = [
            swap for swap in self.divesting_swaps if swap.to.lower() in addresses
        ]
        return self.calculate_total_value_and_fees(
            filtered_divesting_swaps, self.divesting_amount_attr
        )
