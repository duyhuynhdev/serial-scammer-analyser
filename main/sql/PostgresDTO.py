import math
from typing import List, Optional
import sqlalchemy
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy import create_engine
from web3 import Web3

from entity.blockchain import Event, Transaction
from utils import Constant


class Base(DeclarativeBase):
    pass


class Address(Base):
    __tablename__ = "address"
    address: Mapped[str] = mapped_column(primary_key=True)
    is_contract: Mapped[bool]
    is_erc20: Mapped[bool]
    is_pool: Mapped[bool]
    is_public_service: Mapped[bool]
    service_type: Mapped[Optional[str]]


class Token(Base):
    __tablename__ = "token"
    address: Mapped[str] = mapped_column(primary_key=True)
    creator: Mapped[str] = mapped_column(index=True)
    name: Mapped[Optional[str]]
    symbol: Mapped[Optional[str]]
    supply: Mapped[Optional[float]]
    decimals: Mapped[Optional[int]]
    is_malicious: Mapped[bool]


class Pool(Base):
    __tablename__ = "pool"
    address: Mapped[str] = mapped_column(primary_key=True)
    creator: Mapped[str] = mapped_column(index=True)
    token0: Mapped[str] = mapped_column(index=True)
    token1: Mapped[str] = mapped_column(index=True)
    is_malicious: Mapped[bool]

    def get_high_value_position(self) -> int:
        if self.token0 is not None and (
                self.token0.lower() in Constant.HIGH_VALUE_TOKENS
        ):
            return 0
        if self.token1 is not None and (
                self.token1.lower() in Constant.HIGH_VALUE_TOKENS
        ):
            return 1
        raise Exception(
            "Neither token0 nor token1 are in HIGH_VALUE_TOKENS."
        )

    def scam_token_position(self) -> int:
        return 1 - self.get_high_value_position()

    def get_scam_token_address(self):
        return eval(f"self.token{self.scam_token_position()}").lower()


class NormalTransaction(Base):
    __tablename__ = "normal_transaction"
    hash: Mapped[str] = mapped_column(primary_key=True)
    block_number: Mapped[int] = mapped_column(index=True)
    input: Mapped[str]
    timestamp: Mapped[int] = mapped_column(index=True)
    value: Mapped[float]
    is_contract_creation: Mapped[bool] = mapped_column(index=True)
    gas: Mapped[float]
    gas_used: Mapped[float]
    is_error: Mapped[bool]
    function_name: Mapped[str]
    method_id: Mapped[str]
    gas_price: Mapped[float]
    cumulative_gas_used: Mapped[float]
    sender: Mapped[str] = mapped_column(index=True)
    receiver: Mapped[str] = mapped_column(index=True)

    def get_transaction_amount(self):
        if self.is_error:
            return 0
        return float(self.value) / 10 ** Constant.WETH_BNB_DECIMALS

    def is_not_error(self):
        return not self.is_error

    def is_to_empty(self):
        return not self.receiver or (isinstance(self.receiver, float) and math.isnan(self.receiver))

    def is_creation_contract_tx(self):
        return self.is_to_empty()

    def is_in_tx(self, owner):
        if self.is_creation_contract_tx():
            return False
        try:
            to = Web3.to_checksum_address(self.receiver)
            owner = Web3.to_checksum_address(owner)
        except Exception as e:
            return False
        return to == owner

    def is_out_tx(self, owner):
        if self.is_creation_contract_tx():
            return False
        try:
            sender = Web3.to_checksum_address(self.sender)
            owner = Web3.to_checksum_address(owner)
        except Exception as e:
            return False
        return sender == owner

    def is_function_empty(self):
        return (
                isinstance(self.function_name, float) and math.isnan(self.function_name)
        ) or not self.function_name or self.function_name == "NaN"

    def is_transfer_tx(self):
        return self.is_function_empty() and not self.is_contract_creation

    def is_contract_call_tx(self):
        return not self.is_transfer_tx()

    def is_to_eoa(self, owner):
        return (
                self.is_out_tx(owner)
                and self.is_function_empty()
                and not self.is_to_empty()
        )

    def is_to_contract(self, owner):
        return (
                self.is_out_tx(owner)
                and not self.is_function_empty()
                and not self.is_to_empty()
        )

    def get_transaction_fee(self):
        if self.is_error:
            return 0
        return (
                float(self.gas_price) * float(self.gas_used) / 10 ** Constant.WETH_BNB_DECIMALS
        )

    def get_transaction_amount_and_fee(self):
        return self.get_transaction_amount() + self.get_transaction_fee()

    def get_true_transfer_amount(self, address):
        if self.is_in_tx(address):
            return self.get_transaction_amount()
        if self.is_out_tx(address):
            return self.get_transaction_amount() + self.get_transaction_fee()
        return 0

    def to_sql_object(self, input: Transaction.NormalTransaction):
        return NormalTransaction(
            **{
                "hash": input.hash,
                "block_number": input.blockNumber,
                "input": input.input,
                "timestamp": input.timeStamp,
                "value": input.value,
                "gas": input.gas,
                "gas_used": input.gasUsed,
                "is_error": input.isError,
                "function_name": input.functionName,
                "method_id": input.methodId,
                "gas_price": input.gasPrice,
                "cumulative_gas_used": input.cumulativeGasUsed,
                "sender": input.sender,
                "receiver": input.to if isinstance(input.to, str) and len(input.to) > 0 else input.contractAddress,
                "is_contract_creation": False if isinstance(input.to, str) and len(input.to) > 0 else True,
            }
        )


class InternalTransaction(Base):
    __tablename__ = "internal_transaction"
    hash: Mapped[str] = mapped_column(primary_key=True)
    trace_id: Mapped[str] = mapped_column(primary_key=True)
    block_number: Mapped[int] = mapped_column(index=True)
    input: Mapped[str]
    timestamp: Mapped[int] = mapped_column(index=True)
    value: Mapped[float]
    is_contract_creation: Mapped[bool]
    gas: Mapped[float]
    gas_used: Mapped[float]
    is_error: Mapped[bool]
    type: Mapped[str]
    err_code: Mapped[float]
    sender: Mapped[str] = mapped_column(index=True)
    receiver: Mapped[str] = mapped_column(index=True)

    def get_transaction_amount(self):
        if self.is_error:
            return 0
        return float(self.value) / 10 ** Constant.WETH_BNB_DECIMALS

    def is_error(self):
        return self.is_error

    def is_not_error(self):
        return not self.is_error()

    # def is_to_empty(self):
    #     return not self.receiver or (isinstance(self.receiver, float) and math.isnan(self.receiver))

    def is_creation_contract_tx(self):
        return self.is_contract_creation

    def is_in_tx(self, owner):
        if self.is_creation_contract_tx():
            return False
        try:
            to = Web3.to_checksum_address(self.receiver)
            owner = Web3.to_checksum_address(owner)
        except Exception as e:
            return False
        return to == owner

    def is_out_tx(self, owner):
        if self.is_creation_contract_tx():
            return False
        try:
            sender = Web3.to_checksum_address(self.sender)
            owner = Web3.to_checksum_address(owner)
        except Exception as e:
            return False
        return sender == owner

    def to_sql_object(self, input: Transaction.InternalTransaction):
        return InternalTransaction(
            **{
                "hash": input.hash,
                "trace_id": input.traceId,
                "block_number": input.blockNumber,
                "input": input.input,
                "timestamp": input.timeStamp,
                "value": input.value,
                "gas": input.gas,
                "gas_used": input.gasUsed,
                "is_error": input.isError,
                "type": input.type,
                "err_code": input.errCode,
                "sender": input.sender,
                "receiver": input.to if isinstance(input.to, str) and len(input.to) > 0 else input.contractAddress,
                "is_contract_creation": False if isinstance(input.to, str) and len(input.to) > 0 else True,
            }
        )


class PoolMint(Base):
    __tablename__ = "pool_mint"
    transaction_hash: Mapped[str] = mapped_column(primary_key=True)
    address: Mapped[str] = mapped_column(primary_key=True, index=True)
    block_number: Mapped[int] = mapped_column(primary_key=True, index=True)
    timestamp: Mapped[int] = mapped_column(primary_key=True, index=True)
    gas_price: Mapped[float]
    gas_used: Mapped[float]
    sender: Mapped[str] = mapped_column(primary_key=True, index=True)
    amount0: Mapped[float] = mapped_column(primary_key=True)
    amount1: Mapped[float] = mapped_column(primary_key=True)

    def to_sql_object(self, input: Event.MintEvent):
        return PoolMint(**{
            'transaction_hash': input.transactionHash,
            'address': input.address,
            'block_number': input.blockNumber,
            'timestamp': input.timeStamp,
            'gas_price': input.gasPrice,
            'gas_used': input.gasUsed,
            'sender': input.sender,
            'amount0': input.amount0,
            'amount1': input.amount1,
        })


class PoolBurn(Base):
    __tablename__ = "pool_burn"
    transaction_hash: Mapped[str] = mapped_column(primary_key=True)
    address: Mapped[str] = mapped_column(primary_key=True, index=True)
    block_number: Mapped[int] = mapped_column(primary_key=True, index=True)
    timestamp: Mapped[int] = mapped_column(primary_key=True, index=True)
    gas_price: Mapped[float]
    gas_used: Mapped[float]
    sender: Mapped[str] = mapped_column(primary_key=True, index=True)
    receiver: Mapped[str] = mapped_column(primary_key=True, index=True)
    amount0: Mapped[float] = mapped_column(primary_key=True)
    amount1: Mapped[float] = mapped_column(primary_key=True)

    def to_sql_object(self, input: Event.BurnEvent):
        return PoolBurn(**{
            'transaction_hash': input.transactionHash,
            'address': input.address,
            'block_number': input.blockNumber,
            'timestamp': input.timeStamp,
            'gas_price': input.gasPrice,
            'gas_used': input.gasUsed,
            'sender': input.sender,
            'receiver': input.to,
            'amount0': input.amount0,
            'amount1': input.amount1,
        })


class PoolSwap(Base):
    __tablename__ = "pool_swap"
    transaction_hash: Mapped[str] = mapped_column(primary_key=True)
    address: Mapped[str] = mapped_column(primary_key=True, index=True)
    block_number: Mapped[int] = mapped_column(primary_key=True, index=True)
    timestamp: Mapped[int] = mapped_column(primary_key=True, index=True)
    gas_price: Mapped[float]
    gas_used: Mapped[float]
    sender: Mapped[str] = mapped_column(primary_key=True, index=True)
    receiver: Mapped[str] = mapped_column(primary_key=True, index=True)
    amount0_in: Mapped[float] = mapped_column(primary_key=True)
    amount0_out: Mapped[float] = mapped_column(primary_key=True)
    amount1_in: Mapped[float] = mapped_column(primary_key=True)
    amount1_out: Mapped[float] = mapped_column(primary_key=True)

    def to_sql_object(self, input: Event.SwapEvent):
        return PoolSwap(**{
            'transaction_hash': input.transactionHash,
            'address': input.address,
            'block_number': input.blockNumber,
            'timestamp': input.timeStamp,
            'gas_price': input.gasPrice,
            'gas_used': input.gasUsed,
            'sender': input.sender,
            'receiver': input.to,
            'amount0_in': input.amount0In,
            'amount0_out': input.amount0Out,
            'amount1_in': input.amount1In,
            'amount1_out': input.amount1Out,
        })


class PoolTransfer(Base):
    __tablename__ = "pool_transfer"
    transaction_hash: Mapped[str] = mapped_column(primary_key=True)
    address: Mapped[str] = mapped_column(primary_key=True, index=True)
    block_number: Mapped[int] = mapped_column(primary_key=True, index=True)
    timestamp: Mapped[int] = mapped_column(primary_key=True, index=True)
    gas_price: Mapped[float]
    gas_used: Mapped[float]
    sender: Mapped[str] = mapped_column(primary_key=True, index=True)
    receiver: Mapped[str] = mapped_column(primary_key=True, index=True)
    amount: Mapped[float] = mapped_column(primary_key=True)

    def to_sql_object(self, input: Event.TransferEvent):
        return PoolTransfer(**{
            'transaction_hash': input.transactionHash,
            'address': input.address,
            'block_number': input.blockNumber,
            'timestamp': input.timeStamp,
            'gas_price': input.gasPrice,
            'gas_used': input.gasUsed,
            'sender': input.sender,
            'receiver': input.to,
            'amount': input.amount,
        })


class TokenTransfer(Base):
    __tablename__ = "token_transfer"
    transaction_hash: Mapped[str] = mapped_column(primary_key=True)
    address: Mapped[str] = mapped_column(primary_key=True, index=True)
    block_number: Mapped[int] = mapped_column(primary_key=True, index=True)
    timestamp: Mapped[int] = mapped_column(primary_key=True, index=True)
    gas_price: Mapped[float]
    gas_used: Mapped[float]
    sender: Mapped[str] = mapped_column(primary_key=True, index=True)
    receiver: Mapped[str] = mapped_column(primary_key=True, index=True)
    amount: Mapped[float] = mapped_column(primary_key=True)

    def to_sql_object(self, input: Event.TransferEvent):
        return TokenTransfer(**{
            'transaction_hash': input.transactionHash,
            'address': input.address,
            'block_number': input.blockNumber,
            'timestamp': input.timeStamp,
            'gas_price': input.gasPrice,
            'gas_used': input.gasUsed,
            'sender': input.sender,
            'receiver': input.to,
            'amount': input.amount,
        })


if __name__ == '__main__':
    connection_url = "postgresql://postgres:blockchain2024@localhost/binance"
    engine = create_engine(connection_url, echo=True, pool_recycle=3600)
    inspector = sqlalchemy.inspect(engine)
    Base.metadata.create_all(engine)
