import math

from web3 import Web3

from data_collection.DataDecoder import FunctionInputDecoder
from entity.blockchain.DTO import DTO

from utils import Constant


class Transaction(DTO):
    def __init__(
        self,
        blockNumber=None,
        timeStamp=None,
        hash=None,
        sender=None,
        to=None,
        value=None,
        gas=None,
        gasUsed=None,
        contractAddress=None,
        input=None,
        isError=None,
    ):
        super().__init__()
        self.input = input
        self.hash = hash
        self.blockNumber = blockNumber
        self.timeStamp = timeStamp
        self.sender = sender
        self.to = to
        self.value = value
        self.contractAddress = contractAddress
        self.gas = gas
        self.gasUsed = gasUsed
        self.isError = isError

    def from_dict(self, dict):
        for name, value in dict.items():
            setattr(self, name, value)

    def get_transaction_amount(self):
        if (self.isError == 1) or (self.isError == "1"):
            return 0
        return float(self.value) / 10**Constant.WETH_BNB_DECIMALS

    def is_error(self):
        return self.isError == 1 or self.isError == '1'

    def is_not_error(self):
        return not self.is_error()

    def is_to_empty(self):
        return not self.to or (isinstance(self.to, float) and math.isnan(self.to))

    def is_creation_contract_tx(self):
        return self.is_to_empty()

    def is_in_tx(self, owner):
        if self.is_creation_contract_tx():
            return False
        try:
            to = Web3.to_checksum_address(self.to)
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


class NormalTransaction(Transaction):
    def __init__(
        self,
        blockNumber=None,
        timeStamp=None,
        hash=None,
        sender=None,
        to=None,
        value=None,
        gas=None,
        gasUsed=None,
        contractAddress=None,
        input=None,
        isError=None,
        gasPrice=None,
        methodId=None,
        functionName=None,
        cumulativeGasUsed=None,
    ):
        super().__init__(
            blockNumber,
            timeStamp,
            hash,
            sender,
            to,
            value,
            gas,
            gasUsed,
            contractAddress,
            input,
            isError,
        )
        self.functionName = functionName
        self.methodId = methodId
        self.gasPrice = gasPrice
        self.cumulativeGasUsed = cumulativeGasUsed

    def __eq__(self, other):
        if not isinstance(other, NormalTransaction):
            return False
        return self.hash == other.hash

    def __hash__(self):
        return hash(self.hash.lower())

    def is_function_empty(self):
        return (
            isinstance(self.functionName, float) and math.isnan(self.functionName)
        ) or not self.functionName

    def is_transfer_tx(self):
        return self.is_function_empty() and not self.is_to_empty()

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
        if (self.isError == 1) or (self.isError == "1"):
            return 0
        return (
            float(self.gasPrice) * float(self.gasUsed) / 10**Constant.WETH_BNB_DECIMALS
        )

    def get_transaction_amount_and_fee(self):
        return self.get_transaction_amount() + self.get_transaction_fee()

    def get_true_transfer_amount(self, address):
        if self.is_in_tx(address):
            return self.get_transaction_amount()
        if self.is_out_tx(address):
            return self.get_transaction_amount() + self.get_transaction_fee()
        return 0


class InternalTransaction(Transaction):
    def __init__(
        self,
        blockNumber=None,
        timeStamp=None,
        hash=None,
        sender=None,
        to=None,
        value=None,
        gas=None,
        gasUsed=None,
        contractAddress=None,
        input=None,
        isError=None,
        type=None,
        errCode=None,
    ):
        super().__init__(
            blockNumber,
            timeStamp,
            hash,
            sender,
            to,
            value,
            gas,
            gasUsed,
            contractAddress,
            input,
            isError,
        )
        self.type = type
        self.errCode = errCode
