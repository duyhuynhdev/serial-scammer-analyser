import math

from web3 import Web3

from entity.blockchain.DTO import DTO
import numpy as np

from utils import Constant


class Transaction(DTO):
    def __init__(self, blockNumber=None, timeStamp=None, hash=None, sender=None, to=None, value=None, gas=None, gasUsed=None, contractAddress=None, input=None, isError=None):
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
        if (self.isError == 1) or (self.isError == '1'):
            return 0
        return int(self.value) / 10 ** Constant.WETH_BNB_DECIMALS

    def is_creation_contract(self):
        return (isinstance(self.to, float) and math.isnan(self.to)) or not self.to

    def is_in_tx(self, owner):
        if self.is_creation_contract():
            return False
        try:
            to = Web3.to_checksum_address(self.to)
            owner = Web3.to_checksum_address(owner)
        except Exception as e:
            return False
        return to == owner

    def is_out_tx(self, owner):
        if self.is_creation_contract():
            return False
        try:
            sender = Web3.to_checksum_address(self.sender)
            owner = Web3.to_checksum_address(owner)
        except Exception as e:
            return False
        return sender == owner


class NormalTransaction(Transaction):
    def __init__(self, blockNumber=None, timeStamp=None, hash=None, sender=None, to=None, value=None, gas=None, gasUsed=None, contractAddress=None, input=None, isError=None, gasPrice=None,
                 methodId=None, functionName=None, cumulativeGasUsed=None):
        super().__init__(blockNumber, timeStamp, hash, sender, to, value, gas, gasUsed, contractAddress, input, isError)
        self.functionName = functionName
        self.methodId = methodId
        self.gasPrice = gasPrice
        self.cumulativeGasUsed = cumulativeGasUsed

    def get_transaction_fee(self):
        if (self.isError == 1) or (self.isError == '1'):
            return 0
        return int(self.gasPrice * self.gasUsed) / 10 ** Constant.WETH_BNB_DECIMALS

    def is_to_eoa(self, owner):
        return self.is_out_tx(owner) and ((isinstance(self.functionName, float) and math.isnan(self.functionName)) or not self.functionName)

    def is_to_contract(self, owner):
        return not self.is_to_eoa(owner)


class InternalTransaction(Transaction):
    def __init__(self, blockNumber=None, timeStamp=None, hash=None, sender=None, to=None, value=None, gas=None, gasUsed=None, contractAddress=None, input=None, isError=None, type=None, errCode=None):
        super().__init__(blockNumber, timeStamp, hash, sender, to, value, gas, gasUsed, contractAddress, input, isError)
        self.type = type
        self.errCode = errCode
