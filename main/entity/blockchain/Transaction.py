from entity.blockchain.DTO import DTO


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
        return int(self.value) / 10 ** 18



class NormalTransaction(Transaction):
    def __init__(self, blockNumber=None, timeStamp=None, hash=None, sender=None, to=None, value=None, gas=None, gasUsed=None, contractAddress=None, input=None, isError=None, gasPrice=None,
                 methodId=None, functionName=None, cumulativeGasUsed=None):
        super().__init__(blockNumber, timeStamp, hash, sender, to, value, gas, gasUsed, contractAddress, input, isError)
        self.functionName = functionName
        self.methodId = methodId
        self.gasPrice = gasPrice
        self.cumulativeGasUsed = cumulativeGasUsed

    def get_transaction_fee(self):
        return int(self.gasPrice * self.gasUsed) / 10 ** 18

class InternalTransaction(Transaction):
    def __init__(self, blockNumber=None, timeStamp=None, hash=None, sender=None, to=None, value=None, gas=None, gasUsed=None, contractAddress=None, input=None, isError=None, type=None, errCode=None):
        super().__init__(blockNumber, timeStamp, hash, sender, to, value, gas, gasUsed, contractAddress, input, isError)
        self.type = type
        self.errCode = errCode


class SwapTransaction(NormalTransaction):
    def __init__(self, blockNumber=None, timeStamp=None, hash=None, sender=None, to=None, value=None, gas=None, gasUsed=None, contractAddress=None, input=None, isError=None, gasPrice=None,
                 methodId=None, functionName=None, cumulativeGasUsed=None):
        super().__init__(blockNumber, timeStamp, hash, sender, to, value, gas, gasUsed, contractAddress, input, isError, gasPrice, methodId, functionName, cumulativeGasUsed)
        self.token0 = None,
        self.token1 = None,
        self.pool_address = None
        self.scammers = []
