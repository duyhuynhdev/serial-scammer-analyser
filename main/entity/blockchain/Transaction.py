class Transaction:
    def __init__(self, blockNumber, timeStamp, hash, sender, to, value, gas, gasUsed, contractAddress, input, isError):
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


class NormalTransaction(Transaction):
    def __init__(self, blockNumber, timeStamp, hash, sender, to, value, gas, gasUsed, contractAddress, input, isError, gasPrice, methodId, functionName, cumulativeGasUsed,):
        super().__init__(blockNumber, timeStamp, hash, sender, to, value, gas, gasUsed, contractAddress, input, isError)
        self.functionName = functionName
        self.methodId = methodId
        self.gasPrice = gasPrice
        self.cumulativeGasUsed = cumulativeGasUsed


class InternalTransaction(Transaction):
    def __init__(self, blockNumber, timeStamp, hash, sender, to, value, gas, gasUsed, contractAddress, input, isError, type, errCode):
        super().__init__(blockNumber, timeStamp, hash, sender, to, value, gas, gasUsed, contractAddress, input, isError)
        self.type = type
        self.errCode = errCode
