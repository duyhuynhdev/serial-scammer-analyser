class Event:
    def __init__(self, address, event, blockNumber, blockHash, timeStamp, gasPrice, gasUsed, transactionHash):
        self.address = address
        self.event = event
        self.transactionHash = transactionHash
        self.blockHash = blockHash
        self.blockNumber = blockNumber
        self.timeStamp = timeStamp
        self.gasUsed = gasUsed
        self.gasPrice = gasPrice


class MintEvent(Event):
    def __init__(self, address, event, blockNumber, blockHash, timeStamp, gasPrice, gasUsed, transactionHash, sender, amount0, amount1):
        super().__init__(address, event, blockNumber, blockHash, timeStamp, gasPrice, gasUsed, transactionHash)
        self.sender = sender
        self.amount0 = amount0
        self.amount1 = amount1


class BurnEvent(Event):
    def __init__(self, address, event, blockNumber, blockHash, timeStamp, gasPrice, gasUsed, transactionHash, sender, to, amount0, amount1):
        super().__init__(address, event, blockNumber, blockHash, timeStamp, gasPrice, gasUsed, transactionHash)
        self.sender = sender
        self.to = to
        self.amount0 = amount0
        self.amount1 = amount1


class SwapEvent(Event):
    def __init__(self, address, event, blockNumber, blockHash, timeStamp, gasPrice, gasUsed, transactionHash, sender, to, amount0In, amount1In, amount0Out, amount1Out):
        super().__init__(address, event, blockNumber, blockHash, timeStamp, gasPrice, gasUsed, transactionHash)
        self.sender = sender
        self.to = to
        self.amount0In = amount0In
        self.amount0Out = amount0Out
        self.amount1In = amount1In
        self.amount1Out = amount1Out


class TransferEvent(Event):
    def __init__(self, address, event, blockNumber, blockHash, timeStamp, gasPrice, gasUsed, transactionHash, sender, to, amount):
        super().__init__(address, event, blockNumber, blockHash, timeStamp, gasPrice, gasUsed, transactionHash)
        self.sender = sender
        self.to = to
        self.amount = amount
