from entity.blockchain.DTO import DTO


class Event(DTO):
    def __init__(self, address=None, event=None, blockNumber=None, blockHash=None, timeStamp=None, gasPrice=None, gasUsed=None, transactionHash=None, HV_position = 0):
        super().__init__()
        self.address = address
        self.event = event
        self.transactionHash = transactionHash
        self.blockHash = blockHash
        self.blockNumber = blockNumber
        self.timeStamp = timeStamp
        self.gasUsed = gasUsed
        self.gasPrice = gasPrice
        self.HV_position = HV_position

class SwapEvent(Event):
    def __init__(self, address=None, event=None, blockNumber=None, blockHash=None, timeStamp=None, gasPrice=None, gasUsed=None, transactionHash=None, sender=None, to=None, amount0In=None,
                 amount1In=None, amount0Out=None, amount1Out=None, HV_position = 0):
        super().__init__(address, event, blockNumber, blockHash, timeStamp, gasPrice, gasUsed, transactionHash, HV_position)
        self.sender = sender
        self.to = to
        self.amount0In = amount0In
        self.amount0Out = amount0Out
        self.amount1In = amount1In
        self.amount1Out = amount1Out

class MintEvent(Event):
    def __init__(self, address=None, event=None, blockNumber=None, blockHash=None, timeStamp=None, gasPrice=None, gasUsed=None, transactionHash=None, sender=None, amount0=None, amount1=None, HV_position = 0):
        super().__init__(address, event, blockNumber, blockHash, timeStamp, gasPrice, gasUsed, transactionHash, HV_position)
        self.sender = sender
        self.amount0 = amount0
        self.amount1 = amount1


class BurnEvent(Event):
    def __init__(self, address=None, event=None, blockNumber=None, blockHash=None, timeStamp=None, gasPrice=None, gasUsed=None, transactionHash=None, sender=None, to=None, amount0=None, amount1=None, HV_position = 0):
        super().__init__(address, event, blockNumber, blockHash, timeStamp, gasPrice, gasUsed, transactionHash, HV_position)
        self.sender = sender
        self.to = to
        self.amount0 = amount0
        self.amount1 = amount1


class SwapEvent(Event):
    def __init__(self, address=None, event=None, blockNumber=None, blockHash=None, timeStamp=None, gasPrice=None, gasUsed=None, transactionHash=None, sender=None, to=None, amount0In=None,
                 amount1In=None, amount0Out=None, amount1Out=None, HV_position = 0):
        super().__init__(address, event, blockNumber, blockHash, timeStamp, gasPrice, gasUsed, transactionHash, HV_position)
        self.sender = sender
        self.to = to
        self.amount0In = amount0In
        self.amount0Out = amount0Out
        self.amount1In = amount1In
        self.amount1Out = amount1Out


class TransferEvent(Event):
    def __init__(self, address=None, event=None, blockNumber=None, blockHash=None, timeStamp=None, gasPrice=None, gasUsed=None, transactionHash=None, sender=None, to=None, amount=None, HV_position = 0):
        super().__init__(address, event, blockNumber, blockHash, timeStamp, gasPrice, gasUsed, transactionHash, HV_position)
        self.sender = sender
        self.to = to
        self.amount = amount
