
class NodeLabel:
    S = "scammer"
    D = "deposit"
    C = "coordinator"
    SF = "scammer_funder"
    SB = "scammer_beneficiary"
    WT = "wash_trading"
    WTF = "wash_trading_funder"

class Node:
    def __init__(self, address, prev_nodes, next_nodes, in_txs, out_txs, label):
        self.address = address
        self.prev_nodes = prev_nodes
        self.next_nodes = next_nodes
        self.neighbours = set(next_nodes) | set(prev_nodes)
        self.in_txs = in_txs
        self.out_txs = out_txs
        self.in_degree = len(self.in_txs)
        self.out_degree = len(self.out_txs)
        self.label = label