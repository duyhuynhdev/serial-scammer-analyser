from functools import cached_property

from entity.Cluster import ClusterNode, Cluster
from entity.Node import NodeLabel
from entity.blockchain.Address import Pool
from typing import Set, List

from utils import DataLoader


class ClusterProfitCalculator:
    def __init__(self):
        self.dataloader = DataLoader.DataLoader()
        self.cluster = None  # self.cluster will be set in self.calculate()

    @cached_property
    def scammer_nodes(self) -> Set[ClusterNode]:
        """
        Returns a set of scammer nodes in the cluster
        """
        return {
            node
            for node in self.cluster
            if (
                NodeLabel.is_scammer(node)
                and node.address in self.dataloader.unique_scammers
            )
        }

    @cached_property
    def scammer_pools(self) -> Set[Pool]:
        """
        Returns a set of scammer pools belonging to the cluster
        """
        return {
            pool
            for scammer_node in self.scammer_nodes
            for pool in DataLoader.load_pool(scammer_node.address, self.dataloader)
        }

    @cached_property
    def washer_nodes(self) -> Set[ClusterNode]:
        swap_tos = {
            swap.to.lower() for pool in self.scammer_pools for swap in pool.swaps
        }
        return {node for node in self.cluster if node.address in swap_tos}

    def calculate(self, cluster: List[ClusterNode]) -> float:
        """
        Reuse the same instance for calculating profits on different clusters.
        """
        self.cluster = cluster
        # Invalidate cached properties for a new cluster
        self.__dict__.pop("scammer_nodes", None)
        self.__dict__.pop("scammer_pools", None)
        self.__dict__.pop("washer_nodes", None)

        return sum(
            self.calculate_per_pool(scammer_pool) for scammer_pool in self.scammer_pools
        )

    def calculate_per_pool(self, pool: Pool) -> float:
        high_value_token_position = pool.get_high_value_position()
        scam_token_position = 1 - high_value_token_position

        y = 0
        match pool.burns:
            # If the number of burn events is 1, the type of rug-pull is simple
            case [_]:  # Wildcard to match any single item without binding to a variable
                burn_total, fee_total = pool.get_total_burn_value(
                    high_value_token_position
                )
                y += burn_total - fee_total
            # If the number of burn events is greater than 1, the type of rug-pull is "sell"
            case [_, *_]:
                max_swap, swap_fee = pool.get_max_swap_value(high_value_token_position)
                y += max_swap - swap_fee
            case _:
                raise ValueError("pool.burns should have at least one burn")

        x = 0
        mint_total, fee_total = pool.get_total_mint_value(high_value_token_position)
        scam_token = pool.get_scam_token(scam_token_position)
        normal_txs, _ = DataLoader.load_transaction_by_address(scam_token.creator)
        creation_fee = next(
            (
                normal_tx.get_transaction_fee()
                for normal_tx in normal_txs
                if normal_tx.hash == scam_token.creation_tx
            ),
            0,  # Default value if no matching transaction is found
        )
        x = mint_total + fee_total + creation_fee

        z = 0

        return y - x - z

    def calculate_batch(self, clusters: List[List[ClusterNode]]) -> List[float]:
        """
        Batch process multiple clusters by resetting cached properties between calculations.
        """
        return [self.calculate(cluster) for cluster in clusters]


if __name__ == "__main__":
    calculator = ClusterProfitCalculator()
    test_cluster = DataLoader.load_cluster(
        "cluster_0x19b98792e98c54f58c705cddf74316aec0999aa6"
    )
    calculator.calculate(test_cluster)
