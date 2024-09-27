from functools import cached_property

from entity.Cluster import ClusterNode
from entity.Node import NodeLabel
from entity.blockchain.Address import Pool, SwapDirection
from typing import Set, List
from utils import DataLoader


class ClusterProfitCalculator:
    def __init__(self):
        self.dataloader = DataLoader.DataLoader()
        self.cluster = None  # self.cluster will be set in self.calculate()

    @cached_property
    def node_addresses_in_cluster(self) -> Set[str]:
        return {node.address.lower() for node in self.cluster}

    @cached_property
    def scammer_node_addresses(self) -> Set[str]:
        """
        Return a set of scammer nodes in the cluster
        """
        return {
            node.address.lower()
            for node in self.cluster
            if (
                NodeLabel.is_scammer(node)
                # TODO: remove this line once we're confident that the given cluster is made only of scammer nodes
                and node.address
                in self.dataloader.scammers_set

            )
        }

    @cached_property
    def scammer_pools(self) -> Set[Pool]:
        """
        Return a set of scammer pools belonging to the cluster
        """
        return {
            pool
            for scammer_node_address in self.scammer_node_addresses
            for pool in DataLoader.load_pool(scammer_node_address, self.dataloader)
        }

    def initialise_cluster(self, cluster: List[ClusterNode]) -> None:
        self.cluster = cluster
        # Invalidate cached properties for a new cluster
        self.__dict__.pop("node_addresses_in_cluster", None)
        self.__dict__.pop("scammer_node_addresses", None)
        self.__dict__.pop("scammer_pools", None)

    def calculate(self, cluster: List[ClusterNode]) -> float:
        """
        Reuse the same instance for calculating profits on different clusters.
        """
        self.initialise_cluster(cluster)

        return sum(
            self.calculate_profit_per_pool(scammer_pool) for scammer_pool in self.scammer_pools
        )

    def calculate_y_per_pool(self, pool: Pool) -> float:
        y = 0.0

        burn_total, fee_total = pool.get_total_burn_value()
        y += burn_total - fee_total

        sell_total, fee_total = pool.get_total_swap_value(self.node_addresses_in_cluster, SwapDirection.OUT)
        y += sell_total - fee_total

        return y

    def calculate_x_per_pool(self, pool: Pool) -> float:
        x = 0.0

        mint_total, fee_total = pool.get_total_mint_value()
        scam_token = pool.get_scam_token()
        normal_txs, _ = DataLoader.load_transaction_by_address(scam_token.creator)
        token_creation_fee = next(
            (
                normal_tx.get_transaction_fee()
                for normal_tx in normal_txs
                if normal_tx.hash == scam_token.creation_tx
            ),
            # TODO: add a check to return an error if creation_fee is 0.
            0,  # Default value if no matching transaction is found
        )
        x += mint_total + fee_total + token_creation_fee
        return x

    def calculate_z_per_pool(self, pool: Pool) -> float:
        z = 0.0

        buy_total, fee_total = pool.get_total_swap_value(self.node_addresses_in_cluster, SwapDirection.IN)
        z += buy_total + fee_total
        return z

    def calculate_profit_per_pool(self, pool: Pool) -> float:
        """
        Calculate the true profit made in a given pool
        """
        y = self.calculate_y_per_pool(pool)
        x = self.calculate_x_per_pool(pool)
        z = self.calculate_z_per_pool(pool)

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
    print(calculator.calculate(test_cluster))
