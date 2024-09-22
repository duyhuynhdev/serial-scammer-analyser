"""
TODO: 1. get WT in cluster 2. iterate each pool 3. get a list of scammers of the pool
"""

from functools import cached_property

from algorithms.profit_calculators.PoolsProfitCalculator import PoolsProfitCalculator
from entity.Cluster import ClusterNode, Cluster
from entity.Node import NodeLabel
from entity.blockchain.Address import Pool
from typing import Set, List

from utils import DataLoader
from utils.DataLoader import DataLoader as DataLoaderClass


class ClusterProfitCalculator:
    def __init__(self, _dataloader: DataLoaderClass, _cluster: List[ClusterNode]):
        self.dataloader = _dataloader
        self.cluster = _cluster

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
    def pools_profit_calculator(self) -> PoolsProfitCalculator:
        return PoolsProfitCalculator(self.dataloader, self.scammer_pools)

    def calculate(self) -> float:
        return self.pools_profit_calculator.calculate()


if __name__ == "__main__":
    cluster = DataLoader.load_cluster(
        "cluster_0x19b98792e98c54f58c705cddf74316aec0999aa6"
    )
    dataloader = DataLoader.DataLoader()
    ClusterProfitCalculator(dataloader, cluster).calculate()
