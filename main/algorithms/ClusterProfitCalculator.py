from functools import cached_property

from data_collection.AccountCollector import TransactionCollector
from entity.Cluster import ClusterNode
from entity.Node import NodeLabel
from entity.blockchain.Address import Pool, SwapDirection, Token
from typing import Set, List, Union

from entity.blockchain.Transaction import NormalTransaction
from utils import DataLoader
from utils.Utils import get_transaction_by_hash

class ClusterProfitCalculator:
    def __init__(self):
        self.dataloader = DataLoader.DataLoader()
        self.transactionCollector = TransactionCollector()
        # self.cluster_name and self.cluster will be set in the _initialise_cluster function.
        self.cluster_name: Union[str, None] = None
        self.cluster: Union[List[ClusterNode], None] = None

    @cached_property
    def node_addresses_in_cluster(self) -> Set[str]:
        """
        Returns a set of node addresses in the cluster.
        """
        return {node.address.lower() for node in self.cluster}

    @cached_property
    def scammer_node_addresses(self) -> Set[str]:
        """
        Returns a set of scammer nodes in the cluster.
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
        Returns a set of scammer pools belonging to the cluster.
        """
        return {
            pool
            for scammer_node_address in self.scammer_node_addresses
            for pool in DataLoader.load_pool(scammer_node_address, self.dataloader)
        }

    def _initialise_cluster(self, cluster_name: str) -> None:
        """
        Initializes the cluster with the provided cluster_name
        and invalidates cached properties related to the cluster.
        """
        self.cluster_name = cluster_name
        self.cluster = DataLoader.load_cluster(cluster_name)

        # Invalidate cached properties for a new cluster
        self.__dict__.pop("node_addresses_in_cluster", None)
        self.__dict__.pop("scammer_node_addresses", None)
        self.__dict__.pop("scammer_pools", None)

    def calculate(self, cluster_name: str) -> float:
        """
        Calculates the total profit from the given cluster_name

        Initializes the cluster and sums the profit for each scammer pool
        within the cluster.
        """
        self._initialise_cluster(cluster_name)

        return sum(
            self.calculate_profit_per_pool(scammer_pool) for scammer_pool in self.scammer_pools
        )

    def calculate_y_per_pool(self, pool: Pool) -> float:
        """
        Calculates the total value (y) of a pool.

        This function evaluates the total burn value and associated fees, as well as the total swap-out value and fees.
        Swaps are only considered if the destination address (`swap_to`) is included in the set of node addresses in
        the cluster.
        """
        y = 0.0

        burn_total, fee_total = pool.calculate_burn_value_and_fees()
        y += burn_total - fee_total

        sell_total, fee_total = pool.calculate_swap_value_and_fees(self.node_addresses_in_cluster, SwapDirection.OUT)
        y += sell_total - fee_total

        return y

    def calculate_x_per_pool(self, pool: Pool) -> float:
        """
        Calculates the total value (X) for a given pool by evaluating its
        mint value, associated fees, and token creation fee.

        Note that the pool creation event is the same as the mint event; there is no need to add the pool creation
        transaction amount and fee.
        """
        x = 0.0

        mint_total, fee_total = pool.calculate_mint_value_and_fees()
        token_creation_fee = self.calculate_token_creation_fee(pool)

        x += mint_total + fee_total + token_creation_fee

        return x

    def _validate_scam_token_is_created_by_cluster(self, scam_token: Token, pool: Pool) -> None:
        """
        Validate that the scam token creator is in the cluster.
        """
        if scam_token.creator not in self.node_addresses_in_cluster:
            raise ValueError(f"Scam token creator {scam_token.creator} of the scam token {scam_token.name} used in "
                             f"the pool {pool.address} is not in the cluster's node addresses within"
                             f" {self.cluster_name}.")

    def _validate_transaction_amount_is_zero(self, transaction: NormalTransaction, pool: Pool) -> None:
        """
        Validate that the transaction amount is zero.
        """
        if (transaction_amount := transaction.get_transaction_amount()) != 0:
            raise ValueError(
                f"The transaction amount for the transaction hash {transaction.hash} is expected to be zero, "
                f"but it is {transaction_amount} in pool {pool.address} within the cluster {self.cluster_name}."
            )

    def calculate_token_creation_fee(self, pool: Pool) -> float:
        """
        Calculates the creation fee for a scam token in the specified pool.

        This method retrieves the scam token's creator and the transaction hash used for its creation.
        It checks if the creator is a node address in the cluster; if not, it raises an error since the creator is
        expected to be within the set.

        If the creator is valid, the method retrieves all transactions associated with the creator's address
        and uses the `one` function to obtain the transaction fee for the specific creation transaction,
        which is guaranteed to be present in the list of normal transactions.
        """
        token_creation_fee = 0.0

        scam_token = pool.get_scam_token()

        self._validate_scam_token_is_created_by_cluster(scam_token, pool)

        normal_txs, _ = DataLoader.load_transaction_by_address(scam_token.creator)
        token_creation_tx = get_transaction_by_hash(normal_txs, scam_token.creation_tx)

        self._validate_transaction_amount_is_zero(token_creation_tx, pool)

        token_creation_fee += token_creation_tx.get_transaction_fee()

        return token_creation_fee

    def calculate_z_per_pool(self, pool: Pool) -> float:
        """
        Calculate the total 'z' value for a pool by summing swap in (buy) value and fees.
        """
        z = 0.0

        buy_total, fee_total = pool.calculate_swap_value_and_fees(self.node_addresses_in_cluster, SwapDirection.IN)
        z += buy_total + fee_total
        return z

    def calculate_profit_per_pool(self, pool: Pool) -> float:
        """
        Calculate the true profit made in a given pool.
        """
        y = self.calculate_y_per_pool(pool)
        x = self.calculate_x_per_pool(pool)
        z = self.calculate_z_per_pool(pool)

        return y - x - z

    def calculate_batch(self, cluster_names: List[str]) -> List[float]:
        """
        Batch process multiple clusters.
        """
        return [self.calculate(cluster_name) for cluster_name in cluster_names]



if __name__ == "__main__":
    calculator = ClusterProfitCalculator()
    cluster_profit = calculator.calculate("cluster_0x19b98792e98c54f58c705cddf74316aec0999aa6")
    print(cluster_profit)

