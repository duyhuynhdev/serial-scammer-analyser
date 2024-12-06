import functools
import os.path
from functools import cached_property
from collections import namedtuple
import sys
from data_collection.AccountCollector import TransactionCollector, CreatorCollector
from entity.Cluster import ClusterNode
from entity.Node import NodeLabel
from entity.blockchain.Address import Pool, Token
from typing import Set, List, Union, Mapping

from entity.blockchain.Transaction import NormalTransaction
from utils import DataLoader, Constant
from utils.Utils import get_transaction_by_hash

ProfitPool = namedtuple("ProfitPool", ["profit", "pool"])


def log_cluster_calculation(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        print(
            f"{'=' * 100}\n"
            f"       Starting Calculation for Cluster: {args[1]}       \n"
            f"{'=' * 100}"
        )

        result = func(*args, **kwargs)

        print(
            f"{'=' * 100}\n"
            f"       Completed Calculation for Cluster: {args[1]}       \n"
            f"{'=' * 100}"
        )

        print(f"Total pool profits in the cluster: {result}")
        print(
            f"Total transfer fees between nodes in the cluster: {args[0].cluster_transfer_fees}"
        )
        print(
            f"Net cluster profit after deducting transfer fees: {result - args[0].cluster_transfer_fees}"
        )

        return result

    return wrapper


class ClusterProfitCalculator:
    def __init__(self, dex: str):
        self.dex = dex
        self.dataloader = DataLoader.DataLoader(dex=self.dex)
        self.transactionCollector = TransactionCollector()
        # self.cluster_name and self.cluster will be set in the _initialise_cluster function.
        self.cluster_name: Union[str, None] = None
        self.cluster: Union[List[ClusterNode], None] = None

    @cached_property
    def node_addresses_in_cluster(self) -> Set[str]:
        """
        Returns a set of node addresses in the cluster.
        """
        return {node.address.lower() for node in self.cluster if len(node.labels) > 0}

    @cached_property
    def scammer_node_addresses(self) -> Set[str]:
        """
        Returns a set of scammer nodes in the cluster.
        """
        return {
            node.address.lower()
            for node in self.cluster
            if NodeLabel.is_scammer(node)
            # TODO: remove this line once we're confident that the given cluster does not
            #  include contracts
            # and node.address in self.dataloader.scammers_set
        }

    @cached_property
    def scammer_pools(self) -> Set[Pool]:
        """
        Returns a set of scammer pools belonging to the cluster.
        """
        return {
            pool
            for scammer_node_address in self.scammer_node_addresses
            for pool in DataLoader.load_pool(
                scammer_node_address, self.dataloader, dex=self.dex
            )
        }

    @cached_property
    def cluster_transfers(self) -> Set[NormalTransaction]:
        """
        Returns a set of NormalTransaction objects representing transfer transactions between
        nodes in the cluster.
        """
        return {
            normal_tx
            for node_address in self.node_addresses_in_cluster
            for normal_tx in TransactionCollector().get_transactions(
                node_address, dex=self.dex
            )[0]
            if normal_tx.is_transfer_tx()
            and normal_tx.sender.lower() in self.node_addresses_in_cluster
            and normal_tx.to.lower() in self.node_addresses_in_cluster
        }

    @cached_property
    def cluster_transfer_fees(self) -> float:
        """
        Returns the total transfer fees incurred from direct transfers between nodes within this
        cluster.

        This includes only the fees from normal transactions between the nodes
        """
        return sum(
            float(tx.gasUsed) * float(tx.gasPrice) / 10**Constant.WETH_BNB_DECIMALS
            for tx in self.cluster_transfers
        )

    def _initialise_cluster(self, cluster_name: str) -> None:
        """
        Initializes the cluster with the provided cluster_name
        and invalidates cached properties related to the cluster.
        """
        self.cluster_name = cluster_name
        self.cluster = DataLoader.load_cluster(cluster_name, dex=self.dex)

        # Invalidate cached properties for a new cluster
        self.__dict__.pop("node_addresses_in_cluster", None)
        self.__dict__.pop("scammer_node_addresses", None)
        self.__dict__.pop("scammer_pools", None)
        self.__dict__.pop("cluster_transfers", None)
        self.__dict__.pop("cluster_transfer_fees", None)

    @log_cluster_calculation
    def calculate(self, cluster_name: str) -> float:
        """
        Calculates the total profit from the given cluster_name

        Initializes the cluster and sums the profit for each scammer pool
        within the cluster.
        """
        self._initialise_cluster(cluster_name)

        for scammer_pool in self.scammer_pools:
            self.update_profit_metrics_per_pool(scammer_pool)

        pools_sorted_bt_profit = sorted(
            [ProfitPool(pool.profit, pool) for pool in self.scammer_pools],
            key=lambda x: x.profit,
        )

        for _, pool in pools_sorted_bt_profit:
            self.log_pool_calculation_results(pool)

        pools_profits_total = sum(
            profit_and_pool.profit for profit_and_pool in pools_sorted_bt_profit
        )

        return pools_profits_total

    def calculate_y_per_pool(self, pool: Pool) -> float:
        """
        Calculates the total value (y) of a pool.

        This function evaluates the total burn value and associated fees, as well as the total
        rug-pulling withdrawal value and fees by nodes in the scam cluster.
        """
        y = 0.0

        burn_total, fee_total = pool.calculate_total_burn_value_and_fees()
        y += burn_total - fee_total

        rug_pulling_withdrawal_total, fee_total = (
            pool.calculate_total_divesting_value_and_fees_by_addressees(
                self.node_addresses_in_cluster
            )
        )
        y += rug_pulling_withdrawal_total - fee_total

        return y

    def calculate_x_per_pool(self, pool: Pool) -> float:
        """
        Calculates the total value (X) for a given pool by evaluating its
        mint value, associated fees, and token creation fee.

        Note that the pool creation event is the same as the mint event; there is no need to add
        the pool creation transaction amount and fee.
        """
        x = 0.0

        mint_total, fee_total = pool.calculate_total_mint_value_and_fees()
        token_creation_fee = self.calculate_token_creation_fee(pool)

        x += mint_total + fee_total + token_creation_fee

        return x

    def _validate_scam_token_is_created_by_cluster(
        self, scam_token_creator_info: Mapping[str, str], pool: Pool
    ) -> None:
        """
        Validate that the scam token creator is in the cluster.
        """
        if (
            scam_token_creator_info["contractCreator"]
            not in self.node_addresses_in_cluster
        ):
            raise ValueError(
                f"Scam token creator {scam_token_creator_info['contractCreator']} of the scam "
                f"token used in the pool {pool.address} is not in the cluster's node addresses "
                f"within {self.cluster_name}."
            )

    def _validate_transaction_amount_is_zero(
        self, transaction: NormalTransaction, pool: Pool
    ) -> None:
        """
        Validate that the transaction amount is zero.
        """
        if (transaction_amount := transaction.get_transaction_amount()) != 0:
            raise ValueError(
                f"The transaction amount for the transaction hash {transaction.hash} is expected "
                f"to be zero, but it is {transaction_amount} in pool {pool.address} within the "
                f"cluster {self.cluster_name}."
            )

    def calculate_token_creation_fee(self, pool: Pool) -> float:
        """
        Calculates the creation fee for a scam token in the specified pool.

        This method retrieves the scam token's creator and the transaction hash used for its
        creation. It checks if the creator is a node address in the cluster; if not, it raises an
        error since the creator is expected to be within the set.

        If the creator is valid, the method retrieves all transactions associated with the
        creator's address and uses the `one` function to obtain the transaction fee for the
        specific creation transaction, which is guaranteed to be present in the list of normal
        transactions.
        """
        token_creation_fee = 0.0

        scam_token_creator_info = CreatorCollector().get_token_creator(
            pool.scam_token_address, dex=self.dex
        )

        self._validate_scam_token_is_created_by_cluster(scam_token_creator_info, pool)

        normal_txs, _ = DataLoader.load_transaction_by_address(
            scam_token_creator_info["contractCreator"], dex=self.dex
        )
        token_creation_tx = get_transaction_by_hash(
            normal_txs, scam_token_creator_info["txHash"]
        )

        # self._validate_transaction_amount_is_zero(token_creation_tx, pool)

        token_creation_fee += token_creation_tx.get_transaction_fee() + token_creation_tx.get_transaction_amount()

        return token_creation_fee

    def calculate_z_per_pool(self, pool: Pool) -> float:
        """
        Calculate the total 'z' value for a pool by summing disingenuous investing value and fees
        by nodes in the scam cluster.
        """
        z = 0.0

        disingenuous_investing_value_total, fee_total = (
            pool.calculate_total_investing_value_and_fees_by_addressees(
                self.node_addresses_in_cluster
            )
        )
        z += disingenuous_investing_value_total + fee_total
        return z

    def get_legitimate_investor_node_addresses(self, pool: Pool) -> Set[str]:
        return pool.investing_node_addresses - self.node_addresses_in_cluster

    def get_scam_investor_node_addresses(self, pool: Pool) -> Set[str]:
        return pool.investing_node_addresses & self.node_addresses_in_cluster

    @staticmethod
    def calculate_profit_per_pool(x: float, y: float, z: float) -> float:
        return y - x - z

    def update_profit_metrics_per_pool(self, pool: Pool) -> None:
        """
        Calculate the true profit made in a given pool.
        """
        y = self.calculate_y_per_pool(pool)
        x = self.calculate_x_per_pool(pool)
        z = self.calculate_z_per_pool(pool)
        profit = self.calculate_profit_per_pool(x, y, z)

        pool.x = x
        pool.y = y
        pool.z = z
        pool.profit = profit

    def log_pool_calculation_results(self, pool: Pool) -> None:
        # Get legitimate and scam investor node addresses
        legitimate_addresses = self.get_legitimate_investor_node_addresses(pool)
        scam_addresses = self.get_scam_investor_node_addresses(pool)

        # Format the addresses for logging
        legitimate_addresses_str = (
            "\n - ".join(legitimate_addresses) if legitimate_addresses else "None"
        )
        scam_addresses_str = "\n - ".join(scam_addresses) if scam_addresses else "None"

        print(
            f"Pool address: {pool.address}\n"
            f"Value of y: {pool.y}\n"
            f"Value of x: {pool.x}\n"
            f"Value of z: {pool.z}\n"
            f"Legitimate investor node addresses:\n - {legitimate_addresses_str}\n"
            f"Scam investor node addresses:\n - {scam_addresses_str}\n"
            f"The profit of this pool is {pool.profit}"
            f"\n"
        )

    def calculate_batch(self, cluster_names: List[str]) -> List[float]:
        """
        Batch process multiple clusters.
        """
        return [self.calculate(cluster_name) for cluster_name in cluster_names]


if __name__ == "__main__":
    uni_accepted_cluster = [1528,
3025,
1519,
1647,
3628,
8739,
7004,
3605,
6615,
7585,
6717,
8653,
117,
7280,
90,
4508,
5518,
8256,
5033,
2027,
268,
641,
4009,
1565,
6696,
6556,
1147,
7031,
262,
8589,
171,
667,
2042,
6,
4152,
1507,
8028,
1573,
4205,
1263,
3049,
717,
5069,
3213,
3106,
1706,
3525,
7556,
2029,
8283,
8057,
3540,
1253,
5190,
7045,
8745,
735,
1739,
5747,
2116,
3015,
1686,
6016,
4064,
3636,
5218,
1540,
567,
2249,
6607,
1559,
85,
6155,
4060,
3594,
4006,
7562,
8631,
2111,
5710,
3112,
7063,
7275,
2503,
2057,
1256,
2047,
3166,
2054,
4596,
8612,
4020,
6724,
1731,
8125,
5011,
5093,
5067,
6509,
5013,
8699,
5029,
511,
1050,
1022,
4036,
5538,
6561,
7608,
5587,
6168,
5197,
249,
5736,
8279,
6114,
1570,
6530,
4004,
5543,
8587,
7581,
1650,
4141,
1664,
3116,
4590,
645,
2187,
7159,
8597,
5062,
306,
7153,
]
    # pan_accepted_cluster = [2002,
    #                         5009,
    #                         6010,
    #                         1008,
    #                         8004,
    #                         2005,
    #                         11007,
    #                         11516,
    #                         4004,
    #                         8504,
    #                         503,
    #                         10504]
    uni_dex = "univ2"
    # pancake_dex = "panv2"
    for cid in uni_accepted_cluster:
        if os.path.exists(f"profit/cluster_{cid}.txt"):
            print("SKIP", f"profit/cluster_{cid}")
            continue
        orignal_std_out = sys.stdout
        calculator = ClusterProfitCalculator(dex=uni_dex)
        sys.stdout = open(f"profit/cluster_{cid}.txt", "w")
        calculator.calculate(f"cluster_{cid}")
        # break
        sys.stdout = orignal_std_out
