import os

import pandas as pd
from tqdm import tqdm

from sql.DataQuerier import DataQuerier
from sql.PostgresDTO import *
from utils.ProjectPath import ProjectPath

path = ProjectPath()


class ClusterProfitCalculator:
    def __init__(self, dex: str = 'univ2'):
        self.querier = DataQuerier(dex=dex)

    def get_all_scammer_pools(self, members):
        """
        Returns a set of scammer pools belonging to the cluster.
        """
        pools = []
        for member in members:
            pools.extend(self.querier.get_scam_pool(member))
        return pools

    def calculate_total_event_value_and_fees(self, items, amount_attr: str):
        """
        Generic method to calculate the total values and fees from a list of transactions.
        :param items: List of transaction objects (mints, burns, swaps).
        :param amount_attr: The attribute of the item to be evaluated for the amount.
        :return: A tuple of total value and total fees.
        """
        total_value, total_fees = 0, 0

        for item in items:
            total_value += float(getattr(item, amount_attr)) / 10 ** Constant.POOL_DECIMALS
            total_fees += (
                    float(item.gas_used)
                    * float(item.gas_price)
                    / 10 ** Constant.WETH_BNB_DECIMALS
            )

        return total_value, total_fees

    def calculate_total_mint_value_and_fees(self, pool: Pool):
        mints = self.querier.get_pool_mint(pool.address)
        return self.calculate_total_event_value_and_fees(
            mints, f"amount{pool.get_high_value_position()}"
        )

    def calculate_total_burn_value_and_fees(self, pool: Pool):
        burns = self.querier.get_pool_burn(pool.address)
        return self.calculate_total_event_value_and_fees(
            burns, f"amount{pool.get_high_value_position()}"
        )

    def calculate_total_investing_value_and_fees_by_addressees(self, pool: Pool, addresses):
        investing_swaps = self.querier.get_pool_investing_swap(pool)
        filtered_investing_swaps = [
            swap for swap in investing_swaps if swap.receiver.lower() in addresses
        ]
        return self.calculate_total_event_value_and_fees(
            filtered_investing_swaps, f"amount{pool.get_high_value_position()}_in"
        )

    def calculate_total_divesting_value_and_fees_by_addressees(self, pool: Pool, addresses):
        divesting_swaps = self.querier.get_pool_devesting_swap(pool)
        filtered_divesting_swaps = [
            swap for swap in divesting_swaps if swap.receiver.lower() in addresses
        ]
        return self.calculate_total_event_value_and_fees(
            filtered_divesting_swaps, f"amount{pool.get_high_value_position()}_out"
        )

    def calculate_token_creation_fee(self, pool: Pool):
        token_creation_fee = 0.0
        creator = pool.creator
        scam_token_address = pool.get_scam_token_address()
        token_creation_tx = self.querier.get_contract_creation_tx(creator, scam_token_address)
        if token_creation_tx:
            token_creation_fee += (
                    token_creation_tx.get_transaction_fee()
                    + token_creation_tx.get_transaction_amount()
            )
        return token_creation_fee

    def calculate_x_per_pool(self, pool):
        """
              Calculates the total value (X) for a given pool by evaluating its
              mint value, associated fees, and token creation fee.

              Note that the pool creation event is the same as the mint event; there is no need to add
              the pool creation transaction amount and fee.
              """

        x = 0.0

        mint_total, fee_total = self.calculate_total_mint_value_and_fees(pool)
        token_creation_fee = self.calculate_token_creation_fee(pool)

        x += mint_total + fee_total + token_creation_fee

        return x

    def calculate_y_per_pool(self, pool: Pool, members) -> float:
        """
        Calculates the total value (y) of a pool.

        This function evaluates the total burn value and associated fees, as well as the total
        rug-pulling withdrawal value and fees by nodes in the scam cluster.
        """
        y = 0.0

        burn_total, fee_total = self.calculate_total_burn_value_and_fees(pool)
        y += burn_total - fee_total

        rug_pulling_withdrawal_total, fee_total = (
            self.calculate_total_divesting_value_and_fees_by_addressees(
                pool,
                members
            )
        )
        y += rug_pulling_withdrawal_total - fee_total

        return y

    def calculate_z_per_pool(self, pool: Pool, members) -> float:
        """
        Calculate the total 'z' value for a pool by summing disingenuous investing value and fees
        by nodes in the scam cluster.
        """
        z = 0.0

        disingenuous_investing_value_total, fee_total = (
            self.calculate_total_investing_value_and_fees_by_addressees(
                pool, members
            )
        )
        z += disingenuous_investing_value_total + fee_total
        return z

    def get_profit_metrics_per_pool(self, pool: Pool, group_id, members):
        """
        Calculate the true profit made in a given pool.
        """
        y = self.calculate_y_per_pool(pool, members) * 1.0
        x = self.calculate_x_per_pool(pool) * 1.0
        z = self.calculate_z_per_pool(pool, members) * 1.0
        true_profit = (y - x - z) * 1.0
        naive_profit = (y - x) * 1.0
        return {
            "group_id": group_id,
            "pool_address": pool.address.lower(),
            "y_revenue": y,
            "x_cost": x,
            "z_wash_trade": z,
            "naive_profit": naive_profit,
            "true_profit": true_profit,
        }

    def calculate(self, group_id, members):
        data = []
        scam_pools = self.get_all_scammer_pools(members)
        for pool in scam_pools:
            data.append(self.get_profit_metrics_per_pool(pool,group_id, members))
        return data


def calculate_cluster_profit(dex='univ2'):
    df = pd.read_csv(os.path.join(eval(f"path.{dex}_processed_path"), "sql_scammer_group.csv"))
    grouped = df.groupby('group_id')
    calculator = ClusterProfitCalculator(dex=dex)
    result = []
    for name, group in tqdm(grouped):
        members = group["scammer"].unique()
        data = calculator.calculate(name, members)
        result.extend(data)
    out_df = pd.DataFrame(result)
    out_df.to_csv(os.path.join(eval(f"path.{dex}_processed_path"), "cluster_awareness_profit.csv"), index=False)


if __name__ == '__main__':
    calculate_cluster_profit(dex='univ2')
    # calculate_cluster_profit(dex='panv2')
    # calculator = ClusterProfitCalculator(dex='panv2')
    # pool = calculator.querier.get_pool_by_address("0xf6fe7fb58da14e0e0d1c49c985e71a5cd67c157a")
    # y = calculator.calculate_y_per_pool(pool[0],[])
    # print(y)