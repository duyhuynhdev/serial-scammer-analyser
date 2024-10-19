# Cluster Profit Calculator

This tool calculates the true profit for scam clusters on decentralized exchanges (DEXs) like Uniswap (v2) and PancakeSwap (v2). It analyzes token mints, burns, withdrawals, and fees within clusters of pools and nodes.

## Features

- **Single Cluster Calculation:** Calculates profit for a specific cluster.
- **Batch Processing:** Supports calculating profits for multiple clusters.
- **Detailed Pool Analysis:** Analyzes pools and transactions, distinguishing between legitimate and scam nodes.

## Usage
### Single Cluster Calculation
1. Specify the decentralized exchange (DEX) and cluster name in the code:
```python
if __name__ == "__main__":
    uni_dex = "univ2"  # for Uniswap v2
    pancake_dex = "panv2"  # for PancakeSwap v2
    calculator = ClusterProfitCalculator(dex=uni_dex)
    calculator.calculate("cluster_126")
```
The cluster_name should refer to a CSV file (without the .csv extension) located in the resources/data/{dex name}/processed/cluster directory.

2. To calculate profit for a single cluster, run the following code:
```sh
python main/algorithms/ClusterProfitCalculator.py
```

### Batch Cluster Calculation
1. To calculate profits for multiple clusters at once, modify the following script:
```python
if __name__ == "__main__":
    cluster_names = ["cluster_126", "cluster_127", "cluster_128"]
    calculator = ClusterProfitCalculator(dex=uni_dex)
    calculator.calculate_batch(cluster_names)
```

2. Finally, run the following code:
```sh
python main/algorithms/ClusterProfitCalculator.py
```
