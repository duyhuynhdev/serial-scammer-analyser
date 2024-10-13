import copy
import sys
import os

import queue

import numpy as np
import pandas as pd
import pickle
import networkx as nx
import time
from ordered_set import OrderedSet


sys.path.append(os.path.join(os.path.dirname(sys.path[0])))

from utils.Utils import  TransactionUtils, Constant
from data_collection.AccountCollector import TransactionCollector
from utils.DataLoader import DataLoader, load_pool
from utils.ProjectPath import ProjectPath
from entity.blockchain import Transaction



dataloader = DataLoader()
path = ProjectPath()
transaction_collector = TransactionCollector()

all_funding_txs = set()
all_funding_tx_hashes = set()
all_scammer_addrs = set(dataloader.scammers)
visited_tx = set()
atomic_MSF_groups = []
F_txs = {}
B_txs = {}

def get_first_add_last_remove_lqd_txs(scammer_addr):
    # first_add_lqd_tran = None
    # first_add_amount = None
    # last_remove_lqd_tran = None
    # last_remove_amount = None
    first_add_lqd_tran = Transaction()
    first_add_amount = Transaction
    last_remove_lqd_tran = Transaction()
    last_remove_amount = Transaction()
    def calc_liquidity_amount(event, use_value):
        return event.amount0 / 10 ** 18 if use_value == 0 else event.amount1 / 10 ** 18

    t = time.time()
    scammer_pools = load_pool(scammer_addr, dataloader)
    print(f"Time to load pool {time.time() - t}")

    all_add_lqd_trans = {}
    all_remove_lqd_trans = {}


    for pool_index in range(len(scammer_pools)):
        eth_pos = scammer_pools[pool_index].get_high_value_position()
        add_lqd_trans = scammer_pools[pool_index].mints
        for tx in add_lqd_trans:
            all_add_lqd_trans[tx] = calc_liquidity_amount(tx, eth_pos)
        remove_lqd_trans = scammer_pools[pool_index].burns
        for tx in remove_lqd_trans:
            all_remove_lqd_trans[tx] = calc_liquidity_amount(tx, eth_pos)

    # get add_lqd_trans with min timestamp (first_add_lqd_tran) and remove_lqd_trans with max timestamp (last_remove_lqd_tran)
    min = 10e14
    for tx in all_add_lqd_trans.keys():
        if int(tx.timeStamp) < min:
            min = int(tx.timeStamp)
            first_add_lqd_tran = tx
            first_add_amount = all_add_lqd_trans[tx]

    max = 0
    for tx in all_remove_lqd_trans.keys():
        if int(tx.timeStamp) > max:
            max = int(tx.timeStamp)
            last_remove_lqd_tran = tx
            last_remove_amount = all_remove_lqd_trans[tx]

    return first_add_lqd_tran, first_add_amount, last_remove_lqd_tran, last_remove_amount

def get_first_add_last_remove_lqd_txs_decoder(normal_txs, internal_txs):
    first_add_lqd_tran = None
    first_add_amount = None
    last_remove_lqd_tran = None
    last_remove_amount = None
    min = 10e14
    max = 0
    for normal_tx in normal_txs:
        if TransactionUtils.is_scam_add_liq(normal_tx, dataloader):
            if int(normal_tx.timeStamp) < min:
                min = int(normal_tx.timeStamp)
                first_add_lqd_tran = normal_tx
                first_add_amount = normal_tx.get_transaction_amount()

        if TransactionUtils.is_scam_remove_liq(normal_tx, dataloader):
            if int(normal_tx.timeStamp) > max:
                max = int(normal_tx.timeStamp)
                last_remove_lqd_tran = normal_tx
                last_remove_amount = TransactionUtils.get_related_amount_from_internal_txs(normal_tx, normal_txs, internal_txs)
    return first_add_lqd_tran, first_add_amount, last_remove_lqd_tran, last_remove_amount

def get_valid_funding_txs(all_scammmer_addrs):
    global all_funding_txs, all_funding_tx_hashes, F_txs, B_txs
    tmp_txs = set()
    count_no_lqd = 0
    count_lqd = 0
    for scammer_addr in all_scammmer_addrs:
        B_txs[scammer_addr] = []
        F_txs[scammer_addr] = []
        # print(f'Scammer {index}')
        # scammer_addr = '0xB2b1f0De0B5Bcb234d4c60378a5Bbd2c8aCC3096'
        normal_txs, internal_txs = transaction_collector.get_transactions(scammer_addr, dex='univ2')
        # t = time.time()
        # first_add_lqd_tx, funding_value, last_remove_lqd_tx, revenue_value = get_first_add_last_remove_lqd_txs(scammer_addr)
        first_add_lqd_tx, funding_value, last_remove_lqd_tx, revenue_value = get_first_add_last_remove_lqd_txs_decoder(normal_txs, internal_txs)
        # print(f"Time to get first add lqd and last remove lqd {time.time() - t}")
        if first_add_lqd_tx == None or funding_value == None or last_remove_lqd_tx == None or revenue_value == None:
            print(f"Scammer {scammer_addr} has no first_add_lqd_tx or no last_remove_lqd_txs")
            count_no_lqd += 1
            continue
        count_lqd += 1
        print(f'Scammer {scammer_addr}')
        funding_value = funding_value * (10 ** Constant.WETH_BNB_DECIMALS)
        revenue_value = revenue_value * (10 ** Constant.WETH_BNB_DECIMALS)

        # get txs_in s.t: tx in, tx.sender in all scammer list, before first scam tx of scammer_addr
        # get txs_out s.t: tx out, tx.to in all scammber list, after last scam tx of scammer_addr
        txs_in = []
        txs_out = []
        for tx in normal_txs:
            if tx.is_in_tx(scammer_addr):
                if tx.sender != scammer_addr and tx.sender in all_scammmer_addrs and int(tx.timeStamp) < int(first_add_lqd_tx.timeStamp):
                    txs_in.append(tx)
            if tx.is_out_tx(scammer_addr):
                if tx.to != scammer_addr and tx.to in all_scammmer_addrs and int(tx.timeStamp) > int(last_remove_lqd_tx.timeStamp):
                    txs_out.append(tx)

        # sort txs_in, txs_out w.r.t values (descending order)
        txs_in_values = [float(tx.value) for tx in txs_in]
        sorted_txs_in = [x for _, x in sorted(zip(txs_in_values, txs_in), key=lambda pair: pair[0], reverse=True)]

        txs_out_values = [float(tx.value) for tx in txs_out]
        sorted_txs_out = [x for _, x in sorted(zip(txs_out_values, txs_out), key=lambda pair: pair[0], reverse=True)]

        # get the top tx_in that the sum of values cover the funding_value
        f_txs = []
        sum_in = 0
        for tx in sorted_txs_in:
            if sum_in >= funding_value:
                break
            sum_in += float(tx.value)
            f_txs.append(tx)
        if sum_in >= funding_value:
            F_txs[scammer_addr] = f_txs # update funding txs to scammer addr (funders of scammer addr)
            tmp_txs.update(f_txs)

        # and get the top tx_out that the sum of values is greater than 0.9 revenue_value
        b_txs = []
        sum_out = 0
        for tx in sorted_txs_out:
            if sum_out >= 0.9 * revenue_value:
                break
            sum_out += float(tx.value)
            b_txs.append(tx)
        if sum_out >= 0.9 * revenue_value:
            B_txs[scammer_addr] = b_txs # update funding txs from scammer addr (beneficiaries of scammer addr)
            tmp_txs.update(b_txs)

    print(f'Number of scammer has no first_add_lqd_tx or no last_remove_lqd_tx: {count_no_lqd}')
    print(f'Number of scammer has both first_add_lqd_tx and no last_remove_lqd_tx: {count_lqd}')
    for tx in tmp_txs:
        sender = tx.sender
        to = tx.to
        if tx.hash in [tx_.hash for tx_ in B_txs[sender]] and tx.hash in [tx_.hash for tx_ in F_txs[to]]:
            if tx.hash not in all_funding_tx_hashes:
                all_funding_tx_hashes.add(tx.hash)
                all_funding_txs.add(tx)

class MaximalScamFundingCluster():
    def __init__(self, tx_id):
        self.id = tx_id.hash
        self.V = set()
        self.E = set()
        q = queue.Queue()
        q.put(tx_id)
        # only valid funding txs in queue
        while not q.empty():
            tx = q.get()
            self.E.add(tx)
            sender = tx.sender
            receiver = tx.to
            visited_tx.add(tx.hash)
            self.V.add(sender)
            self.V.add(receiver)

            for b_tx in B_txs[sender]:
                if b_tx.hash in all_funding_tx_hashes and b_tx.hash not in visited_tx:
                    q.put(b_tx)

            for f_tx in F_txs[receiver]:
                if f_tx.hash in all_funding_tx_hashes and f_tx.hash not in visited_tx:
                    q.put(f_tx)

    def merge(self, other_group):
        E_hashes = set()
        E_ = set()
        for e in self.E.union(other_group.E):
            if e.hash not in E_hashes:
                E_hashes.add(e.hash)
                E_.add(e)
        self.E = E_
        self.V = self.V.union(other_group.V)

def create_atomic_MSF_groups():
    for tx in all_funding_txs:
        if tx.hash not in visited_tx:
            atomic_MSF_groups.append(MaximalScamFundingCluster(tx))

def find_MSF_clusters(atomic_MSF_groups):
    graph = nx.Graph()
    for group in atomic_MSF_groups:
        graph.add_node(group)
    for group1 in atomic_MSF_groups:
        for group2 in atomic_MSF_groups:
            if len(group1.V.intersection(group2.V)) != 0:
                graph.add_edge(group1, group2)

    connected_components = list(nx.connected_components(graph))
    MSF_clusters = []
    for cc in connected_components:
        cc = list(cc)
        msf_cluster = copy.deepcopy(cc[0]) # using deep copy
        for i in range(1, len(cc)):
            msf_cluster.merge(cc[i])
        MSF_clusters.append(msf_cluster)
    return connected_components, MSF_clusters

if __name__ == '__main__':
    # dex = "univ2"
    # address = "0x9a3a50f4d0df8dae8fe97e89edd2a39b51c86997"
    # dataloader = DataLoader(dex=dex)
    # normal_txs, internal_txs = transaction_collector.get_transactions(address, dex=dex, key_idx=17)
    # for normal_tx in normal_txs:
    #     # print("Function name: ",normal_tx.functionName, " is matched", True if "addLiquidity" in str(normal_tx.functionName) or "removeLiquidity" in str(normal_tx.functionName) else False)
    #     if TransactionUtils.is_scam_add_liq(normal_tx, dataloader):
    #         amount = normal_tx.get_transaction_amount()
    #         print(f"\tFOUND SCAM LIQ ADDING {normal_tx.hash} WITH ADDED AMOUNT iS {amount}")
    #
    #     if TransactionUtils.is_scam_remove_liq(normal_tx, dataloader):
    #         amount = TransactionUtils.get_related_amount_from_internal_txs(normal_tx, internal_txs)
    #         print(f"\tFOUND SCAM LIQ REMOVAL {normal_tx.hash} WITH REMOVED AMOUNT iS {amount}")

    start_time = time.time()
    # all_scammer_addrs = pd.read_csv("complex_chain_example.txt", header=None)
    # all_scammer_addrs = [s.lower() for s in all_scammer_addrs.to_numpy().flatten().tolist()]
    # # print(len(all_scammer_addrs))
    # all_scammer_addrs = list(OrderedSet(all_scammer_addrs))[:10]

    # group_id = 150
    # scammers_set = set(dataloader.scammers)
    # scammers_group = set(dataloader.group_scammers[group_id])
    # print(f"LOAD {len(scammers_group)} SCAMMER FROM GROUP {group_id}")
    # all_scammer_addrs = scammers_set.intersection(scammers_group)

    print(len(all_scammer_addrs))

    if not os.path.exists('funding_txs.pkl'):
        get_valid_funding_txs(all_scammer_addrs)
        with open('funding_txs.pkl', 'wb') as file:
            pickle.dump((all_funding_txs, F_txs, B_txs), file)
    else:
        with open('funding_txs.pkl', 'rb') as file:
            all_funding_txs, F_txs, B_txs = pickle.load(file)
    print(f"Getting valid funding txs in {time.time() - start_time}")

    t = time.time()
    create_atomic_MSF_groups()

    # if not os.path.exists('atomic_groups.pkl'):
    #     create_atomic_MSF_groups()
    #     with open('atomic_groups.pkl', 'wb') as file:
    #         pickle.dump(atomic_MSF_groups, file)
    # else:
    #     with open('atomic_groups.pkl', 'rb') as file:
    #         atomic_MSF_groups = pickle.load(file)
    # print(f"Creating atomic MSF groups in {time.time() - t}")

    t = time.time()
    connected_components, MSF_clusters = find_MSF_clusters(atomic_MSF_groups)

    # if not os.path.exists('MSF_clusters.pkl'):
    #     connected_components, MSF_clusters = find_MSF_clusters(atomic_MSF_groups)
    #     with open('MSF_clusters.pkl', 'wb') as file:
    #         pickle.dump((connected_components, MSF_clusters), file)
    # else:
    #     with open('MSF_clusters.pkl', 'rb') as file:
    #         connected_components, MSF_clusters = pickle.load(file)

    print(f"Finding connected components MSF_clusters in {time.time() - t}")

    print(f"Total time finding MSF_clusters is {time.time() - start_time}")

    # Statistics
    df = pd.DataFrame(index=range(len(MSF_clusters)),
                              columns=['cluster_id', 'len(V)', 'V', 'len(E)', 'E', 'len', 'width',
                                       'widest_atomic_group.V', 'widest_atomic_group.E'])
                                       # 'inputs', 'outputs', 'fund_in', 'fund_out'])
    all_funding_tx_hashes = set([tx.hash for tx in all_funding_txs])
    for i, cluster in enumerate(MSF_clusters):
        df.loc[i, 'cluster_id'] = cluster.id
        df.loc[i, 'len(V)'] = len(cluster.V)
        df.loc[i, 'V'] = cluster.V
        df.loc[i, 'len(E)'] = len(cluster.E)
        df.loc[i, 'E'] = [e.hash for e in cluster.E]
        df.loc[i, 'len'] = len(connected_components[i])

        widest_atomic_group = list(connected_components[i])[np.argmax([len(atomic_group.V) for atomic_group in connected_components[i]])]
        df.loc[i, 'width'] = len(widest_atomic_group.V)
        df.loc[i, 'widest_atomic_group.V'] = widest_atomic_group.V
        df.loc[i, 'widest_atomic_group.E'] = [e.hash for e in widest_atomic_group.E]

        # # determine inputs, outputs
        # inputs = []
        # outputs = []
        # for v in cluster.V:
        #     if v in F_txs.keys():
        #         # if set([tx.hash for tx in F_txs[v]]).isdisjoint(all_funding_tx_hashes):
        #         if len(F_txs[v]) == 0 and len(B_txs[v]) > 0:
        #             inputs.append(v)
        #     else:
        #         inputs.append(v)
        #     if v in B_txs.keys():
        #         if set([tx.hash for tx in B_txs[v]]).isdisjoint(all_funding_tx_hashes):
        #             outputs.append(v)
        #     else:
        #         outputs.append(v)
        # df.loc[i, 'inputs'] = inputs
        # df.loc[i, 'outputs'] = outputs
        #
        # # determine fund_in, fund_out
        # df.loc[i, 'fund_in'] = sum([tx.get_transaction_amount() for v in inputs if v in B_txs.keys() for tx in B_txs[v]])
        # df.loc[i, 'fund_out'] = sum([tx.get_transaction_amount() for v in outputs if v in F_txs.keys() for tx in F_txs[v]])

    df.to_csv('MSF_clusters_statistics.csv')
