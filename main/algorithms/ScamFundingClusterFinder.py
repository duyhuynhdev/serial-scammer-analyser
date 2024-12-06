import copy
import sys
import os

import queue

import numpy as np
import pandas as pd
import pickle
import networkx as nx
import time


sys.path.append(os.path.join(os.path.dirname(sys.path[0])))

from utils.Utils import  TransactionUtils, Constant
from data_collection.AccountCollector import TransactionCollector
from utils.DataLoader import DataLoader, load_light_pool
from utils.ProjectPath import ProjectPath
from entity.blockchain import Transaction


dex = 'panv2'
dataloader = DataLoader(dex=dex)
path = ProjectPath()
transaction_collector = TransactionCollector()

all_funding_txs = set()
all_funding_tx_hashes = set()
all_scammer_addrs = set()
visited_tx = set()
atomic_MSF_groups = []
F_txs = {}
B_txs = {}

def get_first_add_last_remove_lqd_txs(scammer_addr):
    first_add_timestamp = 0
    first_add_amount = 0
    last_remove_timestamp = 0
    last_remove_amount = 0
    found_add = False
    found_rev = False

    def calc_liquidity_amount(event, use_value):
        return event.amount0 / 10 ** 18 if use_value == 0 else event.amount1 / 10 ** 18

    # t = time.time()
    scammer_pools = load_light_pool(scammer_addr, dataloader, dex)
    # print(f"Time to load pool {time.time() - t}")

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
            first_add_timestamp = tx.timeStamp
            first_add_amount = all_add_lqd_trans[tx]

    max = 0
    for tx in all_remove_lqd_trans.keys():
        if int(tx.timeStamp) > max:
            max = int(tx.timeStamp)
            last_remove_timestamp = tx.timeStamp
            last_remove_amount = all_remove_lqd_trans[tx]

    if first_add_timestamp > 0 and first_add_amount > 0:
        found_add = True
    if last_remove_timestamp > 0 and last_remove_timestamp > 0:
        found_rev = True

    return found_add, first_add_timestamp, first_add_amount, found_rev, last_remove_timestamp, last_remove_amount

def get_first_add_last_remove_lqd_txs_decoder(normal_txs, internal_txs):
    first_add_timestamp = 0
    first_add_amount = 0
    last_remove_timestamp = 0
    last_remove_amount = 0
    found_add = False
    found_rev = False

    min = 10e14
    max = 0
    for normal_tx in normal_txs:
        if TransactionUtils.is_scam_add_liq(normal_tx, dataloader):
            if int(normal_tx.timeStamp) < min:
                min = int(normal_tx.timeStamp)
                first_add_amount = TransactionUtils.get_add_liq_amount(normal_tx, normal_txs, dataloader)
                # print(f"\tFOUND SCAM LIQ ADDING {normal_tx.hash} WITH ADDED AMOUNT iS {amount}")
                if first_add_amount > 0:
                    found_add = True
                    first_add_timestamp = min

        if TransactionUtils.is_scam_remove_liq(normal_tx, dataloader):
            if int(normal_tx.timeStamp) > max:
                max = int(normal_tx.timeStamp)
                last_remove_amount = TransactionUtils.get_related_amount_from_internal_txs(normal_tx, normal_txs, internal_txs)
                # print(f"\tFOUND SCAM LIQ REMOVAL {normal_tx.hash} WITH REMOVED AMOUNT iS {amount}")
                if last_remove_amount > 0:
                    found_rev = True
                    last_remove_timestamp = max
    return found_add, first_add_timestamp, first_add_amount, found_rev, last_remove_timestamp, last_remove_amount

def get_valid_funding_txs(all_scammmer_addrs):
    global all_funding_txs, all_funding_tx_hashes, F_txs, B_txs
    tmp_txs = set()
    # t = time.time()
    for index, scammer_addr in enumerate(all_scammmer_addrs):
        if index % 100 == 0:
            print({f"Done till scammer index = {index}"})
        normal_txs, internal_txs = transaction_collector.get_transactions(scammer_addr, dex=dex)
        found_add, first_add_timestamp, funding_value, found_rev, last_remove_timestamp, revenue_value = get_first_add_last_remove_lqd_txs_decoder(normal_txs, internal_txs)
        # if not found_add:
        #     print(f"Scammer {index}: {scammer_addr} has no first add lqd")
        #     F_txs[scammer_addr] = []
        # if not found_rev:
        #     print(f"Scammer {index}: {scammer_addr} has no last remove lqd")
        #     B_txs[scammer_addr] = []
        if (not found_add) or (not found_rev):
            found_add, first_add_timestamp, funding_value, found_rev, last_remove_timestamp, revenue_value = get_first_add_last_remove_lqd_txs(
                scammer_addr)
            if not found_add:
                print(f"Scammer {index}: {scammer_addr} has no first add lqd")
                F_txs[scammer_addr] = []
            if not found_rev:
                print(f"Scammer {index}: {scammer_addr} has no last remove lqd")
                B_txs[scammer_addr] = []
        if not found_add and not found_rev:
            continue
        # print(f"Time to get first add lqd and last remove lqd {time.time() - t}")
        funding_value = funding_value * (10 ** Constant.WETH_BNB_DECIMALS)
        revenue_value = revenue_value * (10 ** Constant.WETH_BNB_DECIMALS)

        B_txs[scammer_addr] = []
        F_txs[scammer_addr] = []
        # get txs_in s.t: tx in, tx.sender in all scammer list, before first scam tx of scammer_addr
        # get txs_out s.t: tx out, tx.to in all scammber list, after last scam tx of scammer_addr
        txs_in = []
        txs_out = []
        if found_add:
            for tx in normal_txs:
                if tx.is_in_tx(scammer_addr):
                    if tx.sender != scammer_addr and tx.sender in all_scammmer_addrs and int(tx.timeStamp) < int(first_add_timestamp):
                        txs_in.append(tx)
            # sort txs_in w.r.t values (descending order)
            txs_in_values = [float(tx.value) for tx in txs_in]
            sorted_txs_in = [x for _, x in
                             sorted(zip(txs_in_values, txs_in), key=lambda pair: pair[0], reverse=True)]
            # get the top tx_in that the sum of values cover the funding_value
            f_txs = []
            sum_in = 0
            for tx in sorted_txs_in:
                if sum_in >= funding_value:
                    break
                sum_in += float(tx.value)
                f_txs.append(tx)
            if sum_in >= funding_value:
                F_txs[scammer_addr] = f_txs  # update funding txs to scammer addr (funders of scammer addr)
                tmp_txs.update(f_txs)

        if found_rev:
            for tx in normal_txs:
                if tx.is_out_tx(scammer_addr):
                    if tx.to != scammer_addr and tx.to in all_scammmer_addrs and int(tx.timeStamp) > int(last_remove_timestamp):
                        txs_out.append(tx)
            txs_out_values = [float(tx.value) for tx in txs_out]
            sorted_txs_out = [x for _, x in sorted(zip(txs_out_values, txs_out), key=lambda pair: pair[0], reverse=True)]
            # get the top tx_out that the sum of values is greater than 0.9 revenue_value
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
        self.inputs = set()
        self.outputs = set()
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
            self.inputs.add(sender)
            self.outputs.add(receiver)

            if all(b_tx.hash in all_funding_tx_hashes for b_tx in B_txs[sender]):
                for b_tx in B_txs[sender]:
                    if b_tx.hash in all_funding_tx_hashes and b_tx.hash not in visited_tx:
                        q.put(b_tx)
            if all(f_tx.hash in all_funding_tx_hashes for f_tx in F_txs[receiver]):
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
        self.E = copy.deepcopy(E_)

        self.inputs = self.inputs.union(other_group.inputs) - self.V.intersection(other_group.V)
        self.outputs = self.outputs.union(other_group.outputs) - self.V.intersection(other_group.V)

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
            if group1.id != group2.id and not group1.V.isdisjoint(group2.V):
                graph.add_edge(group1, group2)
                graph.add_edge(group2, group1)

    connected_components = list(nx.connected_components(graph))
    MSF_clusters = []
    for cc in connected_components:
        cc = list(cc)
        msf_cluster = copy.deepcopy(cc[0]) # using deep copy
        for i in range(1, len(cc)):
            msf_cluster.merge(cc[i])
        MSF_clusters.append(msf_cluster)
    return connected_components, MSF_clusters

# def test_remove_lqd_detector():
#     scammer_addrs = ["0x9a3a50f4d0df8dae8fe97e89edd2a39b51c86997", \
#                    "0xe7daf02024dfcf0d36ed49a6f9b33beb430edb5b",
#                     "0x62dc0eafe9ff0fe87302491d05cb9446ddf81f3f",
#                     "0x37af1c8c957d017a6b596ef6589923dbba1e2a7a",
#                     "0xc2bfc6ac230664b2ad76f850d46c0bc208df6b6d",
#                     "0x9eafeca178376002a45f936dbfd866185f5697d8",
#                     "0xc20390d3a70f2143ae7c967386ee2b2f1d6d66d2",
#                     "0x30948fe32bbec0b1a76095908f0dd0600d857643",
#                     "0xf88a8c160846afd82801b37a309425403d7c8e87",
#                     "0x612a6b21dbe9ad95857a4d1c5838f4804f3526bd",
#                     "0xbbb8915d99377512773887db6b424fe91b50e4f7"]
#     for addr in scammer_addrs:
#
#         start_time = time.time()
#         print(get_first_add_last_remove_lqd_txs(addr))
#         print(f'Time {time.time() - start_time}')
#         t = time.time()
#         normal_txs, internal_txs = transaction_collector.get_transactions(addr, dex=dex)
#         print(get_first_add_last_remove_lqd_txs_decoder(normal_txs, internal_txs))
#         print(f'Time {time.time() - t}')

if __name__ == '__main__':
    # 1. Test a simple chain example
    # all_scammer_addrs = pd.read_csv("complex_chain_example.txt", header=None)
    # all_scammer_addrs = [s.lower() for s in all_scammer_addrs.to_numpy().flatten().tolist()]

    # # 2. Test group 150
    # group_id = 150
    # scammers_set = set(dataloader.scammers)
    # scammers_group = set(dataloader.group_scammers[group_id])
    # print(f"LOAD {len(scammers_group)} SCAMMER FROM GROUP {group_id}")
    # all_scammer_addrs = scammers_set.intersection(scammers_group)

    # 3. Test all scammers
    all_scammer_addrs = set(dataloader.scammers)
    print(len(all_scammer_addrs))

    # get_valid_funding_txs(all_scammer_addrs)
    # create_atomic_MSF_groups()
    # connected_components, MSF_clusters = find_MSF_clusters(atomic_MSF_groups)

    if not os.path.exists(f"funding_txs_{dex}.pkl"):
        get_valid_funding_txs(all_scammer_addrs)
        with open(f'funding_txs_{dex}.pkl', 'wb') as file:
            pickle.dump((all_funding_txs, all_funding_tx_hashes, F_txs, B_txs), file)
    else:
        with open(f'funding_txs_{dex}.pkl', 'rb') as file:
            all_funding_txs, all_funding_tx_hashes, F_txs, B_txs = pickle.load(file)

    if not os.path.exists(f'atomic_groups_{dex}.pkl'):
        create_atomic_MSF_groups()
        with open(f'atomic_groups_{dex}.pkl', 'wb') as file:
            pickle.dump(atomic_MSF_groups, file)
    else:
        with open(f'atomic_groups_{dex}.pkl', 'rb') as file:
            atomic_MSF_groups = pickle.load(file)

    if not os.path.exists(f'MSF_clusters_{dex}.pkl'):
        connected_components, MSF_clusters = find_MSF_clusters(atomic_MSF_groups)
        with open(f'MSF_clusters_{dex}.pkl', 'wb') as file:
            pickle.dump((connected_components, MSF_clusters), file)
    else:
        with open(f'MSF_clusters_{dex}.pkl', 'rb') as file:
            connected_components, MSF_clusters = pickle.load(file)

    # Statistics
    df = pd.DataFrame(index=range(len(MSF_clusters)),
                              columns=['cluster_id', 'len(V)', 'len(E)', 'len', 'width',
                                       'widest_atomic_group.V', 'widest_atomic_group.E',
                                       'inputs', 'outputs', 'fund_in', 'fund_out'])
    for i, cluster in enumerate(MSF_clusters):
        df.loc[i, 'cluster_id'] = cluster.id
        df.loc[i, 'len(V)'] = len(cluster.V)
        # df.loc[i, 'V'] = cluster.V
        df.loc[i, 'len(E)'] = len(cluster.E)
        # df.loc[i, 'E'] = [e.hash for e in cluster.E]
        df.loc[i, 'len'] = len(connected_components[i])

        widest_atomic_group = list(connected_components[i])[np.argmax([len(atomic_group.V) for atomic_group in connected_components[i]])]
        df.loc[i, 'width'] = len(widest_atomic_group.V)
        df.loc[i, 'widest_atomic_group.V'] = widest_atomic_group.V
        df.loc[i, 'widest_atomic_group.E'] = [e.hash for e in widest_atomic_group.E]

        # determine fund_in, fund_out
        df.loc[i, 'inputs'] = cluster.inputs
        df.loc[i, 'outputs'] = cluster.outputs
        df.loc[i, 'fund_in'] = sum([tx.get_transaction_amount() for v in cluster.inputs for tx in B_txs[v]])
        df.loc[i, 'fund_out'] = sum([tx.get_transaction_amount() for v in cluster.outputs for tx in F_txs[v]])

    df.to_csv(f"MSF_clusters_statistics_{dex}.csv")