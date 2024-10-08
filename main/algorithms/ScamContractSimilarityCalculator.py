import sys
import os
from collections import Counter

import numpy as np
import pandas as pd
from tqdm import tqdm
import random
from utils import Utils as ut
from similarity import ContractTokenization
from utils import DataLoader

sys.path.append(os.path.join(os.path.dirname(sys.path[0])))
from utils.Settings import Setting
from utils.ProjectPath import ProjectPath

path = ProjectPath()
setting = Setting()


def jaccard_similarity(list1, list2):
    counter1 = Counter(list1)
    counter2 = Counter(list2)
    intersection = list((counter1 & counter2).elements())
    union = list((counter1 | counter2).elements())
    similarity = len(intersection) / len(union) if len(union) > 0 else 0
    return similarity


def create_similarity_dictionary(tokenized_contracts):
    similarity_dictionary = {}
    for address in tokenized_contracts.keys():
        similarity_dictionary[address] = {}
    return similarity_dictionary


def compare_similarities(tokenized_contracts, min_required_similarity=0):
    similarities = create_similarity_dictionary(tokenized_contracts)

    print("Calculating similarities")
    contract_list = list(tokenized_contracts.keys())
    progress = tqdm(total=len(contract_list))
    while len(contract_list) > 0:
        address = contract_list.pop()
        for comparison_address in contract_list:
            similarity_score = jaccard_similarity(tokenized_contracts[address], tokenized_contracts[comparison_address])
            if similarity_score >= min_required_similarity:
                similarities[address][comparison_address] = similarity_score
                similarities[comparison_address][address] = similarity_score
        progress.update(1)
    progress.close()
    return similarities


def compare_similarities_between_sets(tokenized_contracts, tokenized_contracts2, min_required_similarity=0):
    similarities = create_similarity_dictionary(tokenized_contracts)

    print("Calculating similarities")
    contract_list = list(tokenized_contracts.keys())
    contract_list2 = list(tokenized_contracts2.keys())
    while len(contract_list) > 0:
        address = contract_list.pop()
        for comparison_address in contract_list2:
            similarity_score = jaccard_similarity(tokenized_contracts[address], tokenized_contracts2[comparison_address])
            if similarity_score >= min_required_similarity:
                similarities[address][comparison_address] = similarity_score
    return similarities


def load_scammer_tokens(dex='univ2'):
    scammer_df = pd.read_csv(os.path.join(eval("path.{}_processed_path".format(dex)), "filtered_simple_rp_scammers.csv"))
    scammer_pools = scammer_df.groupby("scammer")["pool"].apply(list).to_dict()
    rp_pools = pd.read_csv(os.path.join(eval("path.{}_processed_path".format(dex)), "filtered_simple_rp_pool.csv"))
    rp_pools.fillna("", inplace=True)
    rp_pools = rp_pools[rp_pools["is_rp"] != 0]
    pool_token = dict(zip(rp_pools["pool"], rp_pools["scam_token"]))
    scammer_tokens = dict()
    for s in scammer_pools.keys():
        tokens = [pool_token[pool] for pool in scammer_pools[s] if s in scammer_pools.keys() and pool in pool_token.keys()]
        scammer_tokens[s] = tokens
    return scammer_tokens


def load_data(dex='univ2'):
    scammer_tokens = load_scammer_tokens(dex)
    group_tokens = dict()
    group_scammers = dict()
    file_path = os.path.join(eval('path.{}_processed_path'.format(dex)), "non_swap_simple_rp_scammer_group.csv")
    if os.path.exists(file_path):
        groups = pd.read_csv(file_path)
        group_scammers = groups.groupby("group_id")["scammer"].apply(list).to_dict()
        for idx, row in groups.iterrows():
            group_id = row["group_id"]
            scammer = row["scammer"]
            sts = scammer_tokens[scammer]
            if group_id not in group_tokens:
                group_tokens[group_id] = list()
            group_tokens[group_id].extend(sts)
    return group_tokens, group_scammers


def get_available_hash_data(addresses, dex='univ2'):
    tokenization_path = eval(f"path.{dex}_tokenization_path")
    available_tokens = dict()
    for token_address in addresses:
        token_address = token_address.lower()
        hash_file = os.path.join(tokenization_path, f"{token_address}.hash")
        if os.path.exists(hash_file):
            hashes = ut.read_list_from_file(hash_file)
            available_tokens[token_address] = hashes
    return available_tokens


def pruning_data(available_tokens, limit=100):
    max_items = dict()
    rand_items = set()
    if len(available_tokens) > limit:
        for i in range(limit):
            rand = random.randint(0, len(available_tokens) - 1)
            while rand in rand_items:
                rand = random.randint(0, len(available_tokens) - 1)
            key = list(available_tokens.keys())[rand]
            value = available_tokens[key]
            max_items[key] = value
            rand_items.add(rand)
    else:
        max_items = available_tokens
    return max_items


def intra_cluster_similarity(group_id, group_tokens, dex='univ2', prefix="", limit=10000):
    similarity_path = eval(f"path.{dex}_intra_similarity_path")
    similarity_file = os.path.join(similarity_path, f"{prefix}intra_{group_id}_similarity.json")
    if len(group_tokens) <= 1:
        return {}
    available_tokens = get_available_hash_data(group_tokens, dex=dex)
    print("\t AVAILABLE AST SIZE:", len(available_tokens))
    if len(available_tokens) <= 1:
        return {}
    if len(available_tokens) > limit:
        available_tokens = pruning_data(available_tokens, limit)
        print("\t PRUNNING AST SIZE:", len(available_tokens))
    similarites = compare_similarities(available_tokens, min_required_similarity=0)
    ut.write_json(similarity_file, similarites)
    print("\t SAVED SIM TO ", similarity_file)
    return similarites


def inter_cluster_similarity(group_1_id, group_1_tokens, group_tokens, dex='univ2', limit=1000):
    similarity_path = eval(f"path.{dex}_inter_similarity_path")
    similarity_file = os.path.join(similarity_path, f"inter_{group_1_id}_similarity.json")
    similarites = dict()
    print("*" * 100)
    if len(group_1_tokens) == 0 :
        return {}
    for group_2_id, group_2_tokens in group_tokens.items():
        print("-" * 50)
        if len(group_2_tokens) == 0 or group_1_id == group_2_id:
            print("EMPTY TOKENS >>> SKIP")
            continue
        g_1_available_tokens = get_available_hash_data(group_1_tokens, dex=dex)
        g_2_available_tokens = get_available_hash_data(group_2_tokens, dex=dex)
        print(f"\t GROUP {group_1_id} AVAILABLE AST SIZE:", len(g_1_available_tokens))
        print(f"\t GROUP {group_2_id} AVAILABLE AST SIZE:", len(g_2_available_tokens))
        if len(g_1_available_tokens) == 0 or len(g_2_available_tokens) == 0:
            print("EMPTY AST >>> SKIP")
            continue
        if len(g_1_available_tokens) > limit:
            g_1_available_tokens = pruning_data(g_1_available_tokens, limit)
            print(f"\t GROUP {group_1_id} PRUNNING AST SIZE:", len(g_1_available_tokens))
        if len(g_2_available_tokens) > limit:
            g_2_available_tokens = pruning_data(g_2_available_tokens, limit)
            print(f"\t GROUP {group_2_id} PRUNNING AST SIZE:", len(g_2_available_tokens))
        similarites[group_2_id] = compare_similarities_between_sets(g_1_available_tokens, g_2_available_tokens, min_required_similarity=0)
    ut.write_json(similarity_file, similarites)
    print("\t SAVED SIM TO ", similarity_file)
    print("*"*100)
    return similarites


def calculate_sim(gid, dex, prefix=""):
    similarity_path = eval(f"path.{dex}_intra_similarity_path")
    similarity_file = os.path.join(similarity_path, f"{prefix}_intra_{gid}_similarity.json")
    values = []
    if os.path.exists(similarity_file):
        sims = ut.read_json(similarity_file)
        for k, pairs in sims.items():
            for v in pairs.values():
                values.append(v)
        print("GID:", gid, "SIZE", len(sims), "AVG SIM", np.mean(values))


def calculate_intra_avg_sim(group_tokens, group_scammers, dex='univ2', prefix=""):
    global_sim = []
    sim_data = []
    for gid, tokens in tqdm(group_tokens.items()):
        scammers = group_scammers[gid]
        similarity_path = eval(f"path.{dex}_intra_similarity_path")
        similarity_file = os.path.join(similarity_path, f"{prefix}_intra_{gid}_similarity.json")
        values = []
        sims = []
        if os.path.exists(similarity_file):
            sims = ut.read_json(similarity_file)
            for k, pairs in sims.items():
                for v in pairs.values():
                    values.append(v)
            avg = np.mean(values)
            global_sim.append(avg)
            record = {"group_id:": gid, "scammers": len(scammers), "tokens": len(tokens), "available_tokens": len(sims), "intra_similarity": avg}
            sim_data.append(record)
            print(record)
    df = pd.DataFrame(sim_data)
    df.to_csv(f"{prefix}_intra_similarities.csv", index=False)
    print("TOTAL", len(global_sim), "WITH AVG SIM", np.mean(global_sim))

def generate_intra_sim(group_tokens, group_scammers, dex='univ2'):
    print("GENERATE PAIRS SIM")
    for gid, tokens in tqdm(group_tokens.items()):
        scammers = group_scammers[gid]
        print("GID:", gid, "SCAMMERS:", len(scammers), "TOKENS:", len(tokens))
        if len(scammers) > 1:
            similarites = intra_cluster_similarity(gid, tokens, dex)
        else:
            similarites = intra_cluster_similarity(gid, tokens, dex, prefix="one_scammer_group_")
        if len(similarites) > 0:
            print(gid, ":", similarites)

def generate_inter_sim(group_tokens, dex='univ2'):
    print("GENERATE PAIRS SIM")
    for gid1, tokens1 in tqdm(group_tokens.items()):
        if len(tokens1) > 0:
            inter_cluster_similarity(gid1, tokens1, group_tokens, dex)

if __name__ == '__main__':
    group_tokens, group_scammers = load_data(dex='univ2')
    # print("GENERATE PAIRS SIM")
    # for gid, tokens in tqdm(group_tokens.items()):
    #     scammers = group_scammers[gid]
    #     print("GID:", gid, "SCAMMERS:", len(scammers), "TOKENS:", len(tokens))
    #     if len(scammers) > 1:
    #         similarites = intra_cluster_similarity(gid, tokens, dex='univ2')
    #     else:
    #         similarites = intra_cluster_similarity(gid, tokens, dex='univ2', prefix="one_scammer_group")
    #     if len(similarites) > 0:
    #         print(gid, ":", similarites)
    # print("CALCULATE INTRA SIM")
    # calculate_intra_avg_sim(group_tokens, group_scammers)
    # print("CALCULATE ONE SCAMMER GROUP INTRA SIM")
    # calculate_intra_avg_sim(group_tokens, group_scammers, prefix="one_scammer_group")
    generate_inter_sim(group_tokens, dex='univ2')
