import sys
import os
from collections import Counter

import numpy as np
import pandas as pd
from tqdm import tqdm
import random
from utils import Utils as ut
from analysis import ChartDrawer
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
    group_hashed_tokens = dict()
    group_scammers = dict()
    scammer_hashed_tokens = dict()
    file_path = os.path.join(eval('path.{}_processed_path'.format(dex)), "non_swap_simple_rp_scammer_group.csv")
    if os.path.exists(file_path):
        groups = pd.read_csv(file_path)
        group_scammers = groups.groupby("group_id")["scammer"].apply(list).to_dict()
        for group_id, scammers in group_scammers.items():
            tokens = set()
            for scammer in scammers:
                tokens.update(scammer_tokens[scammer])
                past = get_available_hash_data(scammer_tokens[scammer], dex)
                if len(past) >= 1:
                    scammer_hashed_tokens[scammer] = past
            available_ast = get_available_hash_data(tokens, dex)
            if len(available_ast) >= 1:
                group_hashed_tokens[group_id] = available_ast
    return group_hashed_tokens, group_scammers, scammer_hashed_tokens


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


def intra_cluster_similarity(group_id, hashed_tokens, dex='univ2', prefix="", limit=10000):
    similarity_path = eval(f"path.{dex}_intra_similarity_path")
    similarity_file = os.path.join(similarity_path, f"{prefix}intra_{group_id}_similarity.json")
    if len(hashed_tokens) <= 1:
        return {}
    if len(hashed_tokens) > limit:
        hashed_tokens = pick_random_groups(hashed_tokens, limit)
        print("\t PRUNNING AST SIZE:", len(hashed_tokens))
    similarites = compare_similarities(hashed_tokens, min_required_similarity=0)
    ut.write_json(similarity_file, similarites)
    print("\t SAVED SIM TO ", similarity_file)
    return similarites

def individual_scammer_similarity(address, hashed_tokens, dex='univ2', limit=10000):
    similarity_path = eval(f"path.{dex}_individual_similarity_path")
    similarity_file = os.path.join(similarity_path, f"{address}_similarity.json")
    if len(hashed_tokens) <= 1:
        return {}
    if len(hashed_tokens) > limit:
        hashed_tokens = pick_random_groups(hashed_tokens, limit)
        print("\t PRUNNING AST SIZE:", len(hashed_tokens))
    similarites = compare_similarities(hashed_tokens, min_required_similarity=0)
    ut.write_json(similarity_file, similarites)
    print("\t SAVED SIM TO ", similarity_file)
    return similarites

def pick_random_groups(groups, size):
    if len(groups) < size:
        return groups
    randoms = dict()
    keys = list(groups.keys())
    random.shuffle(keys)
    random_keys = keys[:size]
    for k in random_keys:
        value = groups[k]
        randoms[k] = value
    return randoms


def inter_cluster_similarity(group_1_id, avaiHash1, group_tokens, dex='univ2', limit=100):
    print("*" * 100)
    similarity_path = eval(f"path.{dex}_inter_similarity_path")
    similarity_file = os.path.join(similarity_path, f"inter_{group_1_id}_similarity.json")
    similarites = dict()
    for r in range (0,10):
        random_groups = pick_random_groups(group_tokens, 500)
        for group_2_id, avaiHash2 in random_groups.items():
            print("-" * 50)
            print(f"\t GROUP {group_2_id} AVAILABLE AST SIZE:", len(avaiHash2))
            if len(avaiHash2) == 0:
                print(f"G{group_2_id} EMPTY >>> SKIP")
                print("EMPTY AST >>> SKIP")
                continue
            if len(avaiHash1) > limit:
                avaiHash1 = pruning_data(avaiHash1, limit)
                print(f"\t GROUP {group_1_id} PRUNNING AST SIZE:", len(avaiHash1))
            if len(avaiHash2) > limit:
                avaiHash2 = pruning_data(avaiHash2, limit)
                print(f"\t GROUP {group_2_id} PRUNNING AST SIZE:", len(avaiHash2))
            similarites[group_2_id] = compare_similarities_between_sets(avaiHash1, avaiHash2, min_required_similarity=0)
    ut.write_json(similarity_file, similarites)
    print("\t SAVED SIM TO ", similarity_file)
    print("*" * 100)
    return similarites


def calculate_intra_avg_sim(group_tokens, group_scammers, dex='univ2', prefix=""):
    global_sim = []
    sim_data = []
    for gid, tokens in tqdm(group_tokens.items()):
        scammers = group_scammers[gid]
        similarity_path = eval(f"path.{dex}_intra_similarity_path")
        similarity_file = os.path.join(similarity_path, f"{prefix}intra_{gid}_similarity.json")
        values = []
        if os.path.exists(similarity_file):
            sims = ut.read_json(similarity_file)
            for k, pairs in sims.items():
                for v in pairs.values():
                    values.append(v)
            avg = np.mean(values)
            global_sim.append(avg)
            record = {"group_id:": gid, "scammers": len(scammers), "available_tokens": len(sims), "intra_similarity": avg}
            sim_data.append(record)
            print(record)
    df = pd.DataFrame(sim_data)
    df.to_csv(f"{dex}_{prefix}intra_similarities.csv", index=False)
    print("TOTAL", len(global_sim), "WITH AVG SIM", np.mean(global_sim))


def calculate_inter_avg_sim(group_tokens, group_scammers, dex='univ2'):
    global_sim = []
    sim_data = []
    for gid, tokens in tqdm(group_tokens.items()):
        scammers = group_scammers[gid]
        similarity_path = eval(f"path.{dex}_inter_similarity_path")
        similarity_file = os.path.join(similarity_path, f"inter_{gid}_similarity.json")
        values = []
        if os.path.exists(similarity_file):
            sims = ut.read_json(similarity_file)
            token_count = 0
            for g in sims.values():
                token_count += len(g)
                for k, pairs in g.items():
                    for v in pairs.values():
                        values.append(v)
            avg = np.mean(values)
            global_sim.append(avg)
            record = {"group_id:": gid, "scammers": len(scammers), "available_tokens": token_count, "groups": len(sims), "intra_similarity": avg}
            sim_data.append(record)
            print(record)
    df = pd.DataFrame(sim_data)
    df.to_csv(f"{dex}_inter_similarities.csv", index=False)
    print("TOTAL", len(global_sim), "WITH AVG SIM", np.mean(global_sim))


def generate_intra_sim(group_hashed_tokens, group_scammers, dex='univ2'):
    print("GENERATE PAIRS SIM")
    for gid, hashed_tokens in tqdm(group_hashed_tokens.items()):
        scammers = group_scammers[gid]
        print("GID:", gid, "SCAMMERS:", len(scammers), "HASHED TOKENS:", len(hashed_tokens))
        if len(scammers) > 1:
            similarites = intra_cluster_similarity(gid, hashed_tokens, dex)
        else:
            similarites = intra_cluster_similarity(gid, hashed_tokens, dex, prefix="one_scammer_group_")
        if len(similarites) > 0:
            print(gid, ":", similarites)


def generate_inter_sim(group_tokens, dex='univ2'):
    print("GENERATE PAIRS SIM")
    for gid1, avaiHash1 in tqdm(group_tokens.items()):
        # print("GID:", gid1, "HASHED TOKENS:", len(avaiHash1))
        if len(avaiHash1) > 0:
            inter_cluster_similarity(gid1, avaiHash1, group_tokens, dex)

def generate_individual_sim(scammer_hashed_tokens, dex='univ2'):
    print("GENERATE PAIRS SIM")
    for scammer, hashed_tokens in tqdm(scammer_hashed_tokens.items()):
        print("ADDRESS:", scammer, "HASHED TOKENS:", len(hashed_tokens))
        if len(hashed_tokens) > 1:
            similarites = individual_scammer_similarity(scammer, hashed_tokens, dex)
            if len(similarites) > 0:
                print(scammer, ":", similarites)

def calculate_individual_avg_sim(scammer_hashed_tokens, dex='univ2'):
    global_sim = []
    sim_data = []
    for scammer, hashed_tokens in tqdm(scammer_hashed_tokens.items()):
        similarity_path = eval(f"path.{dex}_individual_similarity_path")
        similarity_file = os.path.join(similarity_path, f"{scammer}_similarity.json")
        values = []
        if os.path.exists(similarity_file):
            sims = ut.read_json(similarity_file)
            for k, pairs in sims.items():
                for v in pairs.values():
                    values.append(v)
            avg = np.mean(values)
            global_sim.append(avg)
            record = {"scammer:": scammer, "available_tokens": len(hashed_tokens), "intra_similarity": avg}
            sim_data.append(record)
            print(record)
    df = pd.DataFrame(sim_data)
    df.to_csv(f"{dex}_individual_similarities.csv", index=False)
    print("TOTAL", len(global_sim), "WITH AVG SIM", np.mean(global_sim))

if __name__ == '__main__':
    dex='univ2'
    group_hashed_tokens, group_scammers, scammer_hashed_tokens = load_data(dex=dex)
    print("DATA SIZE", len(group_hashed_tokens))
    print("SCAMMER SIZE", len(group_scammers))
    print("SCAMMER SIZE", len(scammer_hashed_tokens))
    print("TOTAL HASHED TOKENS", sum([len(s) for s in scammer_hashed_tokens.values()]))
    # generate_intra_sim(group_hashed_tokens, group_scammers, dex=dex)
    # calculate_intra_avg_sim(group_hashed_tokens, group_scammers,dex=dex)
    # calculate_intra_avg_sim(group_hashed_tokens, group_scammers, prefix="one_scammer_group_",dex=dex)
    # generate_inter_sim(group_hashed_tokens, dex=dex)
    # calculate_inter_avg_sim(group_hashed_tokens, group_scammers,dex=dex)
    # generate_individual_sim(scammer_hashed_tokens, dex=dex)
    # calculate_individual_avg_sim(scammer_hashed_tokens, dex=dex)
    # g_count = []
    # for gi, hash_token in group_hashed_tokens.items():
    #     g_count.append(len(hash_token))
    # print(g_count.count(1))
