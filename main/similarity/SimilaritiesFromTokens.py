from main.similarity.Configs import *
from multiprocessing import Pool, TimeoutError
import pandas as pd
from tqdm import tqdm
import random
from collections import Counter


def create_similarity_dictionary(tokenized_contracts):
    similarity_dictionary = {}
    for address in tokenized_contracts.keys():
        similarity_dictionary[address] = {}
    return similarity_dictionary


def compare_similarities_between_sets(tokenized_contracts, tokenized_contracts2, min_required_similarity=default_minimum_similarity_score):
    similarities = create_similarity_dictionary(tokenized_contracts)

    print("Calculating similarities")
    contract_list = list(tokenized_contracts.keys())
    contract_list2 = list(tokenized_contracts2.keys())
    progress = tqdm(total=len(contract_list))
    while len(contract_list) > 0:
        address = contract_list.pop()
        for comparison_address in contract_list2:
            similarity_score = jaccard_similarity(tokenized_contracts[address], tokenized_contracts2[comparison_address])
            if similarity_score >= min_required_similarity:
                similarities[address][comparison_address] = similarity_score
                # similarities[comparison_address][address] = similarity_score
        progress.update(1)
    progress.close()
    return similarities


def compare_similarities(tokenized_contracts, min_required_similarity=default_minimum_similarity_score):
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


def jaccard_similarity(list1, list2):
    counter1 = Counter(list1)
    counter2 = Counter(list2)
    intersection = list((counter1 & counter2).elements())
    union = list((counter1 | counter2).elements())
    similarity = len(intersection) / len(union) if len(union) > 0 else 0
    return similarity


def grouping_by_similarity(similarities_file=default_similarities_file, min_similarity=default_comparison_similarity,
                           out_path=default_groups_file, include_creators=False):
    groups = dict()
    creator_dict = {}

    if include_creators:
        creator_dict = load_contract_creators()

    similarities = load_json_file(similarities_file)
    # # with Pool(processes=4) as pool:  # TODO: Multiprocessing
    # ###
    for src_add in tqdm(similarities.keys()):
        des_add = ""
        try:
            is_grouped = False
            for k, g in groups.items():
                des_add = random.choice(g)
                similarity = similarities[src_add][des_add] if des_add in similarities[src_add] else 0
                if similarity >= min_similarity:
                    g.append(src_add)
                    is_grouped = True
                    break
            if not is_grouped:
                g = [src_add]
                groups[len(groups)] = g
        except Exception as e:
            print(src_add, des_add, e)
    ###

    data = []
    for k, g in groups.items():
        for add in g:
            data_to_add = {"address": add, "group": k, "creator": ""}
            if add in creator_dict:
                data_to_add["creator"] = creator_dict[add]
            data.append(data_to_add)
    df = pd.DataFrame.from_records(data)
    df.to_csv(out_path, index=False)
    return groups


def get_stats_for_similarities(similarity_file_location=default_similarities_file, num_steps=100,
                               output_file_name="similarities_stats", do_csv_also=True):
    stats_dict = {
        "total": 0
    }
    for i in range(num_steps + 1):
        stats_dict[str(i)] = 0

    similarities = load_json_file(similarity_file_location)

    for address in tqdm(similarities.keys()):
        for sub_address in similarities[address].keys():
            curr_similarity = int(similarities[address][sub_address] * num_steps)
            stats_dict[str(curr_similarity)] += 1
            stats_dict["total"] += 1

    if output_file_name is not None:
        save_json_file(output_file_name + ".json", stats_dict)
        if do_csv_also:
            with open(output_file_name + ".csv", "w") as f:
                for i in range(num_steps + 1):
                    f.write(str(i) + "," + str(stats_dict[str(i)]) + "\n")


def load_contract_creators(creators_file=default_creators_file):
    creators = {}
    with open(creators_file, "r") as f:
        for line in f:
            csv = line.split(sep=",")
            creators[str(csv[0])] = str(csv[1])
    return creators


if __name__ == '__main__':
    # pass
    # TODO: Testing function start
    # get_stats_for_similarities(num_steps=100)
    grouping_by_similarity(min_similarity=1.00, include_creators=True)
    # grouping_by_similarity(min_similarity=0.94)
    # TODO: Testing function end
