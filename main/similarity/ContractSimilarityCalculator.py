import sys
import os

sys.path.append(os.path.join(os.path.dirname(sys.path[0])))
from ContractTokenization import *
from utils.Settings import Setting
from utils.ProjectPath import ProjectPath

path = ProjectPath()
setting = Setting()


def select_comparison(address_list_1=None,
                      address_list_2=None,
                      address_file_1=None,
                      address_file_2=None,
                      output_file=None,
                      min_required_similarity=default_minimum_similarity_score):
    similarities = None

    if address_file_1 is not None:
        address_list_1 = read_list_from_file(address_file_1)
    if address_file_2 is not None:
        address_list_2 = read_list_from_file(address_file_2)

    if address_list_1 is not None:
        if address_list_2 is not None:
            similarities = compare_address_similarities_between_sets(address_list_1,
                                                                     address_list_2,
                                                                     min_required_similarity=min_required_similarity)
        else:
            similarities = compare_address_similarities(address_list_1,
                                                        min_required_similarity=min_required_similarity)
        if output_file is not None:
            save_json_file(output_file, similarities)

    return similarities


if __name__ == '__main__':
    sim = select_comparison(["0xFfFf4c7A7282E67C191dC57eF8b6302B7AAD8dF2",
                             "0xffFf981B7F8730fAABd44906aA02FAC15C22F2Ce",
                             "0xFffFF4fF386013f4B9DAC8874f856Dec0E802467",
                             "0xffffffff2ba8f66d4e51811c5190992176930278",
                             "0xfffffffFf15AbF397dA76f1dcc1A1604F45126DB"], min_required_similarity=0)
    print(sim)
