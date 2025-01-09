import glob
import json
import os

import pandas as pd
from sqlalchemy import create_engine, update
from sqlalchemy.orm import Session
from tqdm import tqdm
from web3 import Web3

from data_collection.EventCollector import ContractEventCollector
from entity.blockchain.Event import TransferEvent, SwapEvent, MintEvent, BurnEvent
from entity.blockchain.Transaction import NormalTransaction, InternalTransaction
from sql import PostgresDTO
import utils.DataLoader as DataLoader
from utils.Settings import Setting
from utils.ProjectPath import ProjectPath
from sqlalchemy import select

path = ProjectPath()
setting = Setting()


def create_postgres_engine(dex="univ2"):
    if dex == "univ2":
        db = "ethereum"
    else:
        db = "binance"
    connection_url = "postgresql://postgres:blockchain2024@localhost/" + db
    engine = create_engine(connection_url, echo=False, pool_recycle=3600)
    return engine


def get_existing_transactions(engine, sql):
    session = Session(engine)
    stmt = select(sql)
    existing = set()
    for tx in session.scalars(stmt):
        existing.add(tx.hash.lower())
    return existing


def import_normal_transactions(dex="univ2"):
    transaction_files = glob.glob(os.path.join(eval(f"path.{dex}_normal_tx_path"), "*.csv"))
    engine = create_postgres_engine(dex)
    for transaction_file in tqdm(transaction_files):
        try:
            normal_txs = pd.read_csv(transaction_file)
        except Exception as e:
            print(e)
            continue
        normal_txs.rename(columns={'from': 'sender'}, inplace=True)
        for tx in normal_txs.to_dict('records'):
            ptx = NormalTransaction()
            ptx.from_dict(tx)
            stx = PostgresDTO.NormalTransaction()
            stx = stx.to_sql_object(ptx)
            try:
                with Session(engine) as session:
                    session.add(stx)
                    session.commit()
            except Exception as e:
                print(e)

def update_normal_transactions(dex="univ2"):
    transaction_files = glob.glob(os.path.join(eval(f"path.{dex}_normal_tx_path"), "*.csv"))
    engine = create_postgres_engine(dex)
    for transaction_file in tqdm(transaction_files):
        try:
            normal_txs = pd.read_csv(transaction_file)
        except Exception as e:
            print(e)
            continue
        normal_txs.rename(columns={'from': 'sender'}, inplace=True)
        for tx in normal_txs.to_dict('records'):
            ptx = NormalTransaction()
            ptx.from_dict(tx)
            try:
                with Session(engine) as session:
                    stmt = select(PostgresDTO.NormalTransaction).where(PostgresDTO.NormalTransaction.hash == ptx.hash)
                    sqltx = session.scalars(stmt).one()
                    sqltx.gas_used = ptx.gasUsed
                    session.commit()
            except Exception as e:
                print(e)

def update_scammer_normal_transactions(dex="univ2"):
    engine = create_postgres_engine(dex)
    scammers_df = pd.read_csv(os.path.join(eval('path.{}_processed_path'.format(dex)), "filtered_simple_rp_scammers.csv"))
    scammers_df["scammer"] = scammers_df["scammer"].str.lower()
    scammers = list(scammers_df["scammer"].unique())
    for scammer in tqdm(scammers):
        transaction_file = os.path.join(eval(f"path.{dex}_normal_tx_path"), scammer.lower() + ".csv")
        if not os.path.exists(transaction_file):
            transaction_file = os.path.join(eval(f"path.{dex}_normal_tx_path"), Web3.to_checksum_address(scammer) + ".csv")
        try:
            normal_txs = pd.read_csv(transaction_file)
        except Exception as e:
            print(e)
            continue
        normal_txs.rename(columns={'from': 'sender'}, inplace=True)
        for tx in normal_txs.to_dict('records'):
            ptx = NormalTransaction()
            ptx.from_dict(tx)
            try:
                with Session(engine) as session:
                    stmt = select(PostgresDTO.NormalTransaction).where(PostgresDTO.NormalTransaction.hash == ptx.hash)
                    sqltx = session.scalars(stmt).one()
                    sqltx.gas_used = ptx.gasUsed
                    session.commit()
            except Exception as e:
                print(e)


def import_internal_transactions(dex="univ2"):
    transaction_files = glob.glob(os.path.join(eval(f"path.{dex}_internal_tx_path"), "*.csv"))
    engine = create_postgres_engine(dex)
    # existing_txs_hashes = get_existing_transactions(engine, sql=PostgresDTO.InternalTransaction)
    for transaction_file in tqdm(transaction_files):
        try:
            internal_txs = pd.read_csv(transaction_file)
        except Exception as e:
            print(e)
            continue
        internal_txs.rename(columns={'from': 'sender'}, inplace=True)
        for tx in internal_txs.to_dict('records'):
            ptx = InternalTransaction()
            ptx.from_dict(tx)
            # if ptx.hash.lower() in existing_txs_hashes:
            #     print("TX EXISTS")
            #     continue
            stx = PostgresDTO.InternalTransaction()
            stx = stx.to_sql_object(ptx)
            try:
                with Session(engine) as session:
                    session.add(stx)
                    session.commit()
            except Exception as e:
                print(e)


def import_pool_and_tokens(dex="univ2"):
    engine = create_postgres_engine(dex)
    pool_infos = DataLoader.load_pool_info(dex)
    token_info = DataLoader.load_token_info(dex)
    creator_info = DataLoader.load_creation_info(dex)
    (
        pool_scammers,
        scam_token_pool,
        scam_pools,
        scammers,
        scammer_pools,
    ) = DataLoader.load_rug_pull_dataset(dex=dex, scammer_file_name="filtered_simple_rp_scammers.csv", pool_file_name="filtered_simple_rp_pool.csv")
    print("LOAD POOL INFO")
    scam_pools = set(scam_pools)
    scam_tokens = set(scam_token_pool.keys())
    # for p in tqdm(pool_infos.keys()):
    #     try:
    #         info = pool_infos[p]
    #         creator = creator_info[p]
    #         spool = PostgresDTO.Pool(**{
    #             "address": p,
    #             "creator": creator,
    #             "token0": info["token0"],
    #             "token1": info["token1"],
    #             "is_malicious": p.lower() in scam_pools})
    #         with Session(engine) as session:
    #             session.add(spool)
    #             session.commit()
    #     except Exception as e:
    #         print(e)
    # print("LOAD TOKEN INFO")
    # print(creator_info)
    for t in tqdm(token_info.keys()):
        try:
            info = token_info[t]
            creator = creator_info[t]
            stoken = PostgresDTO.Token(**{
                "address": t,
                "creator": creator,
                "name": info["name"],
                "symbol": info["symbol"],
                "decimals": int(info["decimals"]),
                "supply": int(info["totalSupply"]),
                "is_malicious": t.lower() in scam_tokens})
            with Session(engine) as session:
                session.add(stoken)
                session.commit()
        except Exception as e:
            print(e)

def import_all_pool_events (dex="univ2"):
    engine = create_postgres_engine(dex)
    contract_event_collector = ContractEventCollector()
    transfer_files = glob.glob(os.path.join(eval(f"path.{dex}_pool_events_path"),"Transfer", "*.json"))
    for transfer_file in tqdm(transfer_files):
        try:
            transfer_list = contract_event_collector.parse_event("Transfer", transfer_file)
            transfers = [TransferEvent().from_dict(e) for e in transfer_list]
            for transfer in transfers:
                stransfer = PostgresDTO.PoolTransfer()
                stransfer = stransfer.to_sql_object(transfer)
                with Session(engine) as session:
                    session.add(stransfer)
                    session.commit()
                    session.close()
        except Exception as e:
            print(e)
    swap_files = glob.glob(os.path.join(eval(f"path.{dex}_pool_events_path"),"Swap", "*.json"))
    for swap_file in tqdm(swap_files):
        try:
            swaps_list = contract_event_collector.parse_event("Swap", swap_file)
            swaps = [SwapEvent().from_dict(e) for e in swaps_list]
            for swap in swaps:
                sswap = PostgresDTO.PoolSwap()
                sswap = sswap.to_sql_object(swap)
                with Session(engine) as session:
                    session.add(sswap)
                    session.commit()
                    session.close()
        except Exception as e:
            print(e)
    mint_files = glob.glob(os.path.join(eval(f"path.{dex}_pool_events_path"),"Mint", "*.json"))
    for mint_file in tqdm(mint_files):
        try:
            mint_list = contract_event_collector.parse_event("Mint", mint_file)
            mints = [MintEvent().from_dict(e) for e in mint_list]
            for mint in mints:
                smint = PostgresDTO.PoolMint()
                smint = smint.to_sql_object(mint)
                with Session(engine) as session:
                    session.add(smint)
                    session.commit()
                    session.close()
        except Exception as e:
            print(e)
    burn_files = glob.glob(os.path.join(eval(f"path.{dex}_pool_events_path"),"Burn", "*.json"))
    for burn_file in tqdm(burn_files):
        try:
            print(burn_file)
            burn_list= contract_event_collector.parse_event("Burn", burn_file)
            burns = [BurnEvent().from_dict(e) for e in burn_list]
            for burn in burns:
                sburn = PostgresDTO.PoolBurn()
                sburn = sburn.to_sql_object(burn)
                with Session(engine) as session:
                    session.add(sburn)
                    session.commit()
                    session.close()
        except Exception as e:
            continue
            # print(e)

def import_pool_events(dex="univ2"):
    engine = create_postgres_engine(dex)
    contract_event_collector = ContractEventCollector()
    pool_event_path = eval("path.{}_pool_events_path".format(dex))
    (
        pool_scammers,
        scam_token_pool,
        scam_pools,
        scammers,
        scammer_pools,
    ) = DataLoader.load_rug_pull_dataset(dex=dex, scammer_file_name="filtered_simple_rp_scammers.csv", pool_file_name="filtered_simple_rp_pool.csv")
    for pool_address in tqdm(set(scam_pools)):
        try:
            transfer_list = contract_event_collector.get_event(
                pool_address, "Transfer", pool_event_path, dex
            )
            transfers = [TransferEvent().from_dict(e) for e in transfer_list]
            for transfer in transfers:
                stransfer = PostgresDTO.PoolTransfer()
                stransfer = stransfer.to_sql_object(transfer)
                with Session(engine) as session:
                    session.add(stransfer)
                    session.commit()

            swap_list = contract_event_collector.get_event(
                pool_address, "Swap", pool_event_path, dex
            )
            swaps = [SwapEvent().from_dict(e) for e in swap_list]
            for swap in swaps:
                sswap = PostgresDTO.PoolSwap()
                sswap = sswap.to_sql_object(swap)
                with Session(engine) as session:
                    session.add(sswap)
                    session.commit()

            mint_list = contract_event_collector.get_event(
                pool_address, "Mint", pool_event_path, dex
            )
            mints = [MintEvent().from_dict(e) for e in mint_list]
            for mint in mints:
                smint = PostgresDTO.PoolMint()
                smint = smint.to_sql_object(mint)
                with Session(engine) as session:
                    session.add(smint)
                    session.commit()

            burn_list = contract_event_collector.get_event(
                pool_address, "Burn", pool_event_path, dex
            )
            burns = [BurnEvent().from_dict(e) for e in burn_list]
            for burn in burns:
                sburn = PostgresDTO.PoolBurn()
                sburn = sburn.to_sql_object(burn)
                with Session(engine) as session:
                    session.add(sburn)
                    session.commit()

        except Exception as e:
            print(e)


def import_token_events(dex="univ2"):
    engine = create_postgres_engine(dex)
    contract_event_collector = ContractEventCollector()
    token_event_path = eval("path.{}_token_events_path".format(dex))
    (
        pool_scammers,
        scam_token_pool,
        scam_pools,
        scammers,
        scammer_pools,
    ) = DataLoader.load_rug_pull_dataset(dex=dex, scammer_file_name="filtered_simple_rp_scammers.csv", pool_file_name="filtered_simple_rp_pool.csv")
    for token_address in tqdm(scam_token_pool.keys()):
        token_address = token_address.lower()
        try:
            transfer_list = contract_event_collector.get_event(
                token_address, "Transfer", token_event_path, dex
            )
            transfers = [TransferEvent().from_dict(e) for e in transfer_list]
            for transfer in transfers:
                stransfer = PostgresDTO.TokenTransfer()
                stransfer = stransfer.to_sql_object(transfer)
                with Session(engine) as session:
                    session.add(stransfer)
                    session.commit()
        except Exception as e:
            print(e)


if __name__ == '__main__':
    # update_normal_transactions(dex='panv2')
    # import_all_pool_events(dex='panv2')
    import_all_pool_events(dex='univ2')
    # import_normal_transactions()
    # engine = create_postgres_engine(dex)
    # txs = get_existing_transactions(engine, sql=PostgresDTO.NormalTransaction)
    # print("Result count:",len(txs))
    # import_pool_and_tokens(dex="panv2")
    # import_token_events(dex="panv2")
    # import_pool_events(dex="panv2")
    # import_normal_transactions(dex="panv2")
    # import_internal_transactions(dex="panv2")
