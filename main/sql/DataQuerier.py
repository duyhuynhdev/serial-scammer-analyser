from sqlalchemy import select, create_engine, or_, and_
from sqlalchemy.orm import Session
from sql.PostgresDTO import *


class DataQuerier:
    def __init__(self, dex):
        self.engine = self.create_postgres_engine(dex)

    def create_postgres_engine(self, dex="univ2"):
        if dex == "univ2":
            db = "ethereum"
        else:
            db = "binance"
        connection_url = "postgresql://postgres:blockchain2024@localhost/" + db
        return create_engine(connection_url, echo=False, pool_recycle=3600, pool_size=20, max_overflow=0)

    def get_normal_transactions(self, address):
        session = Session(self.engine)
        stmt = select(NormalTransaction).where(
            or_(NormalTransaction.sender == Web3.to_checksum_address(address),
                NormalTransaction.receiver == Web3.to_checksum_address(address),
                NormalTransaction.sender == address.lower(),
                NormalTransaction.receiver == address.lower()))
        result = session.scalars(stmt).all()
        session.close()
        return result

    def get_internal_transactions(self, address):
        session = Session(self.engine)
        stmt = select(InternalTransaction).where(
            or_(InternalTransaction.sender == Web3.to_checksum_address(address),
                InternalTransaction.receiver == Web3.to_checksum_address(address),
                InternalTransaction.sender == address.lower(),
                InternalTransaction.receiver == address.lower()))
        result = session.scalars(stmt).all()
        session.close()
        return result

    def get_event(self, event_class, address):
        session = Session(self.engine)
        stmt = select(event_class).where(or_(event_class.address == Web3.to_checksum_address(address), event_class.address == address.lower()))
        result = session.scalars(stmt).all()
        session.close()
        return result

    def get_pool_transfer(self, address):
        return self.get_event(PoolTransfer, address)

    def get_pool_mint(self, address):
        return self.get_event(PoolMint, address)

    def get_pool_burn(self, address):
        return self.get_event(PoolBurn, address)

    def get_pool_swap(self, address):
        return self.get_event(PoolSwap, address)

    def get_token_transfer(self, address):
        return self.get_event(TokenTransfer, address)

    def get_pool_investing_swap(self, pool: Pool):
        session = Session(self.engine)
        stmt = select(PoolSwap).where(and_(
            or_(PoolSwap.address == Web3.to_checksum_address(pool.address), PoolSwap.address == pool.address.lower()),
            eval(f"PoolSwap.amount{pool.get_high_value_position()}_in") > 0))
        result = session.scalars(stmt).all()
        session.close()
        return result

    def get_pool_devesting_swap(self, pool: Pool):
        session = Session(self.engine)
        stmt = select(PoolSwap).where(and_(
            or_(PoolSwap.address == Web3.to_checksum_address(pool.address), PoolSwap.address == pool.address.lower()),
            eval(f"PoolSwap.amount{pool.get_high_value_position()}_out") > 0))
        result = session.scalars(stmt).all()
        session.close()
        return result

    def get_scam_pool(self, creator):
        session = Session(self.engine)
        stmt = select(Pool).where(and_(or_(Pool.creator == Web3.to_checksum_address(creator), Pool.creator == creator.lower()), Pool.is_malicious == True))
        result = session.scalars(stmt).all()
        session.close()
        return result

    def get_pool_by_creator(self, creator):
        session = Session(self.engine)
        stmt = select(Pool).where(or_(Pool.creator == Web3.to_checksum_address(creator), Pool.creator == creator.lower()))
        result = session.scalars(stmt).all()
        session.close()
        return result

    def get_pool_by_address(self, address):
        session = Session(self.engine)
        stmt = select(Pool).where(or_(Pool.address == Web3.to_checksum_address(address), Pool.address == address.lower()))
        result = session.scalars(stmt).all()
        session.close()
        return result

    def ensure_valid_eoa_address(self, address):
        print("Ensuring valid_eoa_address={}".format(address))
        normal_txs = self.get_normal_transactions(address)
        for ntx in normal_txs:
            if ntx.sender.lower() == address.lower():  # sender of normal txs must be EOA
                return True
            if ntx.is_contract_creation and ntx.receiver.lower() == address.lower():  # address is a contract created by an EOA
                return False
            if ntx.is_contract_call_tx() and ntx.receiver.lower() == address.lower():  # address is a contract called by other address
                return False
        internal_txs = self.get_internal_transactions(address)
        for itx in internal_txs:
            if itx.sender.lower() == address.lower():  # sender of internal txs must be contract
                return False
            if itx.is_contract_creation and itx.receiver == address.lower():  # address is a contract created by another contract
                return False
        if len(internal_txs) == 0 and len(normal_txs) == 0:
            print("No transactions found")
            return False
        return True

    def get_contract_creation_tx(self, creator, contract):
        session = Session(self.engine)
        stmt = select(NormalTransaction).where(and_(
            or_(NormalTransaction.sender == Web3.to_checksum_address(creator), NormalTransaction.sender == creator.lower()),
            or_(NormalTransaction.receiver == Web3.to_checksum_address(contract), NormalTransaction.receiver == contract.lower()),
            NormalTransaction.is_contract_creation == True))
        result = session.scalars(stmt).first()
        session.close()
        return result


if __name__ == '__main__':
    dex = "univ2"
    # engine = create_postgres_engine(dex)
    # addresses = "0x2f24e0809a0da8e312b33af3d3b4dc46c986cd8f"
    # # results = get_internal_transactions(engine, addresses)
    # # for result in results:
    # #     print(result.__dict__)
    # print(ensure_valid_eoa_address(engine, addresses))
