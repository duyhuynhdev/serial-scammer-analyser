from utils.Settings import Setting
from utils.Path import Path
from hexbytes import HexBytes
from web3._utils.normalizers import BASE_RETURN_NORMALIZERS
from eth_abi.codec import ABICodec
from web3._utils.abi import (
    build_strict_registry,
    map_abi_data,
)
import utils.Utils as ut

path = Path()
setting = Setting()

class EventLogDecoder:
    pool_events = {
        "Sync": {"types": ["uint112", "uint112"], "names": ['reserve0', 'reserve1']},
        "Swap": {"types": ["address", "address", "uint256", "uint256", "uint256", "uint256"], "names": ['sender', 'to', 'amount0In', 'amount1In', 'amount0Out', 'amount1Out']},
        "Burn": {"types": ["address", "address", "uint256", "uint256"], "names": ['sender', 'to', 'amount0', 'amount1']},
        "Mint": {"types": ["address", "uint256", "uint256"], "names": ['sender', 'amount0', 'amount1']},
        "Transfer": {"types": ["address", "address", "uint256"], "names": ['from', 'to', 'value']},
    }

    def __init__(self, event):
        self.event = event
        self.types = self.pool_events[event]['types']
        self.names = self.pool_events[event]['names']

    def decode_event(self, result):
        data = [t[2:] for t in result['topics']]
        data += [result['data'][2:]]
        data = "0x" + "".join(data)
        data = HexBytes(data)
        signature, params = data[:32], data[32:]
        codec = ABICodec(build_strict_registry())
        decoded_data = codec.decode(self.types, HexBytes(params))
        normalized = map_abi_data(BASE_RETURN_NORMALIZERS, self.types, decoded_data)
        parsed_log = {
            "address": result['address'],
            "event": self.event,
            "blockNumber": ut.hex_to_dec(result['blockNumber']),
            "blockHash": result['blockHash'],
            "timeStamp": ut.hex_to_dec(result['timeStamp']),
            "gasPrice": ut.hex_to_dec(result['gasPrice']) if result['gasPrice'] != "0x" else None,
            "gasUsed": ut.hex_to_dec(result['gasUsed']) if result['gasUsed'] != "0x" else None,
            "transactionHash": result['transactionHash'],
        }
        parsed_log.update(dict(zip(self.names, normalized)))
        return parsed_log


if __name__ == '__main__':
    result = {'address': '0x16ee82dc2e16395b148cb6e71c365a1331249ce9',
              'topics': ['0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f', '0x0000000000000000000000007a250d5630b4cf539739df2c5dacb4c659f2488d'],
              'data': '0x00000000000000000000000000000000000000000000000000000014f46b0400000000000000000000000000000000000000000000000000000002bb1c249e2e',
              'blockNumber': '0xa6d3fa',
              'blockHash': '0xb37f516908812fdb5b42762c3281149b01f4f5d9b3fbf7e2adf722fbfe11cd8a',
              'timeStamp': '0x5f6e2e89',
              'gasPrice': '0x199c82cc00',
              'gasUsed': '0x2936b1',
              'logIndex': '0xdf',
              'transactionHash': '0x9b75d8c0225dfc7e65586da9c485ef6dbebb4913be3ecbfe2a7c8300706f9042',
              'transactionIndex': '0x87'}
    decoder = EventLogDecoder("Mint")
    out = decoder.decode_event(result)
    print(out)
