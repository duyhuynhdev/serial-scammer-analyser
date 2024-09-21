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


class FunctionInputDecoder:
    router_functions = {
        # V2 ROUTER
        "0x38ed1739": {"signature": "swapExactTokensForTokens(uint256,uint256,address[],address,uint256)",
                       "types": ["uint256", "uint256", "address[]", "address", "uint256"],
                       "names": ['amountIn', 'amountOutMin', 'path', 'to', 'deadline']},
        "0x8803dbee": {"signature": "swapTokensForExactTokens(uint256,uint256,address[],address,uint256)",
                       "types": ["uint256", "uint256", "address[]", "address", "uint256"],
                       "names": ['amountOut', 'amountInMax', 'path', 'to', 'deadline']},
        "0x7ff36ab5": {"signature": "swapExactETHForTokens(uint256,address[],address,uint256)",
                       "types": ["uint256", "address[]", "address", "uint256"],
                       "names": ['amountOutMin', 'path', 'to', 'deadline']},
        "0x4a25d94a": {"signature": "swapTokensForExactETH(uint256,uint256,address[],address,uint256)",
                       "types": ["uint256", "uint256", "address[]", "address", "uint256"],
                       "names": ['amountOut', 'amountInMax', 'path', 'to', 'deadline']},
        "0x18cbafe5": {"signature": "swapTokensForExactETH(uint256,uint256,address[],address,uint256)",
                       "types": ["uint256", "uint256", "address[]", "address", "uint256"],
                       "names": ['amountIn', 'amountOutMin', 'path', 'to', 'deadline']},
        "0xfb3bdb41": {"signature": "swapETHForExactTokens(uint256,address[],address,uint256)",
                       "types": ["uint256", "address[]", "address", "uint256"],
                       "names": ['amountOut', 'path', 'to', 'deadline']},
        "0x5c11d795": {"signature": "swapExactTokensForTokensSupportingFeeOnTransferTokens(uint256,uint256,address[],address,uint256)",
                       "types": ["uint256", "uint256", "address[]", "address", "uint256"],
                       "names": ['amountIn', 'amountOutMin', 'path', 'to', 'deadline']},
        "0xb6f9de95": {"signature": "swapExactETHForTokensSupportingFeeOnTransferTokens(uint256,address[],address,uint256)",
                       "types": ["uint256", "address[]", "address", "uint256"],
                       "names": ['amountOutMin', 'path', 'to', 'deadline']},
        "0x791ac947": {"signature": "swapExactTokensForETHSupportingFeeOnTransferTokens(uint256,uint256,address[],address,uint256)",
                       "types": ["uint256", "uint256", "address[]", "address", "uint256"],
                       "names": ['amountIn', 'amountOutMin', 'path', 'to', 'deadline']},
        #V3 ROUTER
        "0x472b43f3": {"signature": "swapExactTokensForTokens(uint256,uint256,address[],address,uint256)",
                       "types": ["uint256", "uint256", "address[]", "address"],
                       "names": ['amountIn', 'amountOutMin', 'path', 'to']},
        "0x42712a67": {"signature": "swapTokensForExactTokens(uint256,uint256,address[],address)",
                       "types": ["uint256", "uint256", "address[]", "address"],
                       "names": ['amountOut', 'amountInMax', 'path', 'to']},
        "0xac9650d8": {"signature": "multicall(bytes[])",
                       "types": ["bytes[]"],
                       "names": ['data']},
        "0x5ae401dc": {"signature": "multicall(uint256 deadline, bytes[] data)",
                       "types": ["uint256", "bytes[]"],
                       "names": ['deadline','data']},
    }

    def decode_function_input(self, input):
        try:
            data = HexBytes(input)
            methodId, params = data[:4], data[4:]
            codec = ABICodec(build_strict_registry())
            if methodId.hex() not in self.router_functions.keys():
                # print("Cannot find method ", methodId.hex())
                return []
            function_info = self.router_functions[methodId.hex()]
            signature = function_info["signature"]
            types = function_info["types"]
            names = function_info["names"]
            decoded_data = codec.decode(types, HexBytes(params))
            normalized = map_abi_data(BASE_RETURN_NORMALIZERS, types, decoded_data)
            parsed =  dict(zip(names, normalized))
            if "multicall" in signature:
                parsed_results = []
                for b in parsed["data"]:
                    call = HexBytes(b).hex()
                    parsed_call = self.decode_function_input(call)
                    parsed_results.extend(parsed_call)
                return parsed_results
            return [parsed]
        except Exception as e:
            print(e)
            return []

class EventLogDecoder:
    pool_events = {
        "Sync": {"types": ["uint112", "uint112"], "names": ['reserve0', 'reserve1']},
        "Swap": {"types": ["address", "address", "uint256", "uint256", "uint256", "uint256"], "names": ['sender', 'to', 'amount0In', 'amount1In', 'amount0Out', 'amount1Out']},
        "Burn": {"types": ["address", "address", "uint256", "uint256"], "names": ['sender', 'to', 'amount0', 'amount1']},
        "Mint": {"types": ["address", "uint256", "uint256"], "names": ['sender', 'amount0', 'amount1']},
        "Transfer": {"types": ["address", "address", "uint256"], "names": ['sender', 'to', 'amount']},
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
    # result = {'address': '0x16ee82dc2e16395b148cb6e71c365a1331249ce9',
    #           'topics': ['0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f', '0x0000000000000000000000007a250d5630b4cf539739df2c5dacb4c659f2488d'],
    #           'data': '0x00000000000000000000000000000000000000000000000000000014f46b0400000000000000000000000000000000000000000000000000000002bb1c249e2e',
    #           'blockNumber': '0xa6d3fa',
    #           'blockHash': '0xb37f516908812fdb5b42762c3281149b01f4f5d9b3fbf7e2adf722fbfe11cd8a',
    #           'timeStamp': '0x5f6e2e89',
    #           'gasPrice': '0x199c82cc00',
    #           'gasUsed': '0x2936b1',
    #           'logIndex': '0xdf',
    #           'transactionHash': '0x9b75d8c0225dfc7e65586da9c485ef6dbebb4913be3ecbfe2a7c8300706f9042',
    #           'transactionIndex': '0x87'}
    # decoder = EventLogDecoder("Mint")
    # out = decoder.decode_event(result)
    # print(out)
    # input = "0x5ae401dc000000000000000000000000000000000000000000000000000000006201839100000000000000000000000000000000000000000000000000000000000000400000000000000000000000000000000000000000000000000000000000000001000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000000e4472b43f300000000000000000000000000000000000000000000000005698eef066700000000000000000000000000000000000000000164d3d278366064250aa498237e00000000000000000000000000000000000000000000000000000000000000800000000000000000000000008ebcf38555f1fe4e3b9e68f0001f2cb2f632efdc0000000000000000000000000000000000000000000000000000000000000002000000000000000000000000c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2000000000000000000000000736c5fa7fc85d9c3697203dc0f23ce953c8490f000000000000000000000000000000000000000000000000000000000"
    # decoder = FunctionInputDecoder()
    # print(decoder.decode_function_input(input))
    #
    snt = [
        "swapExactTokensForTokens(uint256,uint256,address[],address,uint256)",
        "swapTokensForExactTokens(uint256,uint256,address[],address,uint256)",
        "swapExactETHForTokens(uint256,address[],address,uint256)",
        "swapTokensForExactETH(uint256,uint256,address[],address,uint256)",
        "swapExactTokensForETH(uint256,uint256,address[],address,uint256)",
        "swapETHForExactTokens(uint256,address[],address,uint256)",
        "swapExactTokensForTokensSupportingFeeOnTransferTokens(uint256,uint256,address[],address,uint256)",
        "swapExactETHForTokensSupportingFeeOnTransferTokens(uint256,address[],address,uint256)",
        "swapExactTokensForETHSupportingFeeOnTransferTokens(uint256,uint256,address[],address,uint256)",
        "swapExactTokensForTokens(uint256,uint256,address[],address)",
        "swapTokensForExactTokens(uint256,uint256,address[],address)",
    ]
    for st in snt:
        strg = ut.keccak_hash(st)
        print(HexBytes(strg)[:4].hex())
