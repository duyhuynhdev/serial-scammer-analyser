from utils.Settings import Setting
from utils.ProjectPath import ProjectPath
from hexbytes import HexBytes
from web3._utils.normalizers import BASE_RETURN_NORMALIZERS
from eth_abi.codec import ABICodec
from web3._utils.abi import (
    build_strict_registry,
    map_abi_data,
)
import utils.Utils as ut

path = ProjectPath()
setting = Setting()


class FunctionInputDecoder:
    codec = ABICodec(build_strict_registry())
    router_remove_liq_functions = {
        "0xbaa2abde": {"signature": "removeLiquidity(address,address,uint256,uint256,uint256,address,uint256)",
                       "types": ["address", "address", "uint256", "uint256", "uint256", "address", "uint256"],
                       "names": ["tokenA", "tokenB", "liquidity", "amountAMin", "amountBMin", "to", "deadline"]},
        "0x02751cec": {"signature": "removeLiquidityETH(address,uint256,uint256,uint256,address,uint256)",
                       "types": ["address", "uint256", "uint256", "uint256", "address", "uint256"],
                       "names": ["token", "liquidity", "amountTokenMin", "amountETHMin", "to", "deadline"]},
        "0xaf2979eb": {"signature": "removeLiquidityETHSupportingFeeOnTransferTokens(address,uint256,uint256,uint256,address,uint256)",
                       "types": ["address", "uint256", "uint256", "uint256", "address", "uint256"],
                       "names": ["token", "liquidity", "amountTokenMin", "amountETHMin", "to", "deadline"]},
        "0xded9382a": {"signature": "removeLiquidityETHWithPermit(address,uint256,uint256,uint256,address,uint256,bool,uint8,bytes32,bytes32)",
                       "types": ["address", "uint256", "uint256", "uint256", "address", "uint256", "bool", "uint8", "bytes32", "bytes32"],
                       "names": ['token', 'liquidity', 'amountTokenMin', 'amountETHMin', 'to', 'deadline', 'approveMax', 'v', 'r', 's']},
        "0x5b0d5984": {"signature": "removeLiquidityETHWithPermitSupportingFeeOnTransferTokens(address,uint256,uint256,uint256,address,uint256,bool,uint8,bytes32,bytes32)",
                       "types": ["address", "uint256", "uint256", "uint256", "address", "uint256", "bool", "uint8", "bytes32", "bytes32"],
                       "names": ['token', 'liquidity', 'amountTokenMin', 'amountETHMin', 'to', 'deadline', 'approveMax', 'v', 'r', 's']},
        "0x2195995c": {"signature": "removeLiquidityWithPermit(address,address,uint256,uint256,uint256,address,uint256,bool,uint8,bytes32,bytes32)",
                       "types": ["address", "address", "uint256", "uint256", "uint256", "address", "uint256", "bool", "uint8", "bytes32", "bytes32"],
                       "names": ['tokenA', 'tokenB', 'liquidity', 'amountAMin', 'amountBMin', 'to', 'deadline', 'approveMax', 'v', 'r', 's']},
    }
    router_add_liq_functions = {
        "0xbaa2abde": {"signature": "addLiquidity(address,address,uint256,uint256,uint256,uint256,address,uint256)",
                       "types": ["address", "address", "uint256", "uint256", "uint256", "uint256", "address", "uint256"],
                       "names": ["tokenA", "tokenB", "amountADesired", "amountBDesired", "amountAMin", "amountBMin", "to", "deadline"]},
        "0xf305d719": {"signature": "addLiquidityETH(address,uint256,uint256,uint256,address,uint256)",
                       "types": ["address", "uint256", "uint256", "uint256", "address", "uint256"],
                       "names": ["token", "amountTokenDesired", "amountTokenMin", "amountETHMin", "to", "deadline"]}
    }
    router_swap_functions = {
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
        ## CUSTOM FUNCTIONS
        "0x0162e2d0": {"signature": "swapETHForExactTokens(uint256[],address[],address,uint256,uint256,uint256,uint256))",
                       "types": ["uint256", "address[]", "address", "uint256", "uint256", "uint256", "uint256"],
                       "names": ['amountOut', 'path', 'to', 'deadline', "unknown0", "unknown1", "unknown2"]},
        "0x088890dc": {"signature": "swapExactETHForTokensSupportingFeeOnTransferTokens(uint256,address[],address,uint256,address)",
                       "types": ["uint256", "address[]", "address", "uint256", "address"],
                       "names": ['amountOutMin', 'path', 'to', 'deadline', 'referrer']},
        # V3 ROUTER
        "0x472b43f3": {"signature": "swapExactTokensForTokens(uint256,uint256,address[],address,uint256)",
                       "types": ["uint256", "uint256", "address[]", "address"],
                       "names": ['amountIn', 'amountOutMin', 'path', 'to']},
        "0x42712a67": {"signature": "swapTokensForExactTokens(uint256,uint256,address[],address)",
                       "types": ["uint256", "uint256", "address[]", "address"],
                       "names": ['amountOut', 'amountInMax', 'path', 'to']},
        "0x0d5f0e3b": {"signature": "uniswapV3SwapTo(uint256,uint256,uint256,uint256[])",
                       "types": ["uint256", "uint256", "address[]", "address"],
                       "names": ['amountOut', 'amountInMax', 'path', 'to']},
        "0xac9650d8": {"signature": "multicall(bytes[])",
                       "types": ["bytes[]"],
                       "names": ['data']},
        "0x5ae401dc": {"signature": "multicall(uint256,bytes[])",
                       "types": ["uint256", "bytes[]"],
                       "names": ['deadline', 'data']},
        # UniversalRouter.execute functions
        # commands[i] is the command that will use inputs[i] as its encoded input parameters.
        "0x3593564c": {"signature": "execute(bytes,bytes[],uint256)",
                       "types": ["bytes", "bytes[]", "uint256"],
                       "names": ['commands', 'inputs', 'deadline']},
    }

    # Transactions to the UniversalRouter all go through the UniversalRouter.execute functions (https://docs.uniswap.org/contracts/universal-router/technical-reference#permit2_permit_batch)
    v3_router_commands = {
        "0x00": {"command": "V3_SWAP_EXACT_IN",
                 "types": ["address", "uint256", "uint256", "bytes", "bool"],
                 "names": ["recipient", "amountIn", "amountOutMin", "path", "fromSender"]},
        "0x01": {"command": "V3_SWAP_EXACT_OUT",
                 "types": ["address", "uint256", "uint256", "bytes", "bool"],
                 "names": ["recipient", "amountOut", "amountInMax", "path", "fromSender"]},
        "0x02": {"command": "PERMIT2_TRANSFER_FROM",
                 "types": [],
                 "names": []},
        "0x03": {"command": "PERMIT2_PERMIT_BATCH",
                 "types": [],
                 "names": []},
        "0x04": {"command": "SWEEP",
                 "types": [],
                 "names": []},
        "0x05": {"command": "TRANSFER",
                 "types": [],
                 "names": []},
        "0x06": {"command": "PAY_PORTION",
                 "types": [],
                 "names": []},
        "0x08": {"command": "V2_SWAP_EXACT_IN",
                 "types": ["address", "uint256", "uint256", "address[]", "bool"],
                 "names": ["recipient", "amountIn", "amountOutMin", "path", "fromSender"]},
        "0x09": {"command": "V2_SWAP_EXACT_OUT",
                 "types": ["address", "uint256", "uint256", "address[]", "bool"],
                 "names": ["recipient", "amountOut", "amountInMax", "path", "fromSender"]},
        "0x0a": {"command": "PERMIT2_PERMIT",
                 "types": [],
                 "names": []},
        "0x0b": {"command": "WRAP_ETH",
                 "types": [],
                 "names": []},
        "0x0c": {"command": "UNWRAP_WETH",
                 "types": [],
                 "names": []},
        "0x0d": {"command": "PERMIT2_TRANSFER_FROM_BATCH",
                 "types": [],
                 "names": []},
        "0x10": {"command": "SEAPORT",
                 "types": [],
                 "names": []},
        "0x11": {"command": "LOOKS_RARE_721",
                 "types": [],
                 "names": []},
        "0x12": {"command": "NFTX",
                 "types": [],
                 "names": []},
        "0x13": {"command": "CRYPTOPUNKS",
                 "types": [],
                 "names": []},
        "0x14": {"command": "LOOKS_RARE_1155",
                 "types": [],
                 "names": []},
        "0x15": {"command": "OWNER_CHECK_721",
                 "types": [],
                 "names": []},
        "0x16": {"command": "OWNER_CHECK_1155",
                 "types": [],
                 "names": []},
        "0x17": {"command": "SWEEP_ERC721",
                 "types": [],
                 "names": []},
        "0x18": {"command": "X2Y2_721",
                 "types": [],
                 "names": []},
        "0x19": {"command": "SUDOSWAP",
                 "types": [],
                 "names": []},
        "0x1a": {"command": "NFT20",
                 "types": [],
                 "names": []},
        "0x1b": {"command": "X2Y2_1155",
                 "types": [],
                 "names": []},
        "0x1c": {"command": "FOUNDATION",
                 "types": [],
                 "names": []},
        "0x1d": {"command": "SWEEP_ERC1155",
                 "types": [],
                 "names": []},
    }

    def decode_swap_command_input(self, commands, inputs):
        command_bytes = HexBytes(commands)
        parsed_results = []
        idx = 0
        is_swap = False
        for dec in command_bytes:
            command_dex = '0x{0:0{1}x}'.format(dec, 2)
            if command_dex in ["0x00", "0x08"]: # swap in
                is_swap = True
            if command_dex in ["0x08", "0x09"]:
                command_info = self.v3_router_commands[command_dex]
                types = command_info["types"]
                names = command_info["names"]
                decoded_data = self.codec.decode(types, HexBytes(inputs[idx]))
                normalized = map_abi_data(BASE_RETURN_NORMALIZERS, types, decoded_data)
                parsed = dict(zip(names, normalized))
                parsed_results.append(parsed)
            idx += 1
        return is_swap, parsed_results

    def decode_function_input(self, input, signature_dict):
        data = HexBytes(input)
        methodId, params = data[:4], data[4:]
        if methodId.hex() not in signature_dict.keys():
            return None
        function_info = signature_dict[methodId.hex()]
        types = function_info["types"]
        names = function_info["names"]
        decoded_data = self.codec.decode(types, HexBytes(params))
        normalized = map_abi_data(BASE_RETURN_NORMALIZERS, types, decoded_data)
        parsed = dict(zip(names, normalized))
        return parsed

    def decode_remove_liq_function_input(self, input):
        try:
            return self.decode_function_input(input, self.router_remove_liq_functions)
        except Exception as e:
            print(e)
            return None

    def decode_add_liq_function_input(self, input):
        try:
            return self.decode_function_input(input, self.router_add_liq_functions)
        except Exception as e:
            print(e)
            return None

    def decode_swap_function_input(self, input):
        try:
            data = HexBytes(input)
            methodId, params = data[:4], data[4:]
            if methodId.hex() not in self.router_swap_functions.keys():
                # print("Cannot find method ", methodId.hex())
                return False, []
            function_info = self.router_swap_functions[methodId.hex()]
            signature = function_info["signature"]
            types = function_info["types"]
            names = function_info["names"]
            decoded_data = self.codec.decode(types, HexBytes(params))
            normalized = map_abi_data(BASE_RETURN_NORMALIZERS, types, decoded_data)
            parsed = dict(zip(names, normalized))
            if "multicall" in signature:
                parsed_results = []
                is_swap_call = False
                for b in parsed["data"]:
                    call = HexBytes(b).hex()
                    is_swap, parsed_call = self.decode_swap_function_input(call)
                    if is_swap:
                        is_swap_call = True
                    parsed_results.extend(parsed_call)
                return is_swap_call, parsed_results
            if "execute" in signature:
                return self.decode_swap_command_input(parsed["commands"], parsed["inputs"])
            return True, [parsed]
        except Exception as e:
            print(e)
            return False, []


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
    # input = "0x3593564c000000000000000000000000000000000000000000000000000000000000006000000000000000000000000000000000000000000000000000000000000000a00000000000000000000000000000000000000000000000000000000065dc466700000000000000000000000000000000000000000000000000000000000000040a08060c00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000400000000000000000000000000000000000000000000000000000000000000800000000000000000000000000000000000000000000000000000000000000200000000000000000000000000000000000000000000000000000000000000032000000000000000000000000000000000000000000000000000000000000003a00000000000000000000000000000000000000000000000000000000000000160000000000000000000000000a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48000000000000000000000000ffffffffffffffffffffffffffffffffffffffff000000000000000000000000000000000000000000000000000000006603d10000000000000000000000000000000000000000000000000000000000000000000000000000000000000000003fc91a3afd70395cd496c647d5a6cc9d4b2b7fad0000000000000000000000000000000000000000000000000000000065dc4b0800000000000000000000000000000000000000000000000000000000000000e000000000000000000000000000000000000000000000000000000000000000414e80e38cfd10836d1132ef09c990f10e2d76ce8eb237f61fc04f8f81d3d345fc203d1a75b5d89ba88f12309bd48dafe29cc2a4f7aef3b5044e973484bedee5201b0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001000000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000299f971000000000000000000000000000000000000000000000000003176d2724fa6d500000000000000000000000000000000000000000000000000000000000000a000000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000000000000000000002000000000000000000000000a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48000000000000000000000000c02aaa39b223fe8d0a0e5c4f27ead9083c756cc20000000000000000000000000000000000000000000000000000000000000060000000000000000000000000c02aaa39b223fe8d0a0e5c4f27ead9083c756cc200000000000000000000000037a8f295612602f2774d331e562be9e61b83a327000000000000000000000000000000000000000000000000000000000000000f00000000000000000000000000000000000000000000000000000000000000400000000000000000000000000000000000000000000000000000000000000001000000000000000000000000000000000000000000000000003163d3ec406d9f"
    # decoder = FunctionInputDecoder()
    # print(decoder.decode_swap_function_input(input))

    input = "0x5b0d5984000000000000000000000000c750ee1d84671048cce637a406249af88877ab71000000000000000000000000000000000000000000000000006bc25714dff592000000000000000000000000000000000000000000000000000235cfb97d685f000000000000000000000000000000000000000000000000142b5113940160ed00000000000000000000000030948fe32bbec0b1a76095908f0dd0600d8576430000000000000000000000000000000000000000000000000000000063c30a130000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001c9c9d0b05945c5f6d2f47d8ee0e52d9ba64d4afa28edc8a47ebb24ad8d0fcabdf0c587e090ad509ebecf4ef800d77546fdc60e28816d7c9ab283c91187cf6182d"
    decoder = FunctionInputDecoder()
    print(decoder.decode_remove_liq_function_input(input))

    # snt = [
    #     "swapExactTokensForTokens(uint256,uint256,address[],address,uint256)",
    #     "swapTokensForExactTokens(uint256,uint256,address[],address,uint256)",
    #     "swapExactETHForTokens(uint256,address[],address,uint256)",
    #     "swapTokensForExactETH(uint256,uint256,address[],address,uint256)",
    #     "swapExactTokensForETH(uint256,uint256,address[],address,uint256)",
    #     "swapETHForExactTokens(uint256,address[],address,uint256)",
    #     "swapExactTokensForTokensSupportingFeeOnTransferTokens(uint256,uint256,address[],address,uint256)",
    #     "swapExactETHForTokensSupportingFeeOnTransferTokens(uint256,address[],address,uint256)",
    #     "swapExactTokensForETHSupportingFeeOnTransferTokens(uint256,uint256,address[],address,uint256)",
    #     "swapExactTokensForTokens(uint256,uint256,address[],address)",
    #     "swapTokensForExactTokens(uint256,uint256,address[],address)",
    # ]
    # for st in snt:
    #     strg = ut.keccak_hash(st)
    # print(HexBytes(ut.keccak_hash("addLiquidityETH(address,uint256,uint256,uint256,address,uint256)"))[:4].hex())
