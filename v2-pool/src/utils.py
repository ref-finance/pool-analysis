from near_special_rpc import SpecialNodeJsonProviderError,  SpecialNodeJsonProvider
from base64 import b64encode, b64decode
import json
import os
import time

from config import Cfg

#RPC_ENDPOINT = "https://rpc.blockpi.io/b571d535a0e3f9c2c198d6b9d5d873f5bd859f60"
#RPC_ENDPOINT = "https://rpc.mainnet.near.org"
#RPC_ENDPOINT = "http://51.159.21.86:3030" # outdated
RPC_ENDPOINT = "http://161.117.178.13:3030"
#RPC_ENDPOINT = "https://rpc.testnet.near.org" # test net

def sort_dict(item: dict):
    for k, v in sorted(item.items()):
        item[k] = sorted(v) if isinstance(v, list) else v
    return {k: sort_dict(v) if isinstance(v, dict) else v for k, v in sorted(item.items())}

def parse_pool_id( pool_id ):
   a = pool_id.split("|",2)
   token_x = a[0]
   token_y = a[1]
   fee = int(a[2])
   return (token_x, token_y, fee)

def OpenFile(filepath):
   with open(filepath, mode='r', encoding="utf-8") as f:
      json_obj = json.load(f)
   return json_obj

def open_info_file(filename):
    filepath = "output/height_%s/%s.json" % (Cfg.BLOCK_ID, filename)
    if os.path.exists(filepath):
        json_obj = None
        with open(filepath, mode='r', encoding="utf-8") as f:
            json_obj = json.load(f)
        # print("[INFO] %s loaded" % filepath)
        return json_obj
    else:
        print("[WARNING] file %s not exist, consider genrate them first." % filepath)
        raise Exception("File not exist. %s" % (filepath, )) 

def gen_rawdata_filepath(filename):
    # if dir not exist, create it
    filepath = "data/height_%s" % (Cfg.BLOCK_ID, )
    if not os.path.exists(filepath):
        print("[WARNING]Path not exist, create %s" % filepath)
        os.makedirs(filepath)
    return "%s/%s.json" % (filepath, filename)


def gen_info_filepath(filename):
    # if dir not exist, create it
    filepath = "output/height_%s" % (Cfg.BLOCK_ID, )
    if not os.path.exists(filepath):
        print("[WARNING]Path not exist, create %s" % filepath)
        os.makedirs(filepath)
    return "%s/%s.json" % (filepath, filename)


def save2file(filename, json_obj, info=True, sort_keys=False):
    # print(json_obj)
    filepath = gen_info_filepath(
        filename) if info else gen_rawdata_filepath(filename)
    with open(filepath, mode='w', encoding="utf-8") as f:
        # f.write(json.dumps(json_obj))
        json.dump(json_obj, f, indent = 2, sort_keys = sort_keys)
        print("%s saved" % filepath)

def get_last_block_height():
    ret = ""
    try:
        conn = SpecialNodeJsonProvider(RPC_ENDPOINT)
        ret = conn.get_start_block()

    except SpecialNodeJsonProviderError as e:
        print("RPC Error: ", e)
    except Exception as e:
        print("Error: ", e)

    return ret


def fetch_state(contract_id, prefix, filename=None):
    count = 0
    prefix_key = b''
    if prefix:
        prefix_key = b64encode(prefix)

    query_args = {
        "request_type": "view_state",
        "finality": "final",
        "account_id": contract_id,
        "prefix_base64": prefix_key.decode(),
    }
    try:
        conn = SpecialNodeJsonProvider(RPC_ENDPOINT)
        ret = conn.query(query_args)
        count = len(ret['values'])
        if filename:
            save2file(filename, ret)
    except SpecialNodeJsonProviderError as e:
        print("RPC Error: ", e)
    except Exception as e:
        print("Error: ", e)

    return count


def fetch_state_at_height(height, contract_id, prefix, filename):
    count = 0
    # skip if rawdata already exit
    filepath = gen_rawdata_filepath(filename)
    if os.path.exists(filepath):
        json_obj = None
        with open(filepath, mode='r', encoding="utf-8") as f:
            json_obj = json.load(f)
        if json_obj is not None and 'values' in json_obj:
            count = len(json_obj['values'])
        return count

    prefix_key = b''
    if prefix:
        prefix_key = b64encode(prefix)

    query_args = {
        "request_type": "view_state",
        "block_id": height,
        "account_id": contract_id,
        "prefix_base64": prefix_key.decode(),
    }

    flag = False
    for i in range(10):
        if i > 0:
            print("Retry after %d seconds ..." % i)
            time.sleep(i)
        try:
            conn = SpecialNodeJsonProvider(RPC_ENDPOINT)
            ret = conn.query(query_args)
            count = len(ret['values'])
            if filename:
                save2file(filename, ret, info=False)
            flag = True
            break
        except SpecialNodeJsonProviderError as e:
            print("RPC Error: ", e)
        except Exception as e:
            print("Error: ", e)

    if not flag:
        raise Exception("Error fetch state on %s to get %s" %
                        (contract_id, filename))

    return count

def get_user_liquidity(contract_id, lpt_id):
    query_args = {"lpt_id": lpt_id}
    try:
        conn = SpecialNodeJsonProvider(RPC_ENDPOINT)
        ret = conn.view_call(account_id=contract_id, method_name = "get_liquidity", args=json.dumps(query_args).encode('utf8'))
        ret['result'] = json.loads(''.join([chr(x) for x in ret['result']]))
        return ret['result']
    except SpecialNodeJsonProviderError as e:
        print("RPC Error: ", e)
    except Exception as e:
        print("Error: ", e)

    return None

def get_ft_metadata(contract_id):
    query_args = {}
    try:
        conn = SpecialNodeJsonProvider(RPC_ENDPOINT)
        ret = conn.view_call(account_id=contract_id, method_name = "ft_metadata", args=json.dumps(query_args).encode('utf8'))
        ret['result'] = json.loads(''.join([chr(x) for x in ret['result']]))
        return ret['result']
    except SpecialNodeJsonProviderError as e:
        print("RPC Error: ", e)
    except Exception as e:
        print("Error: ", e)

    return None

def fetch_ft_balance_at_height(token_id, account_id, height):
    ret = None

    flag = False

    try:
        #conn = JsonProvider(("172.21.120.89", 3030))
        conn = SpecialNodeJsonProvider(RPC_ENDPOINT)
        ret = conn.view_call_at_height(token_id, "ft_balance_of", bytes('{"account_id": "%s"}' % account_id, encoding='utf-8'), height)
        flag = True
    except SpecialNodeJsonProviderError as e:
        print("RPC Error: ", e)
    except Exception as e:
        print("Error: ", e)

    if not flag:
        raise Exception("Error fetch %s balance for %s on %s" %
                        (token_id, account_id, height))

    return b''.join(map(lambda x:int.to_bytes(x,1,'little'), ret['result']))[1:-1]

if __name__ == '__main__':
    pass
