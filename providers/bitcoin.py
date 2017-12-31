import requests
import json

class BitcoinProvider(object):
    """This object provide methods for worling with bitcoind trough the RPC"""

    def __init__(self, conf):
        self.name = 'bitcoin'
        self.conf = conf
        self.rpc_url = "http://{}:{}@{}:{}".format(
            conf['rpc_user'], conf['rpc_password'],
            conf['rpc_ip'], conf['rpc_port']
        )
        self.rpc_headers = {'content-type' : 'application/json'}

    def get_info(self):
        """Get general info about node"""
        payload = json.dumps({'method':'getinfo', 'params' : [], "jsonrpc": "2.0"})
        r = requests.post(self.rpc_url, headers=self.rpc_headers, data=payload)

        try:
            return r.json()['result']
        except Exception as e:
            return self.get_info()

    def get_last_block(self):
        """Get the height of the current chain"""
        node_info = self.get_info()
        return node_info["blocks"]

        try:
            return r.json()['result']
        except Exception as e:
            return self.get_last_block()

    def get_block_by_height(self, height):
        """Get the JSON representation of the block by it's height"""
        block_hash = self.get_block_hash_by_height(height)
        return self.get_block_by_hash(block_hash)

    def get_block_hash_by_height(self, height):
        """Get the block's hash by it's height"""
        payload = json.dumps({'method':'getblockhash', 'params' : [height], "jsonrpc": "2.0"})
        r = requests.post(self.rpc_url, headers=self.rpc_headers, data=payload)

        try:
            return r.json()['result']
        except Exception as e:
            return self.get_block_hash_by_height(height)

    def get_block_by_hash(self, hash_):
        """Get JSON representation of the block by it's hash"""
        payload = json.dumps({'method':'getblock', 'params' : [hash_], "jsonrpc": "2.0"})
        r = requests.post(self.rpc_url, headers=self.rpc_headers, data=payload)

        try:
            return r.json()['result']
        except Exception as e:
            return self.get_block_by_hash(hash_)

    def get_txn_by_hash(self, hash_):
        """Get JSON representation of the txn by it's hash"""
        payload = json.dumps({'method':'getrawtransaction', 'params' : [hash_, 1], "jsonrpc": "2.0"})
        r = requests.post(self.rpc_url, headers=self.rpc_headers, data=payload)

        try:
            return r.json()['result']
        except Exception as e:
            return self.get_txn_by_hash(hash_)
