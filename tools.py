import redis
import logging

from pprint import pprint
from multiprocessing.pool import ThreadPool as Pool
from functools import partial

logger = logging.getLogger('Blockchain_snapshots')

def update_address_balance(r, coin_name, address, balance_change):
    """
        Balances are stored in Redis in the format 'coin.address.balance'.
        This function increase / decrease address balance in Redis.

        Why I just don't use RAM & dict? Because sharing between threads is tough
        and because of possible atomic problems. Redis could solve this out of the box.
    """

    # Get balance for this address
    current_address_balance = r.hmget(coin_name, address)[0]
    # This address didn't appeared before
    if current_address_balance is None: current_address_balance = 0

    # Save new balance
    r.hmset(coin_name, {address : float(current_address_balance) + balance_change})

def get_affected_addresses(tx_hash, coin_provider):
    """
        For any txn, process each input and output and return
        the balance change for each address, affected by this txn.

        Return : dict {address : balance_change}
    """

    # Genesis block txn
    if tx_hash == "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b": return {}

    affected_addresses = {}
    txn = coin_provider.get_txn_by_hash(tx_hash)

    for vin in txn['vin']: # Iterate on each input
        if 'coinbase' in vin.keys(): continue  # Coinbase transaction
        txid, output_number = vin['txid'], vin['vout']
        vout = coin_provider.get_txn_by_hash(txid)['vout'][output_number]
        if 'addresses' not in vout['scriptPubKey']: continue

        address = vout['scriptPubKey']['addresses'][0]
        affected_addresses[address] = affected_addresses.get(address, 0) - vout['value']

    for vout in txn['vout']:  # Iterate on each output
        if 'addresses' not in vout['scriptPubKey']: continue
        if len(vout['scriptPubKey']['addresses']) != 1: continue

        address = vout['scriptPubKey']['addresses'][0]
        affected_addresses[address] = affected_addresses.get(address, 0) + vout['value']

    return affected_addresses

def update_addresses_from_the_block(height, coin_provider, **kwargs):
    """
        Read all the txns from some block and update balances for each
        affected address.
    """

    logger.debug("Start processing block \#{}".format(height))

    txns_in_block = coin_provider.get_block_by_height(height)['tx']

    r = redis.Redis(host='127.0.0.1', port=6379, db=1, decode_responses=True)

    p = Pool(kwargs.get('pool_size', 5))
    part = partial(get_affected_addresses, coin_provider=coin_provider)
    affected_addresses = p.map(part, txns_in_block)

    for aff in affected_addresses:
        for aff_address in aff:
            update_address_balance(r, coin_provider.name, aff_address, aff[aff_address])

    p.close()
    p.join()

    logger.debug("Finish processing block \#{}".format(height))

    return None
