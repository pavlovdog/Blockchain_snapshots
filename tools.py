import redis
from pprint import pprint

def update_address_balance(coin_name, address, balance, redis_instance):
    """
        Balances are stored in Redis in the format 'coin.address.balance'.
        This function increase / decrease address balance in Redis.

        Why I just don't use RAM & dict? Because sahring between threads is tough
        and because of possible atomic problems. Redis could solve this out of the box.
    """


def update_addresses_from_the_block(height, coin_provider):
    """
        Read all the txns from some block and update balances for each
        affected address.
    """

    txns_in_block = coin_provider.get_block_by_height(height)['tx']
    # r = redis.Redis(host='127.0.0.1', port=6379, db=1)

    for tx_hash in txns_in_block:
        # Genesis block txn
        if tx_hash == "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b": continue

        txn = coin_provider.get_txn_by_hash(tx_hash)  # Get txn as a JSON

        for vin in txn['vin']:
            print (vin)

        for vout in txn['vout']:  # Iterate on each output
            if len(vout['scriptPubKey']['addresses']) != 1: print (tx_hash)

    return None
