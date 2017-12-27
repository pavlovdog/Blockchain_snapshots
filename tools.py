import redis
from pprint import pprint

def update_address_balance(r, coin_name, address, balance_change):
    """
        Balances are stored in Redis in the format 'coin.address.balance'.
        This function increase / decrease address balance in Redis.

        Why I just don't use RAM & dict? Because sahring between threads is tough
        and because of possible atomic problems. Redis could solve this out of the box.
    """

    # Get balance for this address
    current_address_balance = r.hmget(coin_name, address)[0]
    # This address didn't appeared before
    if current_address_balance is None: current_address_balance = 0

    # Save new balance
    r.hmset(coin_name, {address : float(current_address_balance) + balance_change})


def update_addresses_from_the_block(height, coin_provider):
    """
        Read all the txns from some block and update balances for each
        affected address.
    """

    txns_in_block = coin_provider.get_block_by_height(height)['tx']
    r = redis.Redis(host='127.0.0.1', port=6379, db=1, decode_responses=True)

    for tx_hash in txns_in_block:
        # Genesis block txn
        if tx_hash == "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b": continue

        txn = coin_provider.get_txn_by_hash(tx_hash)  # Get txn as a JSON

        for vin in txn['vin']:
            if 'coinbase' in vin.keys(): continue  # Coinbase transaction
            txid, output_number = vin['txid'], vin['vout']
            vout = coin_provider.get_txn_by_hash(txid)['vout'][output_number]
            address = vout['scriptPubKey']['addresses'][0]
            update_address_balance(r, coin_provider.name, address, -vout['value'])

        for vout in txn['vout']:  # Iterate on each output
            if len(vout['scriptPubKey']['addresses']) != 1: print (tx_hash)
            address = vout['scriptPubKey']['addresses'][0]
            update_address_balance(r, coin_provider.name, address, vout['value'])

    return None
