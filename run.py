from argparse import ArgumentParser
from multiprocessing import Pool
from sys import exit
from pprint import pprint
from tqdm import tqdm
from functools import partial

import yaml
import tools
import logging
import providers
import redis

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s [%(levelname)s] (%(filename)s, %(lineno)s) %(message)s")

# -- Console log
log_console = logging.StreamHandler()
log_console.setLevel(logging.INFO)
log_console.setFormatter(formatter)

logger.addHandler(log_console)

parser = ArgumentParser()
parser.add_argument('--conf', '-c', default="conf.yaml", type=str,
    help="Specify the path for the configuration file"
)

if __name__ == "__main__":
    options = parser.parse_args()  # Read command line arguments

    try:  # Read YAML configuration file
        with open(options.conf) as ff:
            conf = yaml.load(ff)
        logger.info("Successfully read config from the '{}'".format(options.conf))
    except yaml.YAMLError as e:
        logger.error("Can't read YAML configuration: {}".format(e))
        exit(1)

    coins = conf['general']['coins']
    logger.info("{} coins specified: {}".format(len(coins), ','.join(coins)))

    # Check RPC for each coin and exit if smth went wrong
    for coin_name in coins:
        if not conf['general']['check_rpc']: continue

        coin_provider = providers.by_name(coin_name, conf[coin_name])

        try: # Get the blockchain's height
            last_block = coin_provider.get_last_block()
            logger.info("'{}' height is {} blocks".format(coin_name, last_block))
        except Exception as e:
            logger.error("Error while checking the '{}' RPC - {}".format(coin_name, e))
            exit(1)

    # Drop Redis state for each coin name as a key
    r = redis.Redis(host='127.0.0.1', port=6379, db=1, decode_responses=True)
    for coin_name in coins:
        if conf['general']['drop_db']: r.delete(coin_name)

    # Get the blockchain snapshot for each coin
    for coin_name in coins:
        logger.info("Getting '{}' state...".format(coin_name))
        coin_provider = providers.by_name(coin_name, conf[coin_name])

        # Check that node's blockchain height >= requested height
        last_block = coin_provider.get_last_block()
        height_to_process = conf[coin_name]['block_number']

        # Check that requested blockchain height is available
        if last_block >= height_to_process:
            logger.info("Node has {} blocks ({} blocks requested)".format(last_block, height_to_process))
        else:
            logger.error("Node has only {} blocks ({} blocks requested)!".format(last_block, height_to_process))
            exit(1)

        p = Pool(conf['general']['blocks_pool_size'])
        part = partial(tools.update_addresses_from_the_block, coin_provider=coin_provider)
        result = list(tqdm(
            p.imap(part, range(height_to_process)),
            total=height_to_process,
            desc="Processing '{}' blocks".format(coin_name)
        ))

        p.close()
        p.join()
