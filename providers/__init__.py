from providers.bitcoin import BitcoinProvider

def by_name(coin_name, coin_conf):
    name_provider = {
        "bitcoin" : BitcoinProvider,
    }

    return name_provider[coin_name](coin_conf)
