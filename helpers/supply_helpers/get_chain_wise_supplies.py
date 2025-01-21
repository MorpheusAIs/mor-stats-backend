import requests
from app.core.config import (MOR_BASE_ADDRESS, MOR_ARBITRUM_ADDRESS, MOR_MAINNET_ADDRESS,
                             ETHERSCAN_API_KEY, ARBISCAN_API_KEY, BASESCAN_API_KEY)


def get_chain_wise_circ_supply():
    chains = ["ethereum", "arbitrum", "base"]
    supplies = {}

    for chain in chains:
        if chain.lower() == "ethereum":
            api_url = (f"https://api.etherscan.io/api?module=stats&action=tokensupply"
                       f"&contractaddress={MOR_MAINNET_ADDRESS}"
                       f"&apikey={ETHERSCAN_API_KEY}")

        elif chain.lower() == "arbitrum":
            api_url = (f"https://api.arbiscan.io/api?module=stats&action=tokensupply"
                       f"&contractaddress={MOR_ARBITRUM_ADDRESS}"
                       f"&apikey={ARBISCAN_API_KEY}")

        elif chain.lower() == "base":
            api_url = (f"https://api.basescan.org/api?module=stats&action=tokensupply"
                       f"&contractaddress={MOR_BASE_ADDRESS}"
                       f"&apikey={BASESCAN_API_KEY}")
        else:
            return 0

        response = requests.get(api_url)
        response = response.json()
        data = round(float(response["result"]) / 1e18, 4)

        supplies[chain] = data

    return supplies