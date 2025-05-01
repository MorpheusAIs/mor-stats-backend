from web3 import Web3

from app.core.config import ETH_RPC_URL


async def get_block_number():
    latest_block = Web3Provider.get_instance().get_block('latest', full_transactions=False)
    return latest_block['number']


class Web3Provider:
    _instance = None

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = Web3(Web3.HTTPProvider(ETH_RPC_URL))
        return cls._instance

