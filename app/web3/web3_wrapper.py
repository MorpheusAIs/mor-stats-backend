from web3 import Web3

from app.core.config import ETH_RPC_URL

class Web3Provider:
    _instance = None

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = Web3(Web3.HTTPProvider(ETH_RPC_URL))
        return cls._instance
