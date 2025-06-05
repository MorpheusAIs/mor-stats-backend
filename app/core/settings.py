"""
Application settings module using direct environment variable access.
"""
import os
from pathlib import Path
from typing import List

# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv()

# Project root directory
PROJECT_ROOT = Path(__file__).parent.parent.parent.absolute()


class DatabaseSettings:
    """Database connection settings."""
    
    @property
    def host(self) -> str:
        return os.getenv("DB_HOST", "localhost")
    
    @property
    def port(self) -> int:
        return int(os.getenv("DB_PORT", "5432"))
    
    @property
    def database(self) -> str:
        return os.getenv("DB_NAME", "postgres")
    
    @property
    def user(self) -> str:
        return os.getenv("DB_USER", "postgres")
    
    @property
    def password(self) -> str:
        return os.getenv("DB_PASSWORD", "postgres")
    
    @property
    def minconn(self) -> int:
        return int(os.getenv("DB_MIN_CONN", "1"))
    
    @property
    def maxconn(self) -> int:
        return int(os.getenv("DB_MAX_CONN", "10"))
    
    @property
    def autocommit(self) -> bool:
        return os.getenv("DB_AUTOCOMMIT", "False").lower() in ("true", "1", "yes")


class Web3Settings:
    """Web3 connection settings."""
    
    @property
    def eth_rpc_url(self) -> str:
        return os.getenv("RPC_URL", "")
    
    @property
    def arb_rpc_url(self) -> str:
        return os.getenv("ARB_RPC_URL", "")
    
    @property
    def base_rpc_url(self) -> str:
        return os.getenv("BASE_RPC_URL", "")
    
    @property
    def etherscan_api_key(self) -> str:
        return os.getenv("ETHERSCAN_API_KEY", "")
    
    @property
    def arbiscan_api_key(self) -> str:
        return os.getenv("ARBISCAN_API_KEY", "")
    
    @property
    def basescan_api_key(self) -> str:
        return os.getenv("BASESCAN_API_KEY", "")


class APISettings:
    """API settings."""
    
    @property
    def cors_origins(self) -> List[str]:
        default_origins = [
            "http://localhost:3000",
            "http://localhost:3001",
            "http://localhost:3002",
            "https://*.vercel.app",
            "https://morpheus-stats-frontend.vercel.app",
            "https://mor-stats-backend-cfcfatfxejhphfg9.centralus-01.azurewebsites.net"
        ]
        
        origins = os.getenv("CORS_ORIGINS", ",".join(default_origins))
        if isinstance(origins, str):
            return [origin.strip() for origin in origins.split(",")]
        return default_origins


class CacheSettings:
    """Cache settings."""
    
    @property
    def ttl_seconds(self) -> int:
        return int(os.getenv("CACHE_TTL_SECONDS", str(12 * 60 * 60)))  # 12 hours default
    
    @property
    def max_size(self) -> int:
        return int(os.getenv("CACHE_MAX_SIZE", "1000"))


class ContractAddresses:
    """Contract addresses."""
    
    @property
    def burn_from_address(self) -> str:
        return os.getenv("BURN_FROM_ADDRESS", "0x151c2b49CdEC10B150B2763dF3d1C00D70C90956")
    
    @property
    def burn_to_address(self) -> str:
        return os.getenv("BURN_TO_ADDRESS", "0x000000000000000000000000000000000000dead")
    
    @property
    def safe_address(self) -> str:
        return os.getenv("SAFE_ADDRESS", "0xb1972e86B3380fd69DCb395F98D39fbF1A5f305A")
    
    @property
    def supply_proxy_address(self) -> str:
        return os.getenv("SUPPLY_PROXY_ADDRESS", "0x6CFe1dDfd88890E08276c7FA9D6DCa1cA4A224a9")
    
    @property
    def distribution_proxy_address(self) -> str:
        return os.getenv("DISTRIBUTION_PROXY_ADDRESS", "0x47176B2Af9885dC6C4575d4eFd63895f7Aaa4790")
    
    @property
    def mor_mainnet_address(self) -> str:
        return os.getenv("MOR_MAINNET_ADDRESS", "0xcbb8f1bda10b9696c57e13bc128fe674769dcec0")
    
    @property
    def mor_arbitrum_address(self) -> str:
        return os.getenv("MOR_ARBITRUM_ADDRESS", "0x092bAaDB7DEf4C3981454dD9c0A0D7FF07bCFc86")
    
    @property
    def mor_base_address(self) -> str:
        return os.getenv("MOR_BASE_ADDRESS", "0x7431aDa8a591C955a994a21710752EF9b882b8e3")
    
    @property
    def steth_token_address(self) -> str:
        return os.getenv("STETH_TOKEN_ADDRESS", "0x5300000000000000000000000000000000000004")


class BlockchainSettings:
    """Blockchain-related settings."""
    
    @property
    def burn_start_block(self) -> int:
        return int(os.getenv("BURN_START_BLOCK", "0"))
    
    @property
    def mainnet_block_1st_jan_2024(self) -> int:
        return int(os.getenv("MAINNET_BLOCK_1ST_JAN_2024", "18913400"))  # 1st January 2024
    
    @property
    def average_block_time(self) -> int:
        return int(os.getenv("AVERAGE_BLOCK_TIME", "15"))
    
    @property
    def total_supply_historical_days(self) -> int:
        return int(os.getenv("TOTAL_SUPPLY_HISTORICAL_DAYS", "30"))
    
    @property
    def total_supply_historical_start_block(self) -> int:
        return int(os.getenv("TOTAL_SUPPLY_HISTORICAL_START_BLOCK", "20432592"))  # 1st August 2024
    
    @property
    def prices_and_volume_data_days(self) -> int:
        return int(os.getenv("PRICES_AND_VOLUME_DATA_DAYS", "300"))


class ExternalAPISettings:
    """External API settings."""
    
    @property
    def dune_api_key(self) -> str:
        return os.getenv("DUNE_API_KEY", "")
    
    @property
    def dune_query_id(self) -> str:
        return os.getenv("DUNE_QUERY_ID", "")
    
    @property
    def github_api_key(self) -> str:
        return os.getenv("GITHUB_API_KEY", "")


class Settings:
    """Main application settings."""
    
    def __init__(self):
        self.database = DatabaseSettings()
        self.web3 = Web3Settings()
        self.api = APISettings()
        self.cache = CacheSettings()
        self.contracts = ContractAddresses()
        self.blockchain = BlockchainSettings()
        self.external_apis = ExternalAPISettings()


# Create a global settings instance
settings = Settings()