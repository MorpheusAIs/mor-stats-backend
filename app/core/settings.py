"""
Application settings module using Pydantic for configuration validation.
"""
from pathlib import Path
from typing import List, Optional

from pydantic import BaseSettings, Field, validator

# Project root directory
PROJECT_ROOT = Path(__file__).parent.parent.parent.absolute()


class DatabaseSettings(BaseSettings):
    """Database connection settings."""
    host: str = Field(..., env="DB_HOST")
    port: int = Field(5432, env="DB_PORT")
    database: str = Field(..., env="DB_NAME")
    user: str = Field(..., env="DB_USER")
    password: str = Field(..., env="DB_PASSWORD")
    minconn: int = Field(1, env="DB_MIN_CONN")
    maxconn: int = Field(10, env="DB_MAX_CONN")
    autocommit: bool = Field(False, env="DB_AUTOCOMMIT")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


class Web3Settings(BaseSettings):
    """Web3 connection settings."""
    eth_rpc_url: str = Field(..., env="RPC_URL")
    arb_rpc_url: str = Field(..., env="ARB_RPC_URL")
    base_rpc_url: str = Field(..., env="BASE_RPC_URL")
    etherscan_api_key: str = Field(..., env="ETHERSCAN_API_KEY")
    arbiscan_api_key: str = Field(..., env="ARBISCAN_API_KEY")
    basescan_api_key: str = Field(..., env="BASESCAN_API_KEY")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


class APISettings(BaseSettings):
    """API settings."""
    cors_origins: List[str] = Field(
        [
            "http://localhost:3000",
            "http://localhost:3001",
            "http://localhost:3002",
            "https://*.vercel.app",
            "https://morpheus-stats-frontend.vercel.app",
            "https://mor-stats-backend-cfcfatfxejhphfg9.centralus-01.azurewebsites.net"
        ],
        env="CORS_ORIGINS"
    )
    
    @validator("cors_origins", pre=True)
    def parse_cors_origins(cls, v):
        """Parse CORS origins from comma-separated string if provided as string."""
        if isinstance(v, str):
            return [origin.strip() for origin in v.split(",")]
        return v
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


class CacheSettings(BaseSettings):
    """Cache settings."""
    ttl_seconds: int = Field(12 * 60 * 60, env="CACHE_TTL_SECONDS")  # 12 hours default
    max_size: int = Field(1000, env="CACHE_MAX_SIZE")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


class ContractAddresses(BaseSettings):
    """Contract addresses."""
    burn_from_address: str = Field("0x151c2b49CdEC10B150B2763dF3d1C00D70C90956")
    burn_to_address: str = Field("0x000000000000000000000000000000000000dead")
    safe_address: str = Field("0xb1972e86B3380fd69DCb395F98D39fbF1A5f305A")
    supply_proxy_address: str = Field("0x6CFe1dDfd88890E08276c7FA9D6DCa1cA4A224a9")
    distribution_proxy_address: str = Field("0x47176B2Af9885dC6C4575d4eFd63895f7Aaa4790")
    mor_mainnet_address: str = Field("0xcbb8f1bda10b9696c57e13bc128fe674769dcec0")
    mor_arbitrum_address: str = Field("0x092bAaDB7DEf4C3981454dD9c0A0D7FF07bCFc86")
    mor_base_address: str = Field("0x7431aDa8a591C955a994a21710752EF9b882b8e3")
    steth_token_address: str = Field("0x5300000000000000000000000000000000000004")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


class BlockchainSettings(BaseSettings):
    """Blockchain-related settings."""
    burn_start_block: int = Field(0)
    mainnet_block_1st_jan_2024: int = Field(18913400)  # 1st January 2024
    average_block_time: int = Field(15)
    total_supply_historical_days: int = Field(30)
    total_supply_historical_start_block: int = Field(20432592)  # 1st August 2024
    prices_and_volume_data_days: int = Field(300)
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


class ExternalAPISettings(BaseSettings):
    """External API settings."""
    dune_api_key: str = Field(..., env="DUNE_API_KEY")
    dune_query_id: str = Field(..., env="DUNE_QUERY_ID")
    slack_url: Optional[str] = Field(None, env="SLACK_URL")
    github_api_key: str = Field(..., env="GITHUB_API_KEY")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


class Settings(BaseSettings):
    """Main application settings."""
    database: DatabaseSettings = DatabaseSettings()
    web3: Web3Settings = Web3Settings()
    api: APISettings = APISettings()
    cache: CacheSettings = CacheSettings()
    contracts: ContractAddresses = ContractAddresses()
    blockchain: BlockchainSettings = BlockchainSettings()
    external_apis: ExternalAPISettings = ExternalAPISettings()
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


# Create a global settings instance
settings = Settings()