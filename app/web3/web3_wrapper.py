"""
Enhanced Web3 wrapper with retry logic and better error handling.
"""
import logging
import time
from functools import wraps
from typing import Any, Callable, Dict, Optional, TypeVar

from web3 import Web3
from web3.exceptions import BlockNotFound, ContractLogicError, TransactionNotFound, Web3Exception

from app.core.exceptions import Web3Error
from app.core.settings import settings

logger = logging.getLogger(__name__)

# Type variable for function return type
T = TypeVar('T')


def with_retry(
    max_retries: int = 3,
    initial_backoff: float = 0.5,
    max_backoff: float = 10.0,
    backoff_factor: float = 2.0
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Decorator for retrying Web3 operations with exponential backoff.
    
    Args:
        max_retries: Maximum number of retry attempts
        initial_backoff: Initial backoff time in seconds
        max_backoff: Maximum backoff time in seconds
        backoff_factor: Factor to multiply backoff time by after each retry
        
    Returns:
        Decorator function
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            retries = 0
            backoff = initial_backoff
            
            while True:
                try:
                    return func(*args, **kwargs)
                except (BlockNotFound, TransactionNotFound, ContractLogicError, Web3Exception) as e:
                    retries += 1
                    if retries > max_retries:
                        logger.error(
                            f"Max retries ({max_retries}) exceeded for {func.__name__}: {str(e)}"
                        )
                        raise Web3Error(
                            message=f"Blockchain operation failed after {max_retries} retries",
                            details={"error": str(e), "function": func.__name__}
                        )
                    
                    # Calculate backoff time with jitter
                    import random
                    jitter = random.uniform(0.8, 1.2)
                    sleep_time = min(backoff * jitter, max_backoff)
                    
                    logger.warning(
                        f"Retry {retries}/{max_retries} for {func.__name__} after {sleep_time:.2f}s: {str(e)}"
                    )
                    time.sleep(sleep_time)
                    
                    # Increase backoff for next retry
                    backoff = min(backoff * backoff_factor, max_backoff)
        
        return wrapper
    
    return decorator


class Web3Provider:
    """Enhanced Web3 provider with retry logic and fallback providers."""
    
    _instance: Optional[Web3] = None
    _fallback_instances: Dict[str, Web3] = {}
    
    @classmethod
    def get_instance(cls) -> Web3:
        """
        Get the primary Web3 instance, initializing it if necessary.
        
        Returns:
            Web3: The Web3 instance
        """
        if cls._instance is None:
            try:
                cls._instance = Web3(Web3.HTTPProvider(
                    settings.web3.eth_rpc_url,
                    request_kwargs={"timeout": 30}
                ))
                # Test connection
                cls._instance.eth.chain_id
                logger.info(f"Connected to Ethereum node at {settings.web3.eth_rpc_url}")
            except Exception as e:
                logger.error(f"Failed to connect to primary Ethereum node: {str(e)}")
                # Try to use a fallback if available
                cls._instance = cls._get_fallback_instance()
        
        return cls._instance
    
    @classmethod
    def get_arb_instance(cls) -> Web3:
        """
        Get the Arbitrum Web3 instance.
        
        Returns:
            Web3: The Arbitrum Web3 instance
        """
        if "arbitrum" not in cls._fallback_instances:
            try:
                cls._fallback_instances["arbitrum"] = Web3(Web3.HTTPProvider(
                    settings.web3.arb_rpc_url,
                    request_kwargs={"timeout": 30}
                ))
                # Test connection
                cls._fallback_instances["arbitrum"].eth.chain_id
                logger.info(f"Connected to Arbitrum node at {settings.web3.arb_rpc_url}")
            except Exception as e:
                logger.error(f"Failed to connect to Arbitrum node: {str(e)}")
                raise Web3Error(
                    message="Failed to connect to Arbitrum node",
                    details={"error": str(e)}
                )
        
        return cls._fallback_instances["arbitrum"]
    
    @classmethod
    def get_base_instance(cls) -> Web3:
        """
        Get the Base Web3 instance.
        
        Returns:
            Web3: The Base Web3 instance
        """
        if "base" not in cls._fallback_instances:
            try:
                cls._fallback_instances["base"] = Web3(Web3.HTTPProvider(
                    settings.web3.base_rpc_url,
                    request_kwargs={"timeout": 30}
                ))
                # Test connection
                cls._fallback_instances["base"].eth.chain_id
                logger.info(f"Connected to Base node at {settings.web3.base_rpc_url}")
            except Exception as e:
                logger.error(f"Failed to connect to Base node: {str(e)}")
                raise Web3Error(
                    message="Failed to connect to Base node",
                    details={"error": str(e)}
                )
        
        return cls._fallback_instances["base"]
    
    @classmethod
    def _get_fallback_instance(cls) -> Web3:
        """
        Try to get a fallback Web3 instance.
        
        Returns:
            Web3: A fallback Web3 instance
            
        Raises:
            Web3Error: If no fallback is available
        """
        # This is a placeholder for a more sophisticated fallback mechanism
        # In a real implementation, you might have multiple backup RPC endpoints
        fallback_urls = [
            "https://eth.llamarpc.com",
            "https://rpc.ankr.com/eth",
            "https://ethereum.publicnode.com"
        ]
        
        for url in fallback_urls:
            try:
                instance = Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": 30}))
                # Test connection
                instance.eth.chain_id
                logger.info(f"Connected to fallback Ethereum node at {url}")
                return instance
            except Exception as e:
                logger.warning(f"Failed to connect to fallback at {url}: {str(e)}")
        
        # If we get here, all fallbacks failed
        raise Web3Error(
            message="Failed to connect to any Ethereum node",
            details={"tried_urls": [settings.web3.eth_rpc_url] + fallback_urls}
        )


@with_retry()
async def get_block_number() -> int:
    """
    Get the latest block number with retry logic.
    
    Returns:
        int: The latest block number
    """
    latest_block = Web3Provider.get_instance().eth.get_block('latest', full_transactions=False)
    return latest_block['number']
