"""
Enhanced PostgreSQL database wrapper with connection pooling and retry logic.
"""
import logging
import time
from contextlib import contextmanager
from dataclasses import dataclass
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Sequence, TypeVar, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool

from app.core.exceptions import DatabaseError

logger = logging.getLogger(__name__)

# Type variable for function return type
T = TypeVar('T')


@dataclass(slots=True)
class DBConfig:
    """Database configuration."""
    host: str = "localhost"
    port: int = 5432
    database: str = "postgres"
    user: str = "postgres"
    password: str = "postgres"
    minconn: int = 1
    maxconn: int = 10
    autocommit: bool = False


def with_retry(
    max_retries: int = 3,
    initial_backoff: float = 0.5,
    max_backoff: float = 10.0,
    backoff_factor: float = 2.0,
    retryable_exceptions: tuple = (psycopg2.OperationalError, psycopg2.InterfaceError)
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Decorator for retrying database operations with exponential backoff.
    
    Args:
        max_retries: Maximum number of retry attempts
        initial_backoff: Initial backoff time in seconds
        max_backoff: Maximum backoff time in seconds
        backoff_factor: Factor to multiply backoff time by after each retry
        retryable_exceptions: Tuple of exceptions that should trigger a retry
        
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
                except retryable_exceptions as e:
                    retries += 1
                    if retries > max_retries:
                        logger.error(
                            f"Max retries ({max_retries}) exceeded for {func.__name__}: {str(e)}"
                        )
                        raise DatabaseError(
                            message=f"Database operation failed after {max_retries} retries",
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


class Database:
    """Enhanced PostgreSQL database wrapper with connection pooling and retry logic."""
    
    def __init__(self, cfg: DBConfig) -> None:
        """
        Initialize the database wrapper.
        
        Args:
            cfg: Database configuration
        """
        self._cfg = cfg
        self._pool = self._create_pool()
    
    def _create_pool(self) -> ThreadedConnectionPool:
        """
        Create a new connection pool.
        
        Returns:
            ThreadedConnectionPool: The connection pool
            
        Raises:
            DatabaseError: If the connection pool cannot be created
        """
        try:
            return ThreadedConnectionPool(
                self._cfg.minconn,
                self._cfg.maxconn,
                host=self._cfg.host,
                port=self._cfg.port,
                dbname=self._cfg.database,
                user=self._cfg.user,
                password=self._cfg.password,
            )
        except psycopg2.Error as e:
            logger.error(f"Failed to create connection pool: {str(e)}")
            raise DatabaseError(
                message="Failed to create database connection pool",
                details={"error": str(e)}
            )
    
    @with_retry()
    def _acquire(self):
        """
        Acquire a connection from the pool with retry logic.
        
        Returns:
            Connection: A database connection
            
        Raises:
            DatabaseError: If a connection cannot be acquired
        """
        try:
            conn = self._pool.getconn()
            conn.autocommit = self._cfg.autocommit
            return conn
        except psycopg2.Error as e:
            logger.error(f"Failed to acquire connection from pool: {str(e)}")
            # Try to recreate the pool if it's exhausted or broken
            try:
                self._pool = self._create_pool()
                conn = self._pool.getconn()
                conn.autocommit = self._cfg.autocommit
                return conn
            except psycopg2.Error as e2:
                logger.error(f"Failed to recreate connection pool: {str(e2)}")
                raise DatabaseError(
                    message="Failed to acquire database connection",
                    details={"error": str(e2)}
                )
    
    def _release(self, conn):
        """
        Release a connection back to the pool.
        
        Args:
            conn: The connection to release
        """
        try:
            self._pool.putconn(conn)
        except psycopg2.Error as e:
            logger.warning(f"Failed to release connection to pool: {str(e)}")
            # Just log the error, don't raise an exception
    
    @contextmanager
    def cursor(self, *, dict_cursor: bool = True):
        """
        Context manager for database cursors.
        
        Args:
            dict_cursor: Whether to use a dictionary cursor
            
        Yields:
            Cursor: A database cursor
        """
        conn = self._acquire()
        try:
            with conn.cursor() as cur:
                yield cur
                if not conn.autocommit:
                    conn.commit()
        except Exception:
            if not conn.autocommit:
                conn.rollback()
            raise
        finally:
            self._release(conn)
    
    @with_retry()
    def execute(self, sql: str, params: Optional[Sequence[Any]] = None) -> None:
        """
        Execute a SQL statement.
        
        Args:
            sql: The SQL statement
            params: The parameters for the SQL statement
        """
        with self.cursor() as cur:
            cur.execute(sql, params)
    
    @with_retry()
    def fetchone(
        self, sql: str, params: Optional[Sequence[Any]] = None
    ) -> Optional[Tuple[Any]]:
        """
        Fetch a single row from the database.
        
        Args:
            sql: The SQL statement
            params: The parameters for the SQL statement
            
        Returns:
            Optional[Dict[str, Any]]: The row as a dictionary, or None if no row was found
        """
        with self.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()
    
    @with_retry()
    def fetchall(
        self, sql: str, params: Optional[Sequence[Any]] = None
    ) -> List[Tuple[Any]]:
        """
        Fetch all rows from the database.
        
        Args:
            sql: The SQL statement
            params: The parameters for the SQL statement
            
        Returns:
            List[Dict[str, Any]]: The rows as dictionaries
        """
        with self.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()
    
    @contextmanager
    def transaction(self):
        """
        Context manager for transactions.
        
        Yields:
            Cursor: A database cursor
        """
        conn = self._acquire()
        try:
            with conn.cursor() as cur:
                yield cur
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self._release(conn)
    
    def close(self):
        """Close all connections in the pool."""
        if hasattr(self, '_pool'):
            self._pool.closeall()
    
    def health_check(self) -> bool:
        """
        Check if the database is healthy.
        
        Returns:
            bool: True if the database is healthy, False otherwise
        """
        try:
            result = self.fetchone("SELECT 1")
            logger.info(f'db health result: {str(result)}')
            return result is not None and result[0] == 1
        except Exception as e:
            logger.error(f"Database health check failed: {str(e)}")
            return False


# Global database instance
_db: Optional[Database] = None


def init_db(cfg: DBConfig) -> Database:
    """
    Initialize the global database instance.
    
    Args:
        cfg: Database configuration
        
    Returns:
        Database: The database instance
        
    Raises:
        RuntimeError: If the database is already initialized
    """
    global _db
    if _db is not None:
        raise RuntimeError("Database already initialized")
    
    _db = Database(cfg)
    return _db


def get_db() -> Database:
    """
    Get the global database instance.
    
    Returns:
        Database: The database instance
        
    Raises:
        RuntimeError: If the database is not initialized
    """
    if _db is None:
        raise RuntimeError("Database not initialized – call init_db() first")
    return _db
