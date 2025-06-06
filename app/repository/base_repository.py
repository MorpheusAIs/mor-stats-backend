"""
Base repository class with common CRUD operations.
"""
from typing import Any, Dict, Generic, List, Optional, Type, TypeVar

from pydantic import BaseModel

from app.db.database import get_db

# Type variable for the model
T = TypeVar('T', bound=BaseModel)


class BaseRepository(Generic[T]):
    """Base repository with common CRUD operations."""

    def __init__(self, model_class: Type[T], table_name: str):
        """
        Initialize the repository.
        
        Args:
            model_class: The Pydantic model class
            table_name: The database table name
        """
        self.model_class = model_class
        self.table_name = table_name
        self.db = get_db()

    def create(self, data: T) -> T:
        """
        Create a new record.
        
        Args:
            data: The data to insert
            
        Returns:
            The created record
        """
        # Convert model to dict, excluding None values and id if it's None
        data_dict = data.model_dump(exclude_none=True)
        if 'id' in data_dict and data_dict['id'] is None:
            del data_dict['id']

        # Build the SQL query
        columns = ', '.join(data_dict.keys())
        placeholders = ', '.join(['%s'] * len(data_dict))
        values = list(data_dict.values())

        sql = f"""
        INSERT INTO {self.table_name} ({columns})
        VALUES ({placeholders})
        RETURNING *
        """

        # Execute the query directly with a cursor to get column names
        with self.db.cursor() as cur:
            cur.execute(sql, values)
            
            # Get column names from cursor description
            columns = [desc[0] for desc in cur.description]
            
            # Fetch the result
            result = cur.fetchone()
            
            if result:
                # Convert tuple to dictionary using column names
                dict_result = dict(zip(columns, result))
                return self.model_class(**dict_result)
            else:
                return None

    def get_by_id(self, id: int) -> Optional[T]:
        """
        Get a record by ID.
        
        Args:
            id: The record ID
            
        Returns:
            The record, or None if not found
        """
        sql = f"SELECT * FROM {self.table_name} WHERE id = %s"
        
        # Execute the query directly with a cursor to get column names
        with self.db.cursor() as cur:
            cur.execute(sql, [id])
            
            # Get column names from cursor description
            columns = [desc[0] for desc in cur.description]
            
            # Fetch the result
            result = cur.fetchone()
            
            if result:
                # Convert tuple to dictionary using column names
                dict_result = dict(zip(columns, result))
                return self.model_class(**dict_result)
            else:
                return None

    def get_all(self, limit: int = 100, offset: int = 0) -> List[T]:
        """
        Get all records with pagination.
        
        Args:
            limit: Maximum number of records to return
            offset: Number of records to skip
            
        Returns:
            List of records
        """
        sql = f"SELECT * FROM {self.table_name} ORDER BY id LIMIT %s OFFSET %s"
        
        # Execute the query directly with a cursor to get column names
        with self.db.cursor() as cur:
            cur.execute(sql, [limit, offset])
            
            # Get column names from cursor description
            columns = [desc[0] for desc in cur.description]
            
            # Fetch all results
            results = cur.fetchall()
            
            # Convert tuples to dictionaries using column names
            dict_results = []
            for result in results:
                dict_result = dict(zip(columns, result))
                dict_results.append(dict_result)
            
            # Create model instances from dictionaries
            return [self.model_class(**dict_result) for dict_result in dict_results]

    def update(self, id: int, data: Dict[str, Any]) -> Optional[T]:
        """
        Update a record.
        
        Args:
            id: The record ID
            data: The data to update
            
        Returns:
            The updated record, or None if not found
        """
        # Remove None values and id from the data
        update_data = {k: v for k, v in data.items() if v is not None and k != 'id'}
        if not update_data:
            return self.get_by_id(id)

        # Build the SQL query
        set_clause = ', '.join([f"{k} = %s" for k in update_data.keys()])
        values = list(update_data.values()) + [id]

        sql = f"""
        UPDATE {self.table_name}
        SET {set_clause}
        WHERE id = %s
        RETURNING *
        """

        # Execute the query directly with a cursor to get column names
        with self.db.cursor() as cur:
            cur.execute(sql, values)
            
            # Get column names from cursor description
            columns = [desc[0] for desc in cur.description]
            
            # Fetch the result
            result = cur.fetchone()
            
            if result:
                # Convert tuple to dictionary using column names
                dict_result = dict(zip(columns, result))
                return self.model_class(**dict_result)
            else:
                return None

    def delete(self, id: int) -> bool:
        """
        Delete a record.
        
        Args:
            id: The record ID
            
        Returns:
            True if the record was deleted, False otherwise
        """
        sql = f"DELETE FROM {self.table_name} WHERE id = %s RETURNING id"
        
        # Execute the query directly with a cursor
        with self.db.cursor() as cur:
            cur.execute(sql, [id])
            result = cur.fetchone()
            return result is not None

    def count(self) -> int:
        """
        Count all records.
        
        Returns:
            The number of records
        """
        sql = f"SELECT COUNT(*) as count FROM {self.table_name}"
        
        # Execute the query directly with a cursor
        with self.db.cursor() as cur:
            cur.execute(sql)
            result = cur.fetchone()
            
            if result:
                # Since we're using a tuple, get the first element
                return result[0]
            else:
                return 0