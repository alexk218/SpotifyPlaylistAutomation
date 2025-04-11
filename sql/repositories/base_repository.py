import pyodbc
from typing import Any, List, Optional, Dict, Tuple, TypeVar, Generic, Type

from utils.logger import setup_logger

# Generic type for models
T = TypeVar('T')


class BaseRepository(Generic[T]):
    """
    Base repository class providing common database operations.
    """

    def __init__(self, connection: pyodbc.Connection, model_class: Type[T] = None):
        """
        Initialize a new BaseRepository.

        Args:
            connection: Active database connection
            model_class: The model class this repository handles
        """
        self.connection = connection
        self.model_class = model_class
        self.table_name = ""  # Override in subclasses
        self.id_column = ""  # Override in subclasses
        self.db_logger = setup_logger('repository', 'sql/repository.log')

    def execute_query(self, query: str, params: Optional[Tuple] = None) -> pyodbc.Cursor:
        """
        Execute a SQL query with optional parameters.

        Args:
            query: SQL query to execute
            params: Parameters for the query

        Returns:
            Cursor object for accessing results

        Raises:
            Exception: If query execution fails
        """
        try:
            cursor = self.connection.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return cursor
        except Exception as e:
            self.db_logger.error(f"Error executing query: {e}")
            self.db_logger.error(f"Query: {query}")
            if params:
                self.db_logger.error(f"Params: {params}")
            raise

    def fetch_all(self, query: str, params: Optional[Tuple] = None) -> List[Any]:
        """
        Execute a query and fetch all results.

        Args:
            query: SQL query to execute
            params: Parameters for the query

        Returns:
            List of results
        """
        cursor = self.execute_query(query, params)
        try:
            results = cursor.fetchall()
            return results
        finally:
            cursor.close()

    def fetch_one(self, query: str, params: Optional[Tuple] = None) -> Optional[Any]:
        """
        Execute a query and fetch a single result.

        Args:
            query: SQL query to execute
            params: Parameters for the query

        Returns:
            Single result or None if no results
        """
        cursor = self.execute_query(query, params)
        try:
            result = cursor.fetchone()
            return result
        finally:
            cursor.close()

    def execute_non_query(self, query: str, params: Optional[Tuple] = None) -> int:
        """
        Execute a non-query SQL statement (INSERT, UPDATE, DELETE).

        Args:
            query: SQL statement to execute
            params: Parameters for the statement

        Returns:
            Number of rows affected
        """
        cursor = self.execute_query(query, params)
        try:
            row_count = cursor.rowcount
            return row_count
        finally:
            cursor.close()

    def get_by_id(self, id_value: Any) -> Optional[T]:
        """
        Get an entity by its ID.

        Args:
            id_value: The ID value to look up

        Returns:
            Entity or None if not found
        """
        if not self.table_name or not self.id_column:
            raise NotImplementedError("table_name and id_column must be set in subclass")

        query = f"SELECT * FROM {self.table_name} WHERE {self.id_column} = ?"
        result = self.fetch_one(query, (id_value,))

        if result:
            return self._map_to_model(result)
        return None

    def get_all(self) -> List[T]:
        """
        Get all entities from the table.

        Returns:
            List of entities
        """
        if not self.table_name:
            raise NotImplementedError("table_name must be set in subclass")

        query = f"SELECT * FROM {self.table_name}"
        results = self.fetch_all(query)

        return [self._map_to_model(row) for row in results]

    def delete_all(self) -> int:
        """
        Delete all records from the table.

        Returns:
            Number of rows deleted
        """
        if not self.table_name:
            raise NotImplementedError("table_name must be set in subclass")

        query = f"DELETE FROM {self.table_name}"
        return self.execute_non_query(query)

    def delete_by_id(self, id_value: Any) -> bool:
        """
        Delete an entity by its ID.

        Args:
            id_value: The ID value to delete

        Returns:
            True if the entity was deleted, False if it wasn't found
        """
        if not self.table_name or not self.id_column:
            raise NotImplementedError("table_name and id_column must be set in subclass")

        query = f"DELETE FROM {self.table_name} WHERE {self.id_column} = ?"
        rows_affected = self.execute_non_query(query, (id_value,))

        return rows_affected > 0

    def _map_to_model(self, row: Any) -> T:
        """
        Map a database row to a model object.

        Args:
            row: Database row

        Returns:
            Model object

        Note:
            This method should be overridden in subclasses to provide specific mapping logic.
        """
        raise NotImplementedError("_map_to_model must be implemented in subclasses")
