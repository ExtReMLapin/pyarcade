import requests

from pyarcade.api.client import Client
from pyarcade.api import config
from typing import Optional, Any, List, Union
from enum import Enum

class DatabaseDao:
    """
    Class to work with ArcadeDB databases
    """

    class IsolationLevel(Enum):
        """Isolation levels for transactions"""
        READ_COMMITTED = "READ_COMMITTED"
        REPEATABLE_READ = "REPEATABLE_READ"

    @staticmethod
    def exists(client, name: str) -> bool:
        """
        Check if a database exists.

        Parameters:
        - client (Client): The ArcadeDB client.
        - name (str): The name of the database.

        Returns:
        bool: True if the database exists, False otherwise.
        """
        response = client.get(f"{config.ARCADE_BASE_EXISTS_ENDPOINT}/{name}")
        return response

    @staticmethod
    def create(client: Client, name: str) -> 'DatabaseDao':
        """
        Create a new database.

        Parameters:
        - client (Client): The ArcadeDB client.
        - name (str): The name of the new database.

        Returns:
        DatabaseDao: An instance of DatabaseDao for the created database.

        Raises:
        - ValueError: If the database already exists.
        - ValueError: If the creation of the database fails.
        """
        if DatabaseDao.exists(client, name):
            raise ValueError(f"Database {name} already exists")

        ret = client.post(config.ARCADE_BASE_SERVER_ENDPOINT, {"command": f"create database {name}"})
        if ret == "ok":
            return DatabaseDao(client, name)
        else:
            raise ValueError(f"Could not create database {name}: {ret}")

    @staticmethod
    def delete(client: Client, name: str) -> bool:
        """
        Delete a database.

        Parameters:
        - client (Client): The ArcadeDB client.
        - name (str): The name of the database to be deleted.

        Returns:
        bool: True if the database is successfully deleted.

        Raises:
        - ValueError: If the database does not exist.
        - ValueError: If the deletion of the database fails.
        """
        if not DatabaseDao.exists(client, name):
            raise ValueError(f"Database {name} does not exist")
        ret = client.post(config.ARCADE_BASE_SERVER_ENDPOINT, {"command": f"drop database {name}"})
        if ret == "ok":
            return True
        else:
            raise ValueError(f"Could not drop database {name}: {ret}")

    @staticmethod
    def list_databases(client: Client) -> List:
        """
        List all databases.

        Parameters:
        - client (Client): The ArcadeDB client.

        Returns:
        str: The list of databases.
        """
        return client.get(config.ARCADE_BASE_LIST_DB_ENDPOINT)

    def __init__(
        self,
        client: Client,
        database_name: str,
    ):
        """
        Initialize a DatabaseDao instance.

        Parameters:
        - client (Client): The ArcadeDB client.
        - database_name (str): The name of the database.

        Raises:
        - ValueError: If the database does not exist. Call create() to create a new database.
        """
        self.client = client
        self.database_name = database_name
        if not DatabaseDao.exists(client, database_name):
            raise ValueError(f"Database {database_name} does not exist, call create()")

    def query(
        self,
        language: str,
        command: str,
        limit: Optional[int] = None,
        params: Optional[Any] = None,
        serializer: Optional[str] = None,
        session_id: Optional[str] = None,
        is_idempotent: Optional[bool] = False
    ) -> Union[str, List, dict]:
        """
        Execute a query on the database.

        Parameters:
        - language (str): The query language.
        - command (str): The query command.
        - limit (int): The limit on the number of results (optional).
        - params: The parameters for the query (optional).
        - serializer (str): The serializer for the query results (optional).
        - session_id: The session ID for the query (optional).
        - is_idempotent: Read-only mode (optional)

        Returns:
        str: The result of the query.
        """
        language = language.lower()
        if language not in config.AVAILABLE_LANGUAGES:
            raise ValueError(f"Language {language} not supported")
        if limit is not None:
            assert isinstance(limit, int), "Limit must be an integer"
        serializer = serializer.lower() if serializer else serializer
        assert serializer in {None, "graph", "record"}, "Serializer must be None, 'graph' or 'record'"

        payload = {
            "command": command,
            "language": language,
        }
        if limit is not None:
            payload["limit"] = limit
        if params is not None:
            payload["params"] = params
        if serializer is not None:
            payload["serializer"] = serializer
        extra_headers = {}
        if session_id is not None:
            extra_headers["arcadedb-session-id"] = session_id
        req = self.client.post(f"{config.ARCADE_BASE_QUERY_ENDPOINT if is_idempotent is True else config.ARCADE_BASE_COMMAND_ENDPOINT}/{self.database_name}", payload, extra_headers=extra_headers)
        return req

    def begin_transaction(self, isolation_level: IsolationLevel = IsolationLevel.READ_COMMITTED) -> str:

        """
        Begin a new transaction.

        Parameters:
        - isolation_level (IsolationLevel): The isolation level for the transaction (default: READ_COMMITTED).

        Returns:
        str: The session ID for the new transaction.
        """
        headers = self.client.post(f"{config.ARCADE_BASE_TRANSACTION_BEGIN_ENDPOINT}/{self.database_name}", {"isolationLevel": isolation_level.value}, return_headers=True)
        return headers["arcadedb-session-id"]

    def commit_transaction(self, session_id) -> None:
        """
        Commit a transaction.

        Parameters:
        - session_id: The session ID of the transaction to be committed.
        """
        self.client.post(f"{config.ARCADE_BASE_TRANSACTION_COMMIT_ENDPOINT}/{self.database_name}", {}, extra_headers={"arcadedb-session-id": session_id})

    def rollback_transaction(self, session_id) -> None:
        """
        Rollback a transaction.

        Parameters:
        - session_id: The session ID of the transaction to be rolled back.
        """
        self.client.post(f"{config.ARCADE_BASE_TRANSACTION_ROLLBACK_ENDPOINT}/{self.database_name}", {}, extra_headers={"arcadedb-session-id": session_id})

    def __repr__(self) -> str:
        """
        Return a string representation of the DatabaseDao instance.

        Returns:
        str: A string representation of the form "<DatabaseDao database_name={self.database_name}> @ {self.client}".
        """
        return f"<DatabaseDao database_name={self.database_name}> @ {self.client}"
