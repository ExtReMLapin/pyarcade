
from ..api.client import Client
from ..api import config
from typing import Optional, Any, List, Union
from enum import Enum
import re
import psycopg
from pygments.lexers import get_lexer_by_name
from pygments.token import string_to_tokentype

cypher_lexer = get_lexer_by_name("py2neo.cypher")

punctuation = string_to_tokentype("Token.Punctuation")
global_var = string_to_tokentype("Token.Name.Variable.Global")
string_liral = string_to_tokentype("Token.Literal.String")


class DatabaseDao:
    """
    Class to work with ArcadeDB databases
    """

    class IsolationLevel(Enum):
        """Isolation levels for transactions"""
        READ_COMMITTED = "READ_COMMITTED"
        REPEATABLE_READ = "REPEATABLE_READ"
        
    class Driver(Enum):
        HTTP="HTTP"
        PSYCOPG="PSYCOPG"

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
        driver:Driver=Driver.HTTP
        
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
        self.driver = driver

            
        if self.driver == self.Driver.PSYCOPG:
            port = self.client.port
            if client.port == 2480:
                print("Auto switching port to 5432 as we're using PSYCOPG driver")
                port = 5432
            self.connection = psycopg.connect(user=self.client.username, password=self.client.password,
                    host=self.client.host,
                    port=port,
                    dbname=self.database_name,
                    sslmode='disable'
                )
        else:
            self.connection = None
        if not DatabaseDao.exists(client, database_name):
            raise ValueError(f"Database {database_name} does not exist, call create()")
        
        
    cypher_var_regex = re.compile(r'\$([a-zA-Z_][a-zA-Z0-9_]*)')
    

    @staticmethod
    def cypher_formater(query: str, params: dict) -> str:

        skipped_params = {}
        tokens = list(cypher_lexer.get_tokens(query))
        i = 0
        len_tokens = len(tokens)
        while i < len_tokens-1:
            if tokens[i][0] == punctuation and tokens[i+1][0] == global_var:
                var_name = tokens[i+1][1]
                assert var_name in params, f"Variable {var_name} not found in the parameters"
                if isinstance(params[var_name], str) and '$' in params[var_name]:
                    skipped_params[var_name] = params[var_name]
                    i += 2
                    continue
                if isinstance(params[var_name], list):
                    skipped_params[var_name] = params[var_name]
                    i += 2
                    continue
                
                
                escaped_string = str(params[var_name]).replace('\\', '\\\\').replace('\'', '\\\'')
                tokens[i] = (string_liral, f"'{escaped_string}'")
                tokens.pop(i+1)
                len_tokens -= 1
    
            i += 1
        return "".join([x[1] for x in tokens]), skipped_params
    
        

    def query(
        self,
        language: str,
        command: str,
        limit: Optional[int] = None,
        params: Optional[Any] = None,
        serializer: Optional[str] = None,
        session_id: Optional[str] = None,
        is_command: Optional[bool] = False
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
        - is_command: If the query is a command (optional), you need this to run non-idempotent commands.

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
        
        if language == "cypher" and params:
            command, new_params = self.cypher_formater(command, params)
            params = new_params if len(new_params) > 0 else None
        payload = {
            "command": command,
            "language": language,
        }
        if limit is not None:
            payload["limit"] = limit
        if params is not None:
            payload["params"] = params
        if serializer is not None:
            assert self.driver == self.Driver.HTTP, "Serializer is only support with HTTP driver"
            payload["serializer"] = serializer
        extra_headers = {}
        if session_id is not None:
            assert self.driver == self.Driver.HTTP, "Session ID is only support with HTTP driver"
            extra_headers["arcadedb-session-id"] = session_id
        if self.driver == self.Driver.HTTP:
            req = self.client.post(f"{config.ARCADE_BASE_QUERY_ENDPOINT if is_command is False else config.ARCADE_BASE_COMMAND_ENDPOINT}/{self.database_name}", payload, extra_headers=extra_headers)
        else:
            with self.connection.cursor(row_factory=psycopg.rows.dict_row) as cursor:
                prefix = "" if language == "sql" else f"{{{language}}}"
                cursor.execute(query=prefix+command, params=params)
                return cursor.fetchall()
            
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
