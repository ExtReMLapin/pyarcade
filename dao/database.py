import logging
import requests

from pyarcade.api.client import Client
from pyarcade.api import config

from enum import Enum

class DatabaseDao:

    class IsolationLevel(Enum):
        READ_COMMITTED = "READ_COMMITTED"
        REPEATABLE_READ = "REPEATABLE_READ"



    def __init__(
        self,
        client: Client,
        database_name: str,
    ):
        self.client = client
        self.database_name = database_name
        if not DatabaseDao.exists(client, database_name):
            raise ValueError(f"Database {database_name} does not exist, call create()")



    def query(self, language, command, limit=None, params=None, serializer=None, session_id=None):
        language = language.lower()
        if not language in config.AVAILABLE_LANGUAGES:
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
        req = self.client.post(f"{config.ARCADE_BASE_QUERY_ENDPOINT}/{self.database_name}" , payload, extra_headers=extra_headers)
        return req
    

    def begin_transaction(self,isolation_level: IsolationLevel = IsolationLevel.READ_COMMITTED):
        headers = self.client.post(f"{config.ARCADE_BASE_TRANSACTION_BEGIN_ENDPOINT}/{self.database_name}", {"isolationLevel":isolation_level.value}, return_headers=True)
        return headers["arcadedb-session-id"]

    def commit_transaction(self, session_id):
        self.client.post(f"{config.ARCADE_BASE_TRANSACTION_COMMIT_ENDPOINT}/{self.database_name}", {}, extra_headers={"arcadedb-session-id":session_id})

    def rollback_transaction(self, session_id):
        self.client.post(f"{config.ARCADE_BASE_TRANSACTION_ROLLBACK_ENDPOINT}/{self.database_name}", {}, extra_headers={"arcadedb-session-id":session_id})

    @staticmethod
    def exists(client, name: str) -> bool:
        response = client.get(f"{config.ARCADE_BASE_EXISTS_ENDPOINT}/{name}")
        return response
        

        


    @staticmethod
    def create(client, name: str):
        
        if DatabaseDao.exists(client, name):
            logging.info(f"Database {name} already exists")
            raise ValueError(f"Database {name} already exists")

        ret =  client.post(config.ARCADE_BASE_SERVER_ENDPOINT,  {"command":f"create database {name}"})
        if ret == "ok":
            return DatabaseDao (client, name)
        else:
            raise ValueError(f"Could not create database {name} : " + ret)



    @staticmethod
    def delete(client, name: str):
        if not DatabaseDao.exists(client, name):
            logging.info(f"Database {name} does not exist")
            raise ValueError(f"Database {name} does not exist")
        ret =  client.post(config.ARCADE_BASE_SERVER_ENDPOINT,  {"command":f"drop database {name}"})
        if ret == "ok":
            return True
        else:
            raise ValueError(f"Could not drop database {name} : " + ret)


    @staticmethod
    def list_databases(client):
        return client.get(config.ARCADE_BASE_LIST_DB_ENDPOINT)

    
    def __repr__(self) -> str:
        return f"<DatabaseDao database_name={self.database_name}> @ {self.client}"
    
