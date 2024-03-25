from typing import Optional, List, Dict
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.declarative import declarative_base

AVAILABLE_DIALECTS = [
    "postgresql", 
    "mysql", 
    "oracle", 
    "mssql", 
    "sqlite"
]

AVAILABLE_ENGINES = {
    "postgresql": ["psycopg2", "pg8000", "asyncpg"],
    "mysql": ["mysqldb", "pymysql"],
    "oracle": ["cx_oracle"],
    "mssql": ["pyodbc", "pymssql"],
    "sqlite": [""]
}



class DBManager:

    def __init__(self, *, database: str, user: str, password: str, 
                 host: str, port: int, dialect: str, engine: str = "", echo: bool=False):
        """Initializes the database engine and session.
        
        Args:
            database (str): The name of the database.
            user (str): The username for the database.
            password (str): The password for the database.
            host (str): The host of the database.
            port (int): The port of the database.
            dialect (str): The dialect of the database.
            engine (str, optional): The engine of the database. Defaults to "".
            echo (bool, optional): If True, the engine will log all the SQL it executes. Defaults to False.
        """
        DBManager.__check_dialect(dialect)
        DBManager.__check_engine(dialect, engine)
        
        # SQLite does not require a username, password, host, and port
        if dialect == "sqlite":  # TODO: Test this
            if database == ":memory:":
                self._database_url = "sqlite://"
            else:
                # Handling both relative (three slashes) and absolute (four slashes) paths
                # An absolute path in SQLite starts with a slash, which leads to four slashes in total
                prefix = "sqlite:///"
                self._database_url = f"{prefix}{database}"
        else:
            engine_spec = dialect if engine == "" else f"{dialect}+{engine}"
            self._database_url = f"{engine_spec}://{user}:{password}@{host}:{port}/{database}"
        
        try:
            self._engine = create_engine(self._database_url, echo=echo)
            self._session = sessionmaker(bind=self._engine, autocommit=False, autoflush=False)
            self._base = declarative_base()
        except SQLAlchemyError as e:
            raise ValueError(f"An error occurred while creating the database session: {e}")
        
    @property
    def base(self):
        return self._base
    
    @property
    def session(self):
        return self._session

    @staticmethod
    def __check_dialect(dialect: str) -> None:
        """Checks if the dialect is supported."""
        if dialect not in AVAILABLE_DIALECTS:
            raise ValueError(f"Dialect '{dialect}' is not supported.")
        
    @staticmethod
    def __check_engine(dialect: str, engine: str) -> None:
        """Checks if the engine is supported."""
        if engine and engine not in AVAILABLE_ENGINES[dialect]:
            raise ValueError(f"Engine '{engine}' is not supported for dialect '{dialect}'.")

    def create_all(self) -> None:
        """Create all tables in the database using the engine."""
        if self._engine is None:
            raise ValueError("Engine is not initialized.")
        self._base.metadata.create_all(self._engine)

    def execute(self, query_string, params: Optional[Dict]=None) -> None:
        """Executes a given SQL command.
        
        Args:
            query_string (str): The SQL command to execute.
            params (Dict, optional): The parameters to pass to the SQL command. Defaults to None.
        """
        with self._session() as session:
            session.execute(
                statement=text(query_string), 
                params=params
            )

    def retrieve_one(self, query_string, params: Optional[Dict]=None) -> tuple | None:
        """Retrieves a single record matching the query.
        
        Args:
            query_string (str): The SQL command to execute.
            params (Dict, optional): The parameters to pass to the SQL command. Defaults to None.
        """
        with self._session() as session:
            result = session.execute(
                statement=text(query_string), 
                params=params
            ).fetchone()
        return result
    
    def retrieve_all(self, query_string, params: Optional[Dict]=None) -> List[tuple] | None:
        """Retrieves all records matching the query.
        
        Args:
            query_string (str): The SQL command to execute.
            params (Dict, optional): The parameters to pass to the SQL command. Defaults to None.
        """
        with self._session() as session:
            result = session.execute(
                statement=text(query_string), 
                params=params
            ).fetchall()
        return result
