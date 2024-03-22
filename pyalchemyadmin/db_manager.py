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

    DATABASE_URL: str = ""
    ENGINE = None
    SESSION = None
    BASE = declarative_base()

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

    @classmethod
    def create_database_session(cls, *, database: str, user: str, password: str, 
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
        cls.__check_dialect(dialect)
        cls.__check_engine(dialect, engine)

        engine_spec = dialect if engine == "" else f"{dialect}+{engine}"

        # SQLite does not require a username, password, host, and port
        if dialect == "sqlite":  # TODO: Test this
            if database == ":memory:":
                cls.DATABASE_URL = "sqlite://"
            else:
                # Handling both relative (three slashes) and absolute (four slashes) paths
                # An absolute path in SQLite starts with a slash, which leads to four slashes in total
                prefix = "sqlite:///"
                cls.DATABASE_URL = f"{prefix}{database}"
        else:
            cls.DATABASE_URL = f"{engine_spec}://{user}:{password}@{host}:{port}/{database}"
        
        try:
            cls.ENGINE = create_engine(cls.DATABASE_URL, echo=echo)
            cls.SESSION = sessionmaker(bind=cls.ENGINE, autocommit=False, autoflush=False)
        except SQLAlchemyError as e:
            raise ValueError(f"An error occurred while creating the database session: {e}")

    @classmethod
    def create_all(cls):
        """Create all tables in the database using the engine."""
        if cls.ENGINE is None:
            raise ValueError("Engine is not initialized. Please call create_database_session first.")
        cls.BASE.metadata.create_all(cls.ENGINE)

    @classmethod
    def execute(cls, query_string, params: Optional[Dict]=None) -> None:
        """Executes a given SQL command.
        
        Args:
            query_string (str): The SQL command to execute.
            params (Dict, optional): The parameters to pass to the SQL command. Defaults to None.
        """
        with cls.SESSION() as session:
            session.execute(
                statement=text(query_string), 
                params=params
            )

    @classmethod
    def retrieve_one(cls, query_string, params: Optional[Dict]=None) -> tuple | None:
        """Retrieves a single record matching the query.
        
        Args:
            query_string (str): The SQL command to execute.
            params (Dict, optional): The parameters to pass to the SQL command. Defaults to None.
        """
        with cls.SESSION() as session:
            result = session.execute(
                statement=text(query_string), 
                params=params
            ).fetchone()
        return result
    
    @classmethod
    def retrieve_all(cls, query_string, params: Optional[Dict]=None) -> List[tuple] | None:
        """Retrieves all records matching the query.
        
        Args:
            query_string (str): The SQL command to execute.
            params (Dict, optional): The parameters to pass to the SQL command. Defaults to None.
        """
        with cls.SESSION() as session:
            result = session.execute(
                statement=text(query_string), 
                params=params
            ).fetchall()
        return result
