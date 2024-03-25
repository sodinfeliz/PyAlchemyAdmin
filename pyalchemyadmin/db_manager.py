from typing import Optional, List, Dict
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.declarative import declarative_base


class DBManager:

    # Supported dialects and engines
    AVAILABLE_DIALECTS = ["postgresql", "mysql", "oracle", "mssql", "sqlite"]
    AVAILABLE_ENGINES = {
        "postgresql": ["psycopg2", "pg8000", "asyncpg"],
        "mysql": ["mysqldb", "pymysql"],
        "oracle": ["cx_oracle"],
        "mssql": ["pyodbc", "pymssql"],
        "sqlite": [""]
    }

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
        if dialect not in self.AVAILABLE_DIALECTS:
            raise ValueError(f"Dialect '{dialect}' is not supported.")
        if engine and engine not in self.AVAILABLE_ENGINES.get(dialect, []):
            raise ValueError(f"Engine '{engine}' is not supported for dialect '{dialect}'.")

        try:
            self._database_url = self._construct_database_url(
                dialect=dialect, database=database, user=user,
                password=password, host=host, port=port, engine=engine
            )
            self._engine = create_engine(self._database_url, echo=echo)
            self._session = sessionmaker(bind=self._engine, autocommit=False, autoflush=False)
            self._base = declarative_base()
        except SQLAlchemyError as e:
            raise ValueError(f"An error occurred while creating the database session: {e}")
        
    @staticmethod
    def _construct_database_url(dialect, database, user, password, host, port, engine):
        """ Constructs the database URL based on the given parameters."""
        if dialect == "sqlite":
            return f"sqlite:///{database}" if database != ":memory:" else "sqlite://"
        else:
            engine_spec = f"{dialect}+{engine}" if engine else dialect
            return f"{engine_spec}://{user}:{password}@{host}:{port}/{database}"
        
    @property
    def base(self):
        return self._base
    
    @property
    def session(self):
        return self._session

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
