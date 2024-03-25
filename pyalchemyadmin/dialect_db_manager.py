from .db_manager import DBManager


class _DialectDBManager(DBManager):
    dialect = ""
    default_port = None

    def __init__(self, *, database: str, user: str, password: str, 
                 host: str, port: int = None, engine: str = "", echo: bool=False):
        """Initializes the database engine and session.
        
        Args:
            database (str): The name of the database.
            user (str): The username for the database.
            password (str): The password for the database.
            host (str): The host of the database.
            port (int): The port of the database.
            engine (str, optional): The engine of the database. Defaults to "".
            echo (bool, optional): If True, the engine will log all the SQL it executes. Defaults to False.
        """
        if port is None:
            port = self.default_port

        super().__init__(
            database=database, 
            user=user, 
            password=password, 
            host=host, 
            port=port,
            dialect=self.dialect, 
            engine=engine, 
            echo=echo
        )


class PostgreDBManager(_DialectDBManager):
    dialect = "postgresql"
    default_port = 5432 


class MySQLDBManager(_DialectDBManager):
    dialect = "mysql"
    default_port = 3306


class OracleDBManager(_DialectDBManager):
    dialect = "oracle"
    default_port = 1521


class MicrosoftSQLServerDBManager(_DialectDBManager):
    dialect = "mssql"
    default_port = 1433


class SQLiteDBManager(_DialectDBManager):
    dialect = "sqlite"

    def __init__(self, *, database: str, echo: bool=False):
        """Initializes the database engine and session.
        
        Args:
            database (str): The name of the database.
            echo (bool, optional): If True, the engine will log all the SQL it executes. Defaults to False.
        """
        super().__init__(
            database=database, user="", password="", host="", port=None,
            dialect=SQLiteDBManager.dialect, engine="", echo=echo
        )


__all__ = ["PostgreDBManager", "MySQLDBManager", "OracleDBManager", "MicrosoftSQLServerDBManager", "SQLiteDBManager"]

