from .db_manager import DBManager


class _DialectDBManager(DBManager):
    dialect = ""
    default_port = None

    @classmethod
    def create_database_session(cls, *, database: str, user: str, password: str, 
                                host: str, port: int = None, engine: str = "", echo: bool=False):
        if port is None:
            port = cls.default_port

        super().create_database_session(
            database=database, user=user, password=password, host=host, port=port,
            dialect=cls.dialect, engine=engine, echo=echo
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
    
    @classmethod
    def create_database_session(cls, *, database: str, echo: bool=False):
        super().create_database_session(
            database=database, user="", password="", host="", port=None,
            dialect=cls.dialect, engine="", echo=echo
        )


__all__ = ["PostgreDBManager", "MySQLDBManager", "OracleDBManager", "MicrosoftSQLServerDBManager", "SQLiteDBManager"]

