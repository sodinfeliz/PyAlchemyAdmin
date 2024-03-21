from .db_manager import DBManager


class PostgreDBManager(DBManager):
    
    @classmethod
    def create_database_session(cls, *, database: str, user: str, password: str, 
                                host: str, port: int, engine: str = "", echo: bool=False):
        super().create_database_session(
            database=database, user=user, password=password, host=host, port=port,
            dialect='postgresql', engine=engine, echo=echo
        )                


class MySQLDBManager(DBManager):
    
    @classmethod
    def create_database_session(cls, *, database: str, user: str, password: str, 
                                host: str, port: int, engine: str = "", echo: bool=False):
        super().create_database_session(
            database=database, user=user, password=password, host=host, port=port,
            dialect='mysql', engine=engine, echo=echo
        )


class OracleDBManager(DBManager):
    
    @classmethod
    def create_database_session(cls, *, database: str, user: str, password: str, 
                                host: str, port: int, engine: str = "", echo: bool=False):
        super().create_database_session(
            database=database, user=user, password=password, host=host, port=port,
            dialect='oracle', engine=engine, echo=echo
        )


class MicrosoftSQLServerDBManager(DBManager):
    
    @classmethod
    def create_database_session(cls, *, database: str, user: str, password: str, 
                                host: str, port: int, engine: str = "", echo: bool=False):
        super().create_database_session(
            database=database, user=user, password=password, host=host, port=port,
            dialect='mssql', engine=engine, echo=echo
        )
