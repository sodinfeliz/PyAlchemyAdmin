from abc import ABC, abstractmethod
from typing import Dict, List, Optional

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from sqlalchemy.orm import declarative_base, sessionmaker


class DBManager(ABC):

    dialect = ""

    # Supported dialects and engines
    AVAILABLE_DIALECTS = ["postgresql", "mysql", "oracle", "mssql", "sqlite"]
    AVAILABLE_ENGINES = {
        "postgresql": ["psycopg2", "pg8000", "asyncpg"],
        "mysql": ["mysqldb", "pymysql"],
        "oracle": ["cx_oracle"],
        "mssql": ["pyodbc", "pymssql"],
        "sqlite": [""],
    }

    def __init__(
        self,
        *,
        database: str,
        user: str,
        password: str,
        host: str,
        port: int,
        dialect: str,
        engine: str = "",
        echo: bool = False,
    ):
        """
        Initializes the database engine and session.

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
            raise ValueError(
                f"Engine '{engine}' is not supported for dialect '{dialect}'."
            )

        try:
            self._database_url = self._construct_database_url(
                dialect=dialect,
                database=database,
                user=user,
                password=password,
                host=host,
                port=port,
                engine=engine,
            )
            self._engine = create_engine(self._database_url, echo=echo)
            self._session = sessionmaker(
                bind=self._engine, autocommit=False, autoflush=False
            )
            self._base = declarative_base()
        except SQLAlchemyError as e:
            raise ValueError(
                f"An error occurred while creating the database session: {e}"
            )

    @staticmethod
    def _construct_database_url(dialect, database, user, password, host, port, engine):
        """Constructs the database URL based on the given parameters."""
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

    @abstractmethod
    def lock_table_command(self, table_name: str) -> text:
        """Returns the SQL command to lock a table."""
        pass

    def _has_column(self, table, column_name: str) -> bool:
        """Check if a column exists in a table."""
        if not issubclass(table, self._base):
            raise ValueError("Invalid table object.")
        inspector = inspect(table)
        return column_name in inspector.columns.keys()

    def _validate_column_existence(self, table, *args, **kwargs) -> None:
        """Validate that all columns exist in the table."""
        columns_to_check = set(args) | set(kwargs.keys())
        for column in columns_to_check:
            if not self._has_column(table, column):
                raise ValueError(
                    f"Column '{column}' does not exist in table '{table.__tablename__}'."
                )

    def create_all_tables(self) -> None:
        """Create all tables in the database using the engine."""
        if self._engine is None:
            raise ValueError("Engine is not initialized.")
        try:
            self._base.metadata.create_all(self._engine)
        except OperationalError as e:
            raise ValueError(f"An error occurred while creating tables: {e}")

    ### Native SQL Operations ###

    def execute(
        self, query_string, params: Optional[Dict] = None, fetch: bool = False
    ) -> None:
        """
        Executes an SQL command with optional parameters and can return results.

        This method is versatile, allowing for execution of SQL statements including SELECT,
        INSERT, UPDATE, DELETE, or any other SQL command. For SELECT statements and others that
        return a result, this method fetches and returns the result set.

        Args:
            query_string (str): SQL command to execute.
            params (Dict[str, Any], optional): Parameters for the SQL command, preventing SQL injection.
            fetch (bool, optional): If True, returns the result set of the query. Defaults to False.

        Returns:
            Any: Result set for retrieval operations if `fetch` is True; otherwise, None.

        Example:
            To retrieve records from the `users` table:

            >>> query_string = "SELECT * FROM users WHERE email = :email"
            >>> params = {'email': 'john.doe@example.com'}
            >>> result = execute(query_string, params)
            >>> for row in result:
            >>>     print(row)

            To insert a new record into the `users` table:

            >>> query_string = "INSERT INTO users (name, email) VALUES (:name, :email)"
            >>> params = {'name': 'Jane Doe', 'email': 'jane.doe@example.com'}
            >>> execute(query_string, params)

            To delete specific images based on their UUIDs from the `second_normalize_form` table:

            >>> query_string = "DELETE FROM second_normalize_form WHERE export_uuid = :export_uuid AND image_uuid IN :image_uuids"
            >>> params = {'export_uuid': request_schema.export_uuid, 'image_uuids': tuple(request_schema.image_uuid)}
            >>> execute(query_string, params)
        """
        with self._session() as session:
            result = session.execute(statement=text(query_string), params=params)
            if fetch:
                return result.fetchall()
            else:
                # For non-fetch queries, commit the transaction
                session.commit()
                return None

    ### CRUD Operations ###

    def create(self, table, **kwargs) -> None:
        """
        Creates a new record in the specified table with the given values.

        Args:
            table (Base): The table class into which the record will be inserted.
            **kwargs: Key-value pairs corresponding to the table column names and the values to insert into the new record.

        Raises:
            ValueError: If an error occurs during the creation of the record or
                        if a specified column in kwargs does not exist in the table.

        Example:
            Assuming we have a table class `Employee` derived from Base, with columns 'id', 'name', 'department':

            >>> employee_data = {'name': 'John Smith', 'department': 'Engineering'}
            >>> create(Employee, **employee_data)
            This would create a new record in the Employee table with the name 'John Smith' and department 'Engineering'.

        Note: This method commits the transaction, ensuring that changes are saved to the database.
        """
        self._validate_column_existence(table, **kwargs)
        try:
            with self._session() as session:
                new_record = table(**kwargs)
                session.add(new_record)
                session.commit()
        except SQLAlchemyError as e:
            raise ValueError(f"An error occurred while creating a record: {e}")

    def bulk_create(self, table, records: List[Dict]) -> None:
        """
        Creates multiple new records in the specified table with the given values.

        Args:
            table (Base): The table class into which the records will be inserted.
            records (List[dict]): A list of dictionaries, where each dictionary represents a record to be inserted.
                The keys in each dictionary should correspond to the table column names, and the values should be
                the values to insert into the respective columns for each record.

        Raises:
            ValueError: If an error occurs during the creation of the records or if a specified column in any of the
                records does not exist in the table.

        Example:
            Assuming we have a table class `Employee` derived from Base, with columns 'id', 'name', 'department':
            >>> employee_data = [
            ...     {'name': 'John Smith', 'department': 'Engineering'},
            ...     {'name': 'Jane Doe', 'department': 'Sales'}
            ... ]
            >>> bulk_create(Employee, employee_data)
            This would create new records in the Employee table for each dictionary in the `employee_data` list.

        Note: This method commits the transaction, ensuring that changes are saved to the database.
        """
        if not records:
            return
        for record_data in records:
            self._validate_column_existence(table, **record_data)

        try:
            with self._session() as session:
                session.bulk_insert_mappings(table, records)
                session.commit()
        except SQLAlchemyError as e:
            raise ValueError(f"An error occurred while creating records: {e}")

    def retrieve(
        self,
        table,
        complex_conditions: Optional[List] = None,
        return_columns: List = None,
        fetch_mode: str = "all",
        **filters,
    ):
        """
        Retrieves records from a table, supporting both simple and complex filters, with optional column selection and fetch mode.

        Args:
            table (Base): Table class to retrieve records from.
            complex_conditions (List, optional): Advanced SQLAlchemy filter conditions.
            return_columns (List[str], optional): Columns to return. Returns all if None.
            fetch_mode (str, optional): 'one' for a single record, 'all' for all matches. Defaults to 'all'.
            **filters: Equality filters ({column_name: value}).

        Returns:
            Depending on `fetch_mode`, either a single model instance ('one') or a list ('all') of instances.
            Returns specified columns, or all if `return_columns` is None.

        Raises:
            ValueError: For retrieval errors or if `fetch_mode` is invalid.

        Example:
            Assuming we have a table class `Article` derived from Base, with columns 'id', 'title', 'author':

            >>> complex_conditions = [Article.title.like('%Python%'), Article.published_date > '2022-01-01']
            >>> articles = retrieve(Article, columns=['id', 'title'], fetch_mode='all', complex_conditions=complex_conditions, author='John Doe')
            This will return a list of `Article` instances with only the 'id' and 'title' columns for articles about Python
            published after January 1, 2022, and written by John Doe.

            To include articles where the title is in a specific list of titles:

            >>> some_titles = ['Python Guide', 'SQLAlchemy Tips', 'Advanced Python']
            >>> condition = Article.title.in_(some_titles)
            >>> articles = retrieve(Article, complex_conditions=[condition])
            This will return a list of `Article` instances for articles whose titles are in the specified list.
        """
        if fetch_mode not in ["all", "one"]:
            raise ValueError("Invalid fetch mode. Use 'all' or 'one'.")

        return_columns = return_columns or []
        self._validate_column_existence(table, *return_columns, **filters)

        try:
            with self._session() as session:
                query = session.query(table).filter_by(**filters)
                if complex_conditions:
                    for condition in complex_conditions:
                        query = query.filter(condition)
                if return_columns:
                    query = query.with_entities(
                        *[getattr(table, col) for col in return_columns]
                    )
                results = query.all() if fetch_mode == "all" else query.first()
        except SQLAlchemyError as e:
            raise ValueError(f"An error occurred while retrieving records: {e}")
        return results

    def update(
        self,
        table,
        update_values: Dict,
        complex_conditions: Optional[List] = None,
        **filters,
    ) -> None:
        """
        Updates table records based on provided filters, conditions, and new values.

        Args:
            table (Base): Table class for the records to update.
            update_values (Dict): {column_name: new_value} pairs for the update.
            complex_conditions (List, optional): Advanced SQLAlchemy filter conditions.
            **filters: Simple equality filters ({column_name: value}).

        Raises:
            ValueError: If no update values are provided or an update error occurs.

        Example:
            >>> update_values = {'name': 'Jane Doe', 'email': 'jane@example.com'}
            >>> complex_conditions = [User.age > 30]
            >>> update(User, update_values=update_values, complex_conditions=complex_conditions, active=True)
            This would update the 'name' and 'email' fields of all active User records where 'age' is greater than 30.
        """
        if not update_values:
            raise ValueError("No update values provided.")

        self._validate_column_existence(table, *update_values.keys(), **filters)

        try:
            with self._session() as session:
                query = session.query(table).filter_by(**filters)
                if complex_conditions:
                    for condition in complex_conditions:
                        query = query.filter(condition)

                # Lock the selected rows for update
                query = query.with_for_update()

                query.update(update_values, synchronize_session="fetch")
                session.commit()
        except SQLAlchemyError as e:
            raise ValueError(f"An error occurred while updating records: {e}")

    def delete(
        self,
        table,
        complex_conditions: Optional[List] = None,
        return_columns: Optional[List[str]] = None,
        error_when_empty: bool = False,
        **filters,
    ) -> None:
        """
        Deletes table records based on filters and conditions, optionally returns specific column values.

        Args:
            table (Base): Table class for the records to delete.
            complex_conditions (List, optional): Advanced SQLAlchemy filter conditions.
            return_columns (List[str], optional): Columns to return from deleted records.
            error_when_empty (bool, optional): If True, raises error if no records deleted.
            **filters: Simple equality filters ({column_name: value}).

        Returns:
            Optional[List]: Values of `return_columns` from deleted records, if specified.

        Raises:
            ValueError: If `error_when_empty` is True and no records are deleted or on deletion error.

        Example:
            >>> complex_conditions = [Comment.date < '2022-01-01']
            >>> delete(Comment, complex_conditions=complex_conditions, error_when_empty=True, user_id=123)
            This would delete all Comment records associated with 'user_id' 123 that were created before January 1, 2022,
            and raise a ValueError if no records were deleted.
        """
        return_columns = return_columns or []
        self._validate_column_existence(table, *return_columns, **filters)

        deleted_records = None
        try:
            with self._session() as session:
                query = session.query(table).filter_by(**filters)
                if complex_conditions:
                    for condition in complex_conditions:
                        query = query.filter(condition)
                if return_columns:
                    deleted_records = query.with_entities(
                        *[getattr(table, col) for col in return_columns]
                    ).all()

                deleted_count = query.delete()
                session.commit()

                if error_when_empty and deleted_count == 0:
                    raise ValueError("No records were deleted.")
        except SQLAlchemyError as e:
            raise ValueError(f"An error occurred while deleting records: {e}")

        if return_columns and deleted_records:
            return deleted_records
        return None

    ### Additional Operations ###

    def exists(
        self, table, complex_conditions: Optional[List] = None, **filters
    ) -> bool:
        """
        Checks if any records in the table match the given simple and complex conditions.

        Args:
            table (Base): The table class to check for existing records.
            complex_conditions (List, optional): Advanced SQLAlchemy filter conditions.
            **filters: Simple equality filters ({column_name: value}).

        Returns:
            bool: True if at least one record matches the conditions, False otherwise.

        Example:
            >>> exists(User, [User.age > 18], name='John Doe')
            Checks if there's at least one User named 'John Doe' over 18.
        """
        self._validate_column_existence(table, **filters)

        with self._session() as session:
            query = session.query(table).filter_by(**filters)
            if complex_conditions:
                for condition in complex_conditions:
                    query = query.filter(condition)
            return query.first() is not None
