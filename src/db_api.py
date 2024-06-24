from sqlalchemy import create_engine, text
from src.settings import HOST, USER, PASSWORD
from sqlalchemy.orm import sessionmaker


class Postgresql_DB_API:
    def __init__(
        self,
        host: str = HOST,
        user: str = USER,
        password: str = PASSWORD,
    ) -> None:
        """
        Initializes the Postgresql_DB_API class with connection parameters.

        Description:
        Initializes the class with the necessary connection parameters and sets
        the initial attributes to None.

        Parameters:
        host (str): Hostname of the PostgreSQL database.
        user (str): Username for the PostgreSQL database.
        password (str): Password for the PostgreSQL database.
        """
        self.host = host
        self.user = user
        self.password = password
        self.dbname = None
        self.engine = None
        self.Session = None

    def connect(self):
        """
        Establishes a connection to the PostgreSQL database using SQLAlchemy.

        Description:
        Establishes a connection to the PostgreSQL database using SQLAlchemy's create_engine.
        If a database name is set, it connects to that specific database, otherwise it connects
        to the server without specifying a database.

        Parameters:
        None
        """
        if self.dbname:
            self.engine = create_engine(
                f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}/{self.dbname}",
                isolation_level="AUTOCOMMIT"
            )
        else:
            self.engine = create_engine(
                f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}",
                isolation_level="AUTOCOMMIT"
            )
        self.Session = sessionmaker(bind=self.engine)

    def create_database(self, db_name: str):
        """
        Creates a new database.

        Description:
        Drops the database if it exists and then creates a new database with the specified name.
        After creating the database, it sets up the SQLAlchemy engine to connect to this new database.

        Parameters:
        dbname (str): The name of the database to be created.

        Returns:
        None
        """
        self.execute_engine_query(f"DROP DATABASE IF EXISTS {db_name}")
        self.execute_engine_query(f"CREATE DATABASE {db_name}")
        self.dbname = db_name
        self.connect()

    def execute_engine_query(self, query: str):
        """
        Executes a SQL query using SQLAlchemy engine.

        Description:
        Executes the given SQL query using the SQLAlchemy engine. This method is generally used for
        queries that do not return results, such as DDL statements.

        Parameters:
        query (str): The SQL query to be executed.

        Returns:
        None
        """
        with self.engine.connect() as conn:
            result = conn.execute(text(query))
            if query.strip().lower().startswith('select'):
                return result.fetchall()
            return None
        
    def close_connection(self):
        """
        Closes the connection to the PostgreSQL database.

        Description:
        Closes the psycopg2 cursor and connection.

        Parameters:
        None

        Returns:
        None
        """
        self.pgcursor.close()
        self.pgconn.close()
