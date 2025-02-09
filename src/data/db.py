import mysql.connector
from mysql.connector import Error

class MySQLConnector:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = None

    def connect(self):
        """Establishes the MySQL database connection."""
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            if self.connection.is_connected():
                print("Connection established successfully.")
        except Error as e:
            print(f"Error connecting to MySQL: {e}")
            # Optionally, raise the exception or handle it as needed.
        return self.connection

    def execute_query(self, query, params=None):
        """Executes a given SQL query with optional parameters."""
        if self.connection is None:
            self.connect()
        cursor = self.connection.cursor(dictionary=True)
        try:
            cursor.execute(query, params)
            result = cursor.fetchall()
            return result
        except Error as e:
            print(f"Error executing query: {e}")
            return None
        finally:
            cursor.close()

    def close(self):
        """Closes the database connection."""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            print("Connection closed.")

    # Optionally, implement context management:
    def __enter__(self):
        return self.connect()

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()