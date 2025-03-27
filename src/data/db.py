import mysql.connector
from mysql.connector import Error
import snowflake.connector
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
import yaml

with open("../../config/snowflake_connection.yaml", "r") as file:
    config = yaml.safe_load(file)
sf_config = config["snowflake"]

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


class SnowflakeDB:
    def __init__(self, user=sf_config["user"], password=sf_config["password"], account=sf_config["account"],
                 warehouse=sf_config["warehouse"], database=sf_config["database"], schema=sf_config["schema"]):
        self.user = user
        self.password = password
        self.account = account
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.connection = None

    def connect(self):
        """Establish a connection to Snowflake if not already connected."""
        if self.connection is None:
            try:
                self.connection = snowflake.connector.connect(
                    user=self.user,
                    password=self.password,
                    account=self.account,
                    warehouse=self.warehouse,
                    database=self.database,
                    schema=self.schema
                )
            except Exception as e:
                raise Exception(f"Error connecting to Snowflake: {e}")

    def disconnect(self):
        """Close the connection if open."""
        if self.connection:
            self.connection.close()
            self.connection = None

    def execute_query(self, query, params=None):
        """Execute a given query with optional parameters."""
        self.connect()
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, params)
            return cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error executing query: {e}")
        finally:
            cursor.close()

    def read_table(self, table_name, batch_size=None, query=None, stream=False):
        """
        Read data from Snowflake as a pandas DataFrame or an iterator of DataFrame chunks.

        Args:
            table_name (str): Name of the table to read.
            batch_size (int, optional): Number of rows per batch for chunked reading.
            query (str, optional): Custom SQL query. If provided, this query is used
                to load data. Otherwise, the entire table is read.
            stream (bool, optional): If True, read from the table's stream (e.g., BANK_NOTE_TB_STREAM).

        Returns:
            pd.DataFrame or Iterator[pd.DataFrame]: The full DataFrame if batch_size is None,
            otherwise an iterator yielding DataFrame chunks.
        """
        self.connect()

        # If a custom query is provided, use that; otherwise, adjust based on the stream flag.
        if query:
            final_query = query
        else:
            # Append _STREAM to the table name if stream is True.
            target_table = f"{table_name}_STREAM" if stream else table_name
            final_query = f"SELECT * FROM {target_table}"

        try:
            if batch_size:
                return pd.read_sql(final_query, self.connection, chunksize=batch_size)
            else:
                return pd.read_sql(final_query, self.connection)
        except Exception as e:
            raise Exception(f"Error reading data: {e}")

    def insert_dataframe(self, df, table_name, if_exists="append"):
        """
        Insert a pandas DataFrame into a Snowflake table.
        - If if_exists is 'replace', the table is recreated and then data is inserted.
        - If if_exists is 'append', the code checks if the table exists; if not, it creates the table with
          column names quoted (preserving case), otherwise it appends rows.

        Args:
            df (pd.DataFrame): The DataFrame to insert.
            table_name (str): Target table name.
            if_exists (str): 'append' to add rows or 'replace' to recreate the table.

        Returns:
            int: Number of rows inserted.
        """
        self.connect()
        cursor = self.connection.cursor()
        try:
            # Create a simple schema with quoted column names to preserve lowercase.
            cols = ", ".join([f'"{col}" VARCHAR' for col in df.columns])

            if if_exists == "replace":
                # Recreate the table with the new schema.
                create_query = f"CREATE OR REPLACE TABLE {table_name} ({cols})"
                cursor.execute(create_query)
            elif if_exists == "append":
                # Check if the table exists.
                cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
                table_exists = cursor.fetchone() is not None
                if not table_exists:
                    # Create the table with quoted column names.
                    create_query = f"CREATE TABLE {table_name} ({cols})"
                    cursor.execute(create_query)

            # Insert data using write_pandas.
            success, nchunks, nrows, _ = write_pandas(self.connection, df, table_name)
            if not success:
                raise Exception("Failed to write DataFrame to Snowflake")
            return nrows
        except Exception as e:
            raise Exception(f"Error inserting data into table {table_name}: {e}")
        finally:
            cursor.close()

    def update_table(self, table_name, set_data, where_data):
        """
        Update records in a table using dictionary inputs for SET and WHERE clauses.

        Args:
            table_name (str): The table to update.
            set_data (dict): Dictionary where keys are columns to update and values are the new values.
            where_data (dict): Dictionary where keys are columns for the WHERE clause and values are the conditions.

        Example:
            update_table("users", {"status": "active"}, {"id": 123})
            will generate and execute:
            UPDATE users SET status = 'active' WHERE id = 123
        """
        self.connect()
        # Build SET clause from set_data
        set_clause = ", ".join(
            [f"{col} = '{val}'" if isinstance(val, str) else f"{col} = {val}"
             for col, val in set_data.items()]
        )
        # Build WHERE clause from where_data
        where_clause = " AND ".join(
            [f"{col} = '{val}'" if isinstance(val, str) else f"{col} = {val}"
             for col, val in where_data.items()]
        )
        query = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}"
        self.execute_query(query)

    def delete_from_table(self, table_name, condition):
        """
        Delete records from a table based on a condition.

        Args:
            table_name (str): The table from which to delete rows.
            condition (str): WHERE clause condition (e.g., "id = 123").
        """
        self.connect()
        query = f"DELETE FROM {table_name} WHERE {condition}"
        self.execute_query(query)