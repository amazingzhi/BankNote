# src/data/migrations.py

from db import SnowflakeDB
from models import USER_TABLE


class MigrationManager:
    def __init__(self, db: SnowflakeDB):
        self.db = db

    def get_current_schema(self, table_name: str) -> dict:
        """
        Query Snowflake's INFORMATION_SCHEMA to get the current table schema.
        Returns a dictionary with column names as keys and a dict of type and default.
        """
        sql = f"""
            SELECT COLUMN_NAME, DATA_TYPE, COLUMN_DEFAULT
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{table_name.upper()}'
        """
        rows = self.db.execute_query(sql)
        schema = {}
        for row in rows:
            col_name, data_type, column_default = row
            # Normalize column names to lowercase for easier comparison.
            schema[col_name.lower()] = {
                "type": data_type.upper() if data_type else None,
                "default": str(column_default).upper() if column_default is not None else None
            }
        return schema

    def create_table(self, table_def: dict):
        """
        Create a new table in Snowflake based on the desired schema defined in table_def.
        """
        table_name = table_def["name"]
        desired_columns = table_def["columns"]

        columns_defs = []
        for col, spec in desired_columns.items():
            if isinstance(spec, dict):
                col_def = f"{col} {spec.get('type')}"
                if spec.get("default") is not None:
                    col_def += f" DEFAULT {spec.get('default')}"
            else:
                # If spec is provided as a simple string.
                col_def = f"{col} {spec}"
            columns_defs.append(col_def)

        cols_sql = ", ".join(columns_defs)
        sql = f"CREATE TABLE {table_name} ({cols_sql})"
        print(f"Creating table with: {sql}")
        self.db.execute_query(sql)
        print(f"Table '{table_name}' created successfully.")

    def migrate_table(self, table_def: dict):
        """
        Compare the desired schema (from models.py) with the current schema in Snowflake.
        If the table doesn't exist, it creates it.
        For differences, generate the appropriate ALTER TABLE statements.
        """
        table_name = table_def["name"]
        desired_columns = table_def["columns"]

        # Normalize desired schema: ensure column names are lowercase.
        normalized_desired = {}
        for col, spec in desired_columns.items():
            if isinstance(spec, dict):
                normalized_desired[col.lower()] = {
                    "type": spec.get("type", "").upper(),
                    "default": str(spec.get("default")).upper() if spec.get("default") is not None else None
                }
            else:
                normalized_desired[col.lower()] = {"type": spec.upper(), "default": None}

        # Get the current schema from Snowflake.
        current_schema = self.get_current_schema(table_name)
        print(f"Current schema for table '{table_name}': {current_schema}")
        print(f"Desired schema for table '{table_name}': {normalized_desired}")

        # If table does not exist, create it.
        if not current_schema:
            print(f"Table '{table_name}' does not exist. Creating new table...")
            self.create_table(table_def)
            return

        # 1. Add columns that are in desired but not in current.
        for col, spec in normalized_desired.items():
            if col not in current_schema:
                self.add_column(table_name, col, spec)

        # 2. Drop columns that exist in current but not in desired.
        for col in current_schema:
            if col not in normalized_desired:
                self.drop_column(table_name, col)

        # 3. For columns that exist in both, modify if type or default differ.
        for col, desired_spec in normalized_desired.items():
            if col in current_schema:
                current_spec = current_schema[col]
                if not self.compare_column_specs(desired_spec, current_spec):
                    self.modify_column(table_name, col, desired_spec)

    def add_column(self, table_name: str, col: str, spec: dict):
        """Generate and execute ALTER TABLE ADD COLUMN."""
        sql = f"ALTER TABLE {table_name} ADD COLUMN {col} {spec['type']}"
        if spec.get("default"):
            sql += f" DEFAULT {spec['default']}"
        print(f"Executing: {sql}")
        self.db.execute_query(sql)
        print(f"Added column '{col}' to table '{table_name}'.")

    def drop_column(self, table_name: str, col: str):
        """Generate and execute ALTER TABLE DROP COLUMN."""
        sql = f"ALTER TABLE {table_name} DROP COLUMN {col}"
        print(f"Executing: {sql}")
        self.db.execute_query(sql)
        print(f"Dropped column '{col}' from table '{table_name}'.")

    def modify_column(self, table_name: str, col: str, spec: dict):
        """
        Generate and execute ALTER TABLE commands to modify the column type
        and default value.
        """
        sql_type = f"ALTER TABLE {table_name} ALTER COLUMN {col} TYPE {spec['type']}"
        print(f"Executing: {sql_type}")
        self.db.execute_query(sql_type)

        if spec.get("default"):
            sql_default = f"ALTER TABLE {table_name} ALTER COLUMN {col} SET DEFAULT {spec['default']}"
        else:
            sql_default = f"ALTER TABLE {table_name} ALTER COLUMN {col} DROP DEFAULT"
        print(f"Executing: {sql_default}")
        self.db.execute_query(sql_default)
        print(f"Modified column '{col}' in table '{table_name}'.")

    def compare_column_specs(self, desired: dict, current: dict) -> bool:
        """
        Compare desired and current column specifications.
        Returns True if they match; otherwise, False.
        """
        if desired["type"] != current["type"]:
            return False
        if desired.get("default") != current.get("default"):
            return False
        return True


# --- Example Usage ---

if __name__ == "__main__":
    # Initialize your SnowflakeDB instance.
    # Replace the placeholders with your actual Snowflake credentials and parameters.
    db = SnowflakeDB(
        user="YOUR_USER",
        password="YOUR_PASSWORD",
        account="YOUR_ACCOUNT",
        warehouse="YOUR_WAREHOUSE",
        database="YOUR_DATABASE",
        schema="YOUR_SCHEMA"
    )

    migration_manager = MigrationManager(db)
    try:
        # Migrate the users table defined in models.py.
        migration_manager.migrate_table(USER_TABLE)
    finally:
        db.disconnect()
