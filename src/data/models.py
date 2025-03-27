# Define your table schema as a dictionary.
# You can later extend this structure with more metadata (e.g., for renaming)
BANKNOTE_ORI_TABLE = {
    "name": "original_data",
    "columns": {
        # If you want to change a column name, you’d create a new key here.
        # For instance, to rename 'id' to 'user_id', update the key.
        "variance": {"type": "FLOAT", "default": None},
        "skewness": {"type": "FLOAT", "default": None},
        "curtosis": {"type": "FLOAT", "default": None},
        "entropy": {"type": "FLOAT", "default": None},
        "class": {"type": "INT", "default": None},
    }
}