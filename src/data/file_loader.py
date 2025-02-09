# src/data/file_loader.py
import os
import pandas as pd


def read_csv(file_name, base_path='../data/raw'):
    """
    Reads a CSV file from the specified base path.

    :param file_name: Name of the CSV file.
    :param base_path: Relative path to the raw data directory.
    :return: DataFrame with the CSV data.
    """
    file_path = os.path.join(base_path, file_name)
    try:
        df = pd.read_csv(file_path)
        return df
    except Exception as e:
        print(f"Error reading CSV file at {file_path}: {e}")
        raise


def read_txt(file_name, base_path='../data/raw'):
    """
    Reads a text file from the specified base path.

    :param file_name: Name of the text file.
    :param base_path: Relative path to the raw data directory.
    :return: Contents of the text file as a string.
    """
    file_path = os.path.join(base_path, file_name)
    try:
        with open(file_path, 'r') as file:
            content = file.read()
        return content
    except Exception as e:
        print(f"Error reading text file at {file_path}: {e}")
        raise
