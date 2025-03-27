import pandas as pd
import uuid

class DataCleaning:
    """
    A class to encapsulate data cleaning functions.
    """

    @staticmethod
    def add_realtime_unique_key(df: pd.DataFrame) -> pd.DataFrame:
        """
        Adds a unique key to each row of the DataFrame for real-time transactional data.
        The key is generated using uuid.uuid4(), ensuring a globally unique identifier for each transaction.

        Parameters:
            df (pd.DataFrame): DataFrame containing real-time transactional data.

        Returns:
            pd.DataFrame: DataFrame with an additional column 'uniq_key' containing the unique identifiers.
        """
        df = df.copy()
        df['uniq_key'] = [str(uuid.uuid4()) for _ in range(df.shape[0])]
        return df