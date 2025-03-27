import pandas as pd


class FeatureCreation:
    """
    A class to encapsulate feature engineering functions.
    Provides static methods to create new features from existing columns.
    """

    @staticmethod
    def add_product_feature(df: pd.DataFrame) -> pd.DataFrame:
        """
        Creates a new feature 'product_feature' as the product of variance, skewness, curtosis, and entropy.
        """
        df = df.copy()
        df['product_feature'] = df['VARIANCE'] * df['SKEWNESS'] * df['CURTOSIS'] * df['ENTROPY']
        return df

    @staticmethod
    def add_sum_feature(df: pd.DataFrame) -> pd.DataFrame:
        """
        Creates a new feature 'sum_feature' as the sum of variance, skewness, curtosis, and entropy.
        """
        df = df.copy()
        df['sum_feature'] = df[['VARIANCE', 'SKEWNESS', 'CURTOSIS', 'ENTROPY']].sum(axis=1)
        return df

    @staticmethod
    def add_ratio_feature(df: pd.DataFrame) -> pd.DataFrame:
        """
        Creates a new feature 'ratio_feature' as the ratio of VARIANCE to (entropy + 1) to avoid division by zero.
        """
        df = df.copy()
        df['ratio_feature'] = df['VARIANCE'] / (df['ENTROPY'] + 1)
        return df

    @staticmethod
    def add_complex_feature(df: pd.DataFrame) -> pd.DataFrame:
        """
        Creates a new feature 'complex_feature' as a combination of the other columns.
        Here, for example, we compute (variance + skewness) multiplied by (curtosis - entropy).
        """
        df = df.copy()
        df['complex_feature'] = (df['VARIANCE'] + df['SKEWNESS']) * (df['CURTOSIS'] - df['ENTROPY'])
        return df

    @staticmethod
    def main_feature_creation(df: pd.DataFrame) -> pd.DataFrame:
        """
        Applies all feature creation methods one by one.

        Parameters:
            df (pd.DataFrame): DataFrame containing columns 'variance', 'skewness', 'curtosis', and 'entropy'.

        Returns:
            pd.DataFrame: DataFrame containing the original columns along with the newly created features.
        """
        df = FeatureCreation.add_product_feature(df)
        df = FeatureCreation.add_sum_feature(df)
        df = FeatureCreation.add_ratio_feature(df)
        df = FeatureCreation.add_complex_feature(df)
        return df