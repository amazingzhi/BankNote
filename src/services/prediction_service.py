from src.data import SnowflakeDB
from src.preprocessing import DataCleaning
from src.features import FeatureCreation
import pickle
# Access the snowflake connection details
snowflake = SnowflakeDB()

# read ori table from snowflake
df = snowflake.read_table(table_name="BANK_NOTE_TB", stream=True)
df = df[['VARIANCE', 'SKEWNESS', 'CURTOSIS', 'ENTROPY', 'CLASS']]

# data cleaning
df_cleaned = DataCleaning.add_realtime_unique_key(df)
df_featured = FeatureCreation.main_feature_creation(df_cleaned)
new_data = df_featured.drop(columns=["CLASS", "uniq_key"])

# Load the saved model from the specified file path
model_path = '../../artifacts/models/BankNote.pickle'
with open(model_path, 'rb') as f:
    model = pickle.load(f)

# Make predictions on the new data
predictions = model.predict(new_data)

# Assume new_data is your DataFrame and predictions is an array or list
new_data['prediction'] = predictions

snowflake.insert_dataframe(new_data, table_name="BANK_NOTE_PRED", if_exists="append")