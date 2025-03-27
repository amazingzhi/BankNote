# import packages
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
import pickle
from src.preprocessing import DataCleaning
from src.features import FeatureCreation
from src.data import SnowflakeDB

# Access the snowflake connection details
snowflake = SnowflakeDB()

# read ori table from snowflake
df = snowflake.read_table(table_name="BANK_NOTE_TB")

# data cleaning
df_cleaned = DataCleaning.add_realtime_unique_key(df)
df_featured = FeatureCreation.main_feature_creation(df_cleaned)
X = df_featured.drop(columns=["CLASS", "uniq_key"])
y = df_featured['CLASS']

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

classifier = RandomForestClassifier(n_estimators=100)
classifier.fit(X_train, y_train)

y_pred = classifier.predict(X_test)

score = accuracy_score(y_test, y_pred)

pickle_out = open('../../artifacts/models/BankNote.pickle', 'wb')
pickle.dump(classifier, pickle_out)
pickle_out.close()