# src/feature_engineering/__init__.py

# Import the database connection class from db.py
from .feature_engineering import FeatureCreation

# Optional: Define __all__ to explicitly declare the public API of this package.
__all__ = [
    "FeatureCreation"
]