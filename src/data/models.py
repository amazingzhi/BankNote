# src/data/models.py
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
from sqlalchemy.orm import relationship

Base = declarative_base()

class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True)
    username = Column(String(50), unique=True, nullable=False)
    email = Column(String(100), unique=True, nullable=False)
    created_at = Column(DateTime)

    # Define relationships (if any)
    posts = relationship("Post", back_populates="author")
    """there is a defined relationship between the User and Post models (which correspond to the users and posts tables). However, this line itself does not create a database column in the users table. Instead, it creates a Python attribute on the User class that lets you access all the related Post objects for that user."""

class Post(Base):
    __tablename__ = 'posts'

    id = Column(Integer, primary_key=True)
    title = Column(String(100))
    content = Column(String)
    author_id = Column(Integer, ForeignKey('users.id')) # this line confirms post table has a foreign key called author_id link to users.id

    # Define relationships (if any)
    author = relationship("User", back_populates="posts")