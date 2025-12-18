from sqlalchemy import Column, Integer, String
from api.db import Base

class Song(Base):
    __tablename__ = "songs"

    id = Column(Integer, primary_key=True, index=True)
    title = Column(String, index=True)
    artist = Column(String, index=True)
    region = Column(String, index=True)
    score = Column(Integer, default=0)