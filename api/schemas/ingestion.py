# api/schemas/ingestion.py

from pydantic import BaseModel, Field
from typing import List


# ---------
# YouTube
# ---------
class YouTubeItem(BaseModel):
    title: str = Field(..., min_length=1)
    artist: str = Field(..., min_length=1)
    views: int = Field(ge=0)


class YouTubePayload(BaseModel):
    items: List[YouTubeItem]


# ---------
# Radio
# ---------
class RadioItem(BaseModel):
    title: str
    artist: str
    plays: int = Field(ge=0)


class RadioPayload(BaseModel):
    items: List[RadioItem]


# ---------
# TV
# ---------
class TVItem(BaseModel):
    title: str
    artist: str
    plays: int = Field(ge=0)


class TVPayload(BaseModel):
    items: List[TVItem]