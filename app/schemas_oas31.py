# app/schemas_oas31.py
from pydantic import BaseModel, Field, ConfigDict
from typing import Union, Literal, Optional
import json

# Example of using OAS 3.1 schema features
class OAS31Config:
    """Configuration for OAS 3.1 schemas"""
    json_schema_extra = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "examples": [
            {"platform": "youtube", "status": "running"}
        ]
    }

class PlatformConfig(BaseModel):
    """Platform-specific configuration with discriminators"""
    platform_type: str = Field(..., discriminator="type")
    
    model_config = ConfigDict(
        json_schema_extra={
            "discriminator": {
                "propertyName": "platform_type",
                "mapping": {
                    "youtube": "#/components/schemas/YouTubeConfig",
                    "twitch": "#/components/schemas/TwitchConfig"
                }
            }
        }
    )

class YouTubeConfig(PlatformConfig):
    """YouTube-specific configuration"""
    platform_type: Literal["youtube"] = "youtube"
    api_key: str = Field(..., min_length=10)
    channel_id: Optional[str] = None
    use_oauth: bool = Field(default=True)
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "platform_type": "youtube",
                "api_key": "AIzaSy...",
                "channel_id": "UC1234567890",
                "use_oauth": True
            }
        }
    )

class TwitchConfig(PlatformConfig):
    """Twitch-specific configuration"""
    platform_type: Literal["twitch"] = "twitch"
    client_id: str
    access_token: str
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "platform_type": "twitch",
                "client_id": "abc123",
                "access_token": "oauth:xyz789"
            }
        }
    )

# Union type that will generate proper oneOf in OAS 3.1
PlatformConfigUnion = Union[YouTubeConfig, TwitchConfig]
