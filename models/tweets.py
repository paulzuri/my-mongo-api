from pydantic import BaseModel
from datetime import datetime
from typing import Optional, Any, Dict

class Tweet(BaseModel):
    text: str
    administracionZonal: str
    origenDatos: str
    tipoQuery: str
    tipoZona: str
    createdAt: Optional[str] = None

class UpdateTweetModel(BaseModel):
    text: Optional[str] = None
    administracionZonal: Optional[str] = None
    origenDatos: Optional[str] = None
    tipoQuery: Optional[str] = None
    tipoZona: Optional[str] = None
    createdAt: Optional[str] = None

class ApifyWebhook(BaseModel):
    userId: str
    createdAt: str
    eventType: str
    eventData: Dict[str, Any]
    resourse: Dict[str, Any]