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
    userId: Optional[str] = None
    createdAt: Optional[str] = None
    eventType: Optional[str] = None
    eventData: Optional[Dict[str, Any]] = None
    resource: Optional[Dict[str, Any]] = None
    globals: Optional[Dict[str, Any]] = None