from pydantic import BaseModel, field_validator
from typing import Optional, Any, Dict

DEFAULT_MAX_ITEMS_PER_RUN = 100
MAX_ITEMS_HARD_LIMIT = 1000

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

class ScraperRequest(BaseModel):
    query: str
    administracionZonal: str
    origenDatos: str
    tipoQuery: str
    tipoZona: str
    maxItems: int = DEFAULT_MAX_ITEMS_PER_RUN

    @field_validator("maxItems")
    @classmethod
    def clamp_max_items(cls, v):
        if v < 1:
            raise ValueError("Número máximo de tweets debe ser al menos 1")
        if v > MAX_ITEMS_HARD_LIMIT:
            raise ValueError(f"Número máximo de tweets  no puede superar {MAX_ITEMS_HARD_LIMIT}")
        return v