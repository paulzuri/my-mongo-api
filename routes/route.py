from fastapi import Query, APIRouter, HTTPException
from models.models import *
from config.database import *
from datetime import datetime
import os
from apify_client import ApifyClient  # pyright: ignore[reportMissingImports]
from pymongo.errors import PyMongoError
import re
import emoji
import requests

router = APIRouter()

DATE_FORMAT = "%a %b %d %H:%M:%S +0000 %Y"

BLACKLIST = {
    "aucas", "paz en su tumba", "musulmanes", "clausuras", "cuenca",
    "jeff bezos", "tramite", "colombia", "españa", "estadio",
    "vidal", "muerte blanca", "madrid", "chucky", "eeuu", "farandula",
    "pacientes", "patiño", "seguro médico", "hincha", "vacaciones",
    "guatemala", "correistas", "político", "borja", "políticos",
    "loja", "perú", "haiti", "correato", "marxismo", "reinoso",
    "correa", "hinchada", "otavalo", "imbabura", "petro", "amlo",
    "jesus", "yasuni", "nobel", "travesti", "dios"
}

BLACKLIST_PATTERN = re.compile(
    '|'.join([rf'\b{re.escape(word)}\b' for word in BLACKLIST]),
    re.IGNORECASE
)

def build_query_context(req: ScraperRequest) -> dict:
    return {
        "query": req.query,
        "administracionZonal": req.administracionZonal,
        "origenDatos": req.origenDatos,
        "tipoQuery": req.tipoQuery,
        "tipoZona": req.tipoZona,
    }

def clean_tweet_text(text: str) -> str:
    if not isinstance(text, str):
        return ""
    text = emoji.replace_emoji(text, replace='')
    text = re.sub(r'@\w+', '', text)
    text = re.sub(r'http\S+|www\S+', '', text)
    text = re.sub(r'#', '', text)
    text = re.sub(r'\s+', ' ', text)
    return text.lower().strip()

def clean_data(items: list) -> list:
    cleaned = []

    for item in items:
        cleaned_text = clean_tweet_text(item.get("text", ""))

        if BLACKLIST_PATTERN.search(cleaned_text):
            continue

        cleaned.append({
            "id":                item.get("id"),
            "createdAt":         item.get("createdAt"),
            "text":              cleaned_text,
            "administracionZonal": item.get("administracionZonal"),
            "origenDatos":       item.get("origenDatos"),
            "tipoQuery":         item.get("tipoQuery"),
            "tipoZona":          item.get("tipoZona"),
        })

    return cleaned

@router.post("/trigger-scraper")
async def trigger_apify_scraper(req: ScraperRequest):
    apify_token = os.getenv("APIFY_TOKEN")
    if not apify_token:
        return {"error": "No existe el token de Apify"}

    client = ApifyClient(apify_token)
    query_context = build_query_context(req)

    run_input = {
        "searchTerms": [req.query],
        "maxItems": req.maxItems,  
        "sort": "Latest",
        "tweetLanguage": "es",
    }

    run = client.actor("kaitoeasyapi/twitter-x-data-tweet-scraper-pay-per-result-cheapest").start(run_input=run_input)

    scrape_run_collection.update_one(
        {"run_id": run["id"]},
        {
            "$set": {
                "run_id": run["id"],
                "query_context": query_context,
                "createdAt": datetime.now().strftime(DATE_FORMAT),
            }
        },
        upsert=True,
    )
    
    return {"msg": "scraper started", "run_id": run["id"]}


@router.get("/runs/{run_id}")
async def get_run_status(run_id: str, maxItems: int = Query(1000)):
    apify_token = os.getenv("APIFY_TOKEN")
    if not apify_token:
        return {"Error": "Token de Apify faltante"}

    run_url = f"https://api.apify.com/v2/actor-runs/{run_id}?token={apify_token}"
    try:
        r = requests.get(run_url, timeout=10)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error de Apify: {e}")

    if not r.ok:
        raise HTTPException(status_code=r.status_code, detail=f"Apify retorna {r.status_code}")

    run_data = r.json().get("data", {})
    status = run_data.get("status")
    dataset_id = run_data.get("defaultDatasetId")

    items_count = 0
    if dataset_id:
        ds_url = f"https://api.apify.com/v2/datasets/{dataset_id}/items?limit={maxItems}&token={apify_token}"
        try:
            dsr = requests.get(ds_url, timeout=10)
            if dsr.ok:
                items = dsr.json()
                if isinstance(items, list):
                    items_count = len(items)
        except Exception:
            items_count = 0

    run_record = scrape_run_collection.find_one({"run_id": run_id}) or {}
    clean_inserted = run_record.get("cleanInserted")

    return {
        "status": status,
        "datasetId": dataset_id,
        "itemsCount": items_count,
        "cleanInserted": clean_inserted,
    }

@router.post("/webhooks/apify")
async def handle_apify_webhook(data: ApifyWebhook):
    if data.eventType == "ACTOR.RUN.SUCCEEDED" and data.resource:
        dataset_id = data.resource.get("defaultDatasetId")
        run_id = data.resource.get("id")

        if dataset_id:
            try:
                client = ApifyClient(os.getenv("APIFY_TOKEN"))
                dataset_items = client.dataset(dataset_id).list_items().items
                run_record = scrape_run_collection.find_one({"run_id": run_id}) or {}
                query_context = run_record.get("query_context", {})

                if dataset_items:
                    print(f"[{run_id}] Dataset recibido: {len(dataset_items)} tweets en total")

                    filler_count = sum(1 for item in dataset_items if item.get("id") == -1)
                    if filler_count > 0:
                        print(f"[{run_id}] Advertencia: omitiendo {filler_count} items de relleno de Apify (id=-1)")

                    normalized_items = []
                    for item in dataset_items:
                        if item.get("id") == -1:
                            continue
                        item["apifyRunId"] = run_id
                        item["administracionZonal"] = query_context.get("administracionZonal")
                        item["origenDatos"] = query_context.get("origenDatos")
                        item["tipoQuery"] = query_context.get("tipoQuery")
                        item["tipoZona"] = query_context.get("tipoZona")
                        normalized_items.append(item)

                    seen = set()
                    unique_items = []
                    for item in normalized_items:
                        if item.get("id") not in seen:
                            seen.add(item.get("id"))
                            unique_items.append(item)
                    normalized_items = unique_items

                    print(f"[{run_id}] Tras eliminar duplicados internos: {len(normalized_items)} tweets únicos")

                    incoming_ids = [item.get("id") for item in normalized_items]
                    existing_ids = {
                        doc["id"] for doc in test_collection.find(
                            {"id": {"$in": incoming_ids}},
                            {"id": 1, "_id": 0}
                        )
                    }
                    new_items = [item for item in normalized_items if item.get("id") not in existing_ids]

                    print(f"[{run_id}] Ya existían en MongoDB: {len(existing_ids)} | Nuevos a insertar: {len(new_items)}")

                    if new_items:
                        result = test_collection.insert_many(new_items)
                        print(f"[{run_id}] Insertados en colección raw: {len(result.inserted_ids)} tweets")

                        cleaned_items = clean_data(new_items)
                        print(f"[{run_id}] Tras limpieza y filtrado: {len(cleaned_items)} tweets | Descartados: {len(new_items) - len(cleaned_items)}")

                        clean_inserted = 0
                        if cleaned_items:
                            test_collection_clean.insert_many(cleaned_items)
                            clean_inserted = len(cleaned_items)
                            print(f"[{run_id}] Insertados en colección clean: {clean_inserted} tweets")
                        else:
                            print(f"[{run_id}] Advertencia: ningún tweet sobrevivió la limpieza, colección clean sin cambios")

                        scrape_run_collection.update_one(
                            {"run_id": run_id},
                            {"$set": {
                                "rawInserted": len(result.inserted_ids),
                                "cleanInserted": clean_inserted,
                            }}
                        )
                    else:
                        print(f"[{run_id}] Advertencia: todos los tweets recibidos ya existían en MongoDB, nada insertado")
                        scrape_run_collection.update_one(
                            {"run_id": run_id},
                            {"$set": {"rawInserted": 0, "cleanInserted": 0}}
                        )
                else:
                    print(f"[{run_id}] Advertencia: el dataset de Apify llegó vacío, nada que procesar")
                    scrape_run_collection.update_one(
                        {"run_id": run_id},
                        {"$set": {"rawInserted": 0, "cleanInserted": 0}}
                    )

            except PyMongoError as e:
                print(f"[{run_id}] Error de MongoDB: {e}")
            except Exception as e:
                print(f"[{run_id}] Error inesperado: {e}")

    return {"status": "webhook processed"}