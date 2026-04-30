from fastapi import Query, APIRouter, HTTPException, status
from models.tweets import Tweet, UpdateTweetModel, ApifyWebhook
from config.database import *
from schema.schemas import list_serial, individual_serial
from bson import ObjectId
from datetime import datetime
import json
import os
from apify_client import ApifyClient  # pyright: ignore[reportMissingImports]
from pydantic import BaseModel, field_validator
from pymongo.errors import PyMongoError
import re
import emoji
from pymongo.collection import ReturnDocument

router = APIRouter()

DATE_FORMAT = "%a %b %d %H:%M:%S +0000 %Y"
MAX_ITEMS_PER_RUN = 20
MAX_ITEMS_HARD_LIMIT = 100

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

class ScraperRequest(BaseModel):
    query: str
    administracionZonal: str
    origenDatos: str
    tipoQuery: str
    tipoZona: str
    maxItems: int = MAX_ITEMS_PER_RUN

    @field_validator("maxItems")
    @classmethod
    def clamp_max_items(cls, v):
        if v < 1:
            raise ValueError("Número máximo de tweets debe ser al menos 1")
        if v > MAX_ITEMS_HARD_LIMIT:
            raise ValueError(f"Número máximo de tweets  no puede superar {MAX_ITEMS_HARD_LIMIT}")
        return v


def build_query_context(req: ScraperRequest) -> dict:
    return {
        "query": req.query,
        "administracionZonal": req.administracionZonal,
        "origenDatos": req.origenDatos,
        "tipoQuery": req.tipoQuery,
        "tipoZona": req.tipoZona,
    }

# cleaning scripts

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
    seen_ids = set()
    cleaned = []

    for item in items:
        tweet_id = item.get("id")

        # deduplicate by id
        if tweet_id in seen_ids:
            continue
        seen_ids.add(tweet_id)

        # clean text
        cleaned_text = clean_tweet_text(item.get("text", ""))

        # skip if text matches blacklist
        if BLACKLIST_PATTERN.search(cleaned_text):
            continue

        cleaned.append({
            "id":                tweet_id,
            "createdAt":         item.get("createdAt"),
            "text":              cleaned_text,
            "administracionZonal": item.get("administracionZonal"),
            "origenDatos":       item.get("origenDatos"),
            "tipoQuery":         item.get("tipoQuery"),
            "tipoZona":          item.get("tipoZona"),
        })

    return cleaned

# crud methods

@router.get("/search")
async def get_tweets_by_fields(
    id: str = Query(None),
    administracionZonal: str = Query(None),
    origenDatos: str = Query(None),
    tipoQuery: str = Query(None),
    tipoZona: str = Query(None)
):

    if id:
        if not ObjectId.is_valid(id):
            raise HTTPException(status_code=400, detail="formato de ID inválido" \
            "")
        tweet = collection_name.find_one({"_id": ObjectId(id)})
        return [individual_serial(tweet)] if tweet else []

    query_filter = {}
    if administracionZonal: query_filter["administracionZonal"] = administracionZonal
    if origenDatos: query_filter["origenDatos"] = origenDatos
    if tipoQuery: query_filter["tipoQuery"] = tipoQuery
    if tipoZona: query_filter["tipoZona"] = tipoZona

    # .find() returns a cursor; list_serial converts the whole batch
    tweets = list_serial(collection_name.find(query_filter))
    
    if not tweets:
        raise HTTPException(status_code=404, detail="no se encontraron tweets con esos parámetros")
        
    return tweets

@router.post("/", status_code=status.HTTP_201_CREATED)
async def post_tweet(tweet: Tweet):

    tweet_dict = dict(tweet)
    
    
    if not tweet_dict.get("createdAt"):
        
        now = datetime.now()
        tweet_dict["createdAt"] = now.strftime("%a %b %d %H:%M:%S +0000 %Y")
    
    
    result = collection_name.insert_one(tweet_dict)
    
    # retornar el tweet para verificar insercion
    new_tweet = collection_name.find_one({"_id": result.inserted_id})
    return individual_serial(new_tweet)

@router.put("/{id}")
async def update_tweet(id: str, tweet: UpdateTweetModel):
    if not ObjectId.is_valid(id):
        raise HTTPException(status_code=400, detail="id inválido")

    update_data = {k: v for k, v in tweet.model_dump().items() if v is not None}

    if not update_data:
        raise HTTPException(status_code=400, detail="no se proporcionaron campos para actualizar")

    if "createdAt" in update_data:
        try:
            datetime.strptime(update_data["createdAt"], DATE_FORMAT)
        except ValueError:
            raise HTTPException(
                status_code=400, 
                detail="formato de fecha incorrecto. use el formato de twitter."
            )

    updated_tweet = collection_name.find_one_and_update(
        {"_id": ObjectId(id)},
        {"$set": update_data},
        return_document=ReturnDocument.AFTER
    )

    if not updated_tweet:
        raise HTTPException(status_code=404, detail="tweet no encontrado")

    return individual_serial(updated_tweet)

@router.delete("/{id}", status_code=status.HTTP_200_OK)
async def delete_tweet(id: str):
    if not ObjectId.is_valid(id):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, 
            detail="error: formato de id inválido"
        )

    deleted_tweet = collection_name.find_one_and_delete({"_id": ObjectId(id)})

    if not deleted_tweet:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, 
            detail="error: no existe un tweet con ese id"
        )

    return {"msg": f"tweet con id {id} ha sido eliminado correctamente"}

@router.post("/trigger-scraper")
async def trigger_apify_scraper(req: ScraperRequest):
    apify_token = os.getenv("APIFY_TOKEN")
    if not apify_token:
        return {"error": "apify token missing"}

    client = ApifyClient(apify_token)
    query_context = build_query_context(req)

    mapping_function = f"""(object) => {{
        return {{
            text: object.full_text || object.text,
            administracionZonal: {json.dumps(req.administracionZonal)},
            origenDatos: {json.dumps(req.origenDatos)},
            tipoQuery: {json.dumps(req.tipoQuery)},
            tipoZona: {json.dumps(req.tipoZona)},
            createdAt: object.created_at
        }}
    }}"""

    run_input = {
        "searchTerms": [req.query],
        "maxItems": req.maxItems,  # was MAX_ITEMS_PER_RUN
        "sort": "Latest",
        "tweetLanguage": "es",
        "customMapFunction": mapping_function
    }

    run = client.actor("kaitoeasyapi/twitter-x-data-tweet-scraper-pay-per-result-cheapest").start(run_input=run_input)

    scrape_run_collection.update_one(
        {"run_id": run["id"]},
        {
            "$set": {
                "run_id": run["id"],
                "query_context": query_context,
                "createdAt": datetime.now().strftime("%a %b %d %H:%M:%S +0000 %Y"),
            }
        },
        upsert=True,
    )
    
    return {"msg": "scraper started", "run_id": run["id"]}

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
                    normalized_items = []

                    for item in dataset_items:
                        if item.get("id") == -1:
                            print("warning: omitiendo datos de relleno de apify")
                            continue
                        item["apifyRunId"] = run_id
                        item["administracionZonal"] = query_context.get("administracionZonal")
                        item["origenDatos"] = query_context.get("origenDatos")
                        item["tipoQuery"] = query_context.get("tipoQuery")
                        item["tipoZona"] = query_context.get("tipoZona")
                        normalized_items.append(item)

                    # deduplicate within the current batch
                    seen = set()
                    unique_items = []
                    for item in normalized_items:
                        if item.get("id") not in seen:
                            seen.add(item.get("id"))
                            unique_items.append(item)
                    normalized_items = unique_items

                    # filter out tweets already in mongo
                    incoming_ids = [item.get("id") for item in normalized_items]
                    existing_ids = {
                        doc["id"] for doc in test_collection.find(
                            {"id": {"$in": incoming_ids}},
                            {"id": 1, "_id": 0}
                        )
                    }

                    new_items = [item for item in normalized_items if item.get("id") not in existing_ids]

                    if new_items:
                        result = test_collection.insert_many(new_items)
                        print(f"success: inserted {len(result.inserted_ids)} items, skipped {len(existing_ids)} duplicates")

                        cleaned_items = clean_data(new_items)
                        if cleaned_items:
                            test_collection_clean.insert_many(cleaned_items)
                            print(f"success: inserted {len(cleaned_items)} cleaned items into test_collection_clean")
                        else:
                            print("warning: no items survived cleaning")
                    else:
                        print("warning: all incoming tweets were duplicates, nothing inserted")

                else:
                    print("warning: apify dataset was empty, nothing to upload")

            except PyMongoError as e:
                print(f"ERROR mongodb: {e}")
            except Exception as e:
                print(f"ERROR unexpected: {e}")

    return {"status": "webhook processed"}