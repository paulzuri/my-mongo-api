from fastapi import Query, APIRouter, HTTPException, status
from models.tweets import Tweet, UpdateTweetModel, ApifyWebhook
from config.database import collection_name, test_collection
from schema.schemas import list_serial, individual_serial
from bson import ObjectId
from datetime import datetime
import os
from apify_client import ApifyClient  # pyright: ignore[reportMissingImports]
from pydantic import BaseModel
from pymongo.errors import PyMongoError

router = APIRouter()

DATE_FORMAT = "%a %b %d %H:%M:%S +0000 %Y"

class ScraperRequest(BaseModel):
    query: str
    administracionZonal: str
    tipoZona: str

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
            raise HTTPException(status_code=400, detail="" \
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

    update_data = {k: v for k, v in tweet.dict().items() if v is not None}

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
        return_document=True
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

    mapping_function = f"""(object) => {{
        return {{
            text: object.full_text || object.text,
            administracionZonal: '{req.administracionZonal}',
            origenDatos: 'apify scraper',
            tipoQuery: 'twitter search',
            tipoZona: '{req.tipoZona}',
            createdAt: object.created_at
        }}
    }}"""

    run_input = {
        "searchTerms": [req.query],
        "maxItems": 3, 
        "sort": "Latest",
        "tweetLanguage": "es",
        "customMapFunction": mapping_function
    }

    run = client.actor("kaitoeasyapi/twitter-x-data-tweet-scraper-pay-per-result-cheapest").start(run_input=run_input)
    
    return {"msg": "scraper started", "run_id": run["id"]}

@router.post("/webhooks/apify")
async def handle_apify_webhook(data: ApifyWebhook):
    if data.eventType == "ACTOR.RUN.SUCCEEDED" and data.resource:
        dataset_id = data.resource.get("defaultDatasetId")
        
        if dataset_id:
            try:
                client = ApifyClient(os.getenv("APIFY_TOKEN"))
                dataset_items = client.dataset(dataset_id).list_items().items
                
                if dataset_items:
                    # try to insert the data
                    result = test_collection.insert_many(dataset_items)
                    print(f"success: inserted {len(result.inserted_ids)} items into mongo")
                else:
                    print("warning: apify dataset was empty, nothing to upload")
                    
            except PyMongoError as e:
                # this catches database-specific errors (connection, permissions, etc.)
                print(f"ERROR mongodb: {e}")
            except Exception as e:
                # this catches everything else (apify client errors, network issues)
                print(f"ERROR unexpected: {e}")
                
    return {"status": "webhook processed"}