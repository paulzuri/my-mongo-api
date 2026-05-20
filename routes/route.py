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

@router.post("/trigger-scraper")
async def trigger_apify_scraper(req: ScraperRequest):
    apify_token = os.getenv("APIFY_TOKEN")
    if not apify_token:
        return {"error": "apify token missing"}

    try:
        client = ApifyClient(apify_token)
        query_context = build_query_context(req)

        run_input = {
            "searchTerms": [req.query],
            "maxItems": req.maxItems,  # was MAX_ITEMS_PER_RUN
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
                    "status": "triggered",
                    "webhookProcessed": False,
                    "createdAt": datetime.now().strftime("%a %b %d %H:%M:%S +0000 %Y"),
                }
            },
            upsert=True,
        )
        
        return {"msg": "scraper started", "run_id": run["id"]}
    except Exception as e:
        print(f"ERROR triggering apify: {e}")
        return {"error": f"Failed to start scraper: {str(e)}"}


@router.get("/runs/{run_id}")
async def get_run_status(run_id: str, maxItems: int = Query(1000)):
    apify_token = os.getenv("APIFY_TOKEN")
    if not apify_token:
        return {"error": "apify token missing"}

    # Fetch run details from Apify API
    run_url = f"https://api.apify.com/v2/actor-runs/{run_id}?token={apify_token}"
    try:
        r = requests.get(run_url, timeout=10)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"error contacting apify: {e}")

    if not r.ok:
        raise HTTPException(status_code=r.status_code, detail=f"apify returned {r.status_code}")

    run_data = r.json()
    apify_run = run_data.get("data", run_data)
    status = apify_run.get("status")
    dataset_id = apify_run.get("defaultDatasetId")
    run_record = scrape_run_collection.find_one({"run_id": run_id}) or {}

    items_count = 0
    normalized_items_count = run_record.get("normalizedItemCount", 0)
    batch_duplicate_count = run_record.get("batchDuplicateCount", 0)
    existing_duplicate_count = run_record.get("existingDuplicateCount", 0)
    inserted_raw_count = run_record.get("insertedRawCount", 0)
    cleaned_items_count = run_record.get("cleanedItemCount", 0)
    error_reason = run_record.get("errorReason")
    if dataset_id:
        try:
            stored_count = run_record.get("datasetItemCount")
            if stored_count is not None:
                items_count = stored_count
            else:
                client = ApifyClient(apify_token)
                items_count = len(client.dataset(dataset_id).list_items(limit=maxItems).items)
        except Exception:
            items_count = 0

    return {
        "status": status,
        "datasetId": dataset_id,
        "itemsCount": items_count,
        "normalizedItemCount": normalized_items_count,
        "batchDuplicateCount": batch_duplicate_count,
        "existingDuplicateCount": existing_duplicate_count,
        "insertedRawCount": inserted_raw_count,
        "cleanedItemCount": cleaned_items_count,
        "errorReason": error_reason,
        "webhookProcessed": run_record.get("webhookProcessed", False),
        "webhookStatus": run_record.get("webhookStatus", "unknown"),
        "queryContext": run_record.get("query_context", {}),
    }

@router.post("/webhooks/apify")
async def handle_apify_webhook(data: ApifyWebhook):
    if data.eventType == "ACTOR.RUN.FAILED" and data.resource:
        run_id = data.resource.get("id")
        error_info = data.resource.get("errorInfo") or {}
        error_message = error_info.get("message", "Unknown error")
        
        print(f"ERROR: Apify run {run_id} failed: {error_message}")
        
        scrape_run_collection.update_one(
            {"run_id": run_id},
            {
                "$set": {
                    "webhookStatus": "failed",
                    "webhookProcessed": True,
                    "errorReason": f"Apify run failed: {error_message}",
                    "webhookProcessedAt": datetime.now().strftime("%a %b %d %H:%M:%S +0000 %Y"),
                }
            },
        )
        return {"status": "webhook processed - run failed"}

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
                    raw_item_count = len(dataset_items)
                    filler_only_count = sum(1 for item in dataset_items if item.get("id") == -1)
                    
                    if filler_only_count == raw_item_count:
                        print(f"warning: run returned {raw_item_count} items but all were filler data")
                        scrape_run_collection.update_one(
                            {"run_id": run_id},
                            {
                                "$set": {
                                    "webhookStatus": "no_real_data",
                                    "webhookProcessed": True,
                                    "datasetItemCount": raw_item_count,
                                    "errorReason": "No real data found, only filler",
                                    "webhookProcessedAt": datetime.now().strftime("%a %b %d %H:%M:%S +0000 %Y"),
                                }
                            },
                        )
                        return {"status": "webhook processed - no real data"}
                    
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

                    normalized_count = len(normalized_items)

                    # deduplicate within the current batch
                    seen = set()
                    unique_items = []
                    for item in normalized_items:
                        if item.get("id") not in seen:
                            seen.add(item.get("id"))
                            unique_items.append(item)
                    normalized_items = unique_items

                    batch_deduped_count = len(normalized_items)
                    batch_duplicate_count = normalized_count - batch_deduped_count

                    # filter out tweets already in mongo
                    incoming_ids = [item.get("id") for item in normalized_items]
                    existing_ids = {
                        doc["id"] for doc in test_collection.find(
                            {"id": {"$in": incoming_ids}},
                            {"id": 1, "_id": 0}
                        )
                    }

                    existing_duplicate_count = len(existing_ids)

                    new_items = [item for item in normalized_items if item.get("id") not in existing_ids]
                    new_items_count = len(new_items)

                    cleaned_items = []

                    if new_items:
                        result = test_collection.insert_many(new_items)
                        inserted_raw_count = len(result.inserted_ids)
                        print(
                            "success: run returned "
                            f"{raw_item_count} items, "
                            f"{normalized_count} remained after cleaning metadata, "
                            f"{batch_duplicate_count} removed as duplicates inside the batch, "
                            f"{existing_duplicate_count} already existed in Mongo, "
                            f"{new_items_count} inserted as raw items ({inserted_raw_count} inserted records)"
                        )

                        cleaned_items = clean_data(new_items)
                        if cleaned_items:
                            cleaned_insert_result = test_collection_clean.insert_many(cleaned_items)
                            print(
                                "success: cleaned pipeline kept "
                                f"{len(cleaned_items)} items and inserted "
                                f"{len(cleaned_insert_result.inserted_ids)} cleaned items into test_collection_clean"
                            )
                        else:
                            print(
                                "warning: no items survived text cleaning after raw insertion; "
                                f"run returned {raw_item_count} items and {new_items_count} were inserted raw"
                            )
                    else:
                        print(
                            "warning: run returned "
                            f"{raw_item_count} items, but all {batch_deduped_count} normalized items "
                            "already existed in Mongo or were duplicate inside the batch; nothing inserted"
                        )
                        cleaned_items = clean_data(new_items)

                        scrape_run_collection.update_one(
                            {"run_id": run_id},
                            {
                                "$set": {
                                    "webhookStatus": "all_duplicates",
                                    "webhookProcessed": True,
                                    "datasetItemCount": raw_item_count,
                                    "normalizedItemCount": normalized_count,
                                    "batchDuplicateCount": batch_duplicate_count,
                                    "existingDuplicateCount": existing_duplicate_count,
                                    "insertedRawCount": 0,
                                    "cleanedItemCount": 0,
                                    "errorReason": "All items were duplicates",
                                    "webhookProcessedAt": datetime.now().strftime("%a %b %d %H:%M:%S +0000 %Y"),
                                }
                            },
                        )
                        return {"status": "webhook processed - all duplicates"}

                    scrape_run_collection.update_one(
                        {"run_id": run_id},
                        {
                            "$set": {
                                "webhookStatus": "processed",
                                "webhookProcessed": True,
                                "datasetItemCount": raw_item_count,
                                "normalizedItemCount": normalized_count,
                                "batchDuplicateCount": batch_duplicate_count,
                                "existingDuplicateCount": existing_duplicate_count,
                                "insertedRawCount": new_items_count,
                                "cleanedItemCount": len(cleaned_items),
                                "webhookProcessedAt": datetime.now().strftime("%a %b %d %H:%M:%S +0000 %Y"),
                            }
                        },
                    )

                else:
                    print("warning: apify dataset was empty, nothing to upload")
                    scrape_run_collection.update_one(
                        {"run_id": run_id},
                        {
                            "$set": {
                                "webhookStatus": "empty_dataset",
                                "webhookProcessed": True,
                                "datasetItemCount": 0,
                                "errorReason": "No results returned from Apify",
                                "webhookProcessedAt": datetime.now().strftime("%a %b %d %H:%M:%S +0000 %Y"),
                            }
                        },
                    )

            except PyMongoError as e:
                print(f"ERROR mongodb: {e}")
                error_msg = str(e)
                scrape_run_collection.update_one(
                    {"run_id": run_id},
                    {
                        "$set": {
                            "webhookStatus": "db_error",
                            "webhookProcessed": True,
                            "errorReason": f"Database error: {error_msg}",
                            "webhookProcessedAt": datetime.now().strftime("%a %b %d %H:%M:%S +0000 %Y"),
                        }
                    },
                )
            except Exception as e:
                print(f"ERROR unexpected: {e}")
                error_msg = str(e)
                scrape_run_collection.update_one(
                    {"run_id": run_id},
                    {
                        "$set": {
                            "webhookStatus": "unexpected_error",
                            "webhookProcessed": True,
                            "errorReason": f"Unexpected error: {error_msg}",
                            "webhookProcessedAt": datetime.now().strftime("%a %b %d %H:%M:%S +0000 %Y"),
                        }
                    },
                )

    return {"status": "webhook processed"}