from fastapi import Query, APIRouter, HTTPException, status
from models.tweets import Tweet, UpdateTweetModel
from config.database import collection_name
from schema.schemas import list_serial, individual_serial
from bson import ObjectId
from datetime import datetime

router = APIRouter()

DATE_FORMAT = "%a %b %d %H:%M:%S +0000 %Y"

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