def individual_serial(tweet) -> dict:
    serialized = {
        "id": str(tweet["_id"]),
        "text": tweet["text"],
        "administracionZonal": tweet["administracionZonal"],
        "origenDatos": tweet["origenDatos"],
        "tipoQuery": tweet["tipoQuery"],
        "tipoZona": tweet["tipoZona"],
    }

    if "query_context" in tweet:
        serialized["query_context"] = tweet["query_context"]

    if "apifyRunId" in tweet:
        serialized["apifyRunId"] = tweet["apifyRunId"]

    return serialized

def list_serial(tweets) -> list:
    return[individual_serial(tweet) for tweet in tweets]