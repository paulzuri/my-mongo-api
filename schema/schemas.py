def individual_serial(tweet) -> dict:
    return {
        "id": str(tweet["_id"]),
        "text": tweet["text"],
        "administracionZonal": tweet["administracionZonal"],
        "origenDatos": tweet["origenDatos"],
        "tipoQuery": tweet["tipoQuery"],
        "tipoZona": tweet["tipoZona"],
    }

def list_serial(tweets) -> list:
    return[individual_serial(tweet) for tweet in tweets]