import json
import time
from datetime import date, datetime, time as dt_time
from pathlib import Path
import requests
import streamlit as st

api_url_trigger = "https://my-mongo-api-g4ii.onrender.com/trigger-scraper"
api_url_base = "https://my-mongo-api-g4ii.onrender.com"

queries_path = Path(__file__).with_name("queries_seguridad_quito.json")

st.set_page_config(page_title="Generar datos", layout="centered")

@st.cache_data(show_spinner=False)
def load_query_list_from_json():
    with queries_path.open(encoding="utf-8") as file:
        return json.load(file)

def build_date_filters(date_since, date_until):
    filters = []
    unix_since = int(datetime.combine(date_since, dt_time.min).timestamp())
    unix_until = int(datetime.combine(date_until, dt_time.max).timestamp())
    filters.append(f"since_time:{unix_since}")
    filters.append(f"until_time:{unix_until}")
    return " ".join(filters)

def build_query_list(date_since, date_until, max_items, selected_zones):
    date_filters = build_date_filters(date_since, date_until)
    query_list = []
    index = 1

    for template in load_query_list_from_json():
        if template["administracion_zonal"] not in selected_zones:
            continue

        final_query = template["query_generado"].strip()
        if date_filters:
            final_query = f"{final_query} {date_filters}"

        query_list.append(
            {
                "index": index,
                "administracion_zonal": template["administracion_zonal"],
                "origen_datos": template["origen_datos"],
                "tipo_query": template["tipo_query"],
                "tipo_zona": template["tipo_zona"],
                "query_generado": template["query_generado"],
                "final_query": " ".join(final_query.split()),
                "max_items": max_items,
            }
        )
        index += 1

    return query_list


def run_scraper(query_item):
    payload = {
        "query": query_item["final_query"],
        "administracionZonal": query_item["administracion_zonal"],
        "origenDatos": query_item["origen_datos"],
        "tipoQuery": query_item["tipo_query"],
        "tipoZona": query_item["tipo_zona"],
        "maxItems": query_item["max_items"],
    }

    try:
        response = requests.post(api_url_trigger, json=payload, timeout=60)
        if not response.ok:
            return {"error": f"{response.status_code}: {response.text}"}
        return response.json()
    except Exception as error:
        return {"error": str(error)}

def check_run_status(run_id):
    try:
        response = requests.get(f"{api_url_base}/runs/{run_id}", timeout=10)
        if response.ok:
            return response.json()
        return {"status": "ERROR"}
    except Exception:
        return {"status": "ERROR"}

st.title("🔬 Generar datos")

col1, col2 = st.columns(2)
date_since = col1.date_input("Desde", value=date.today(), max_value=date.today())
date_until = col2.date_input("Hasta", value=date.today(), max_value=date.today())

if date_since > date_until:
    st.warning("La fecha de inicio no puede ser mayor a la fecha de fin.")

max_items = st.radio(
    "Máximo de tweets por query",
    options=[25, 50, 100, 500, 1000],
    horizontal=True,
)

try:
    raw_templates = load_query_list_from_json()
    all_zones = sorted(list(set(template["administracion_zonal"] for template in raw_templates)))
except Exception as e:
    st.error(f"Error al cargar las administraciones zonales: {e}")
    all_zones = []

selected_zones = st.multiselect(
    "Selección de administraciones zonales",
    options=all_zones,
    default=all_zones,
    placeholder="Elige una o más administraciones zonales"
)

st.divider()

if st.button("Ejecutar queries seleccionados", use_container_width=True, disabled=(date_since > date_until or not selected_zones)):
    final_queries_to_run = build_query_list(date_since, date_until, max_items, selected_zones)
    
    queries_by_zone = {}
    for q in final_queries_to_run:
        zone = q["administracion_zonal"]
        if zone not in queries_by_zone:
            queries_by_zone[zone] = []
        queries_by_zone[zone].append(q)
    
    for zone, items in queries_by_zone.items():
        with st.container(border=True):
            st.markdown(f"### {zone.title()}")
            
            for q in items:
                st.markdown(f"**Query {q['index']}**")
                
                status_placeholder = st.empty()
                
                st.markdown(f"**Origen de datos:** {q['origen_datos']}")
                st.markdown(f"**Tipo query:** {q['tipo_query']}")
                st.markdown(f"**Tipo zona:** {q['tipo_zona']}")
                st.markdown(f"**Query generado:** `{q['final_query']}`")
                
                with status_placeholder.container():
                    with st.spinner("Iniciando tarea en la API..."):
                        trigger_result = run_scraper(q)
                
                if "error" in trigger_result:
                    status_placeholder.error(f"Error al iniciar: {trigger_result['error']}")
                    st.divider()
                    continue
                    
                run_id = trigger_result.get("run_id")
                
                while True:
                    with status_placeholder.container():
                        with st.spinner(f"Generando datos en Apify... (Run ID: {run_id})"):
                            status_data = check_run_status(run_id)
                            current_status = status_data.get("status")
                    
                    if current_status == "SUCCEEDED":
                        status_placeholder.success(f"Completado")
                        break
                    elif current_status in ["FAILED", "ABORTED", "TIMED-OUT", "ERROR"]:
                        status_placeholder.error(f"La tarea falló en Apify con estado: {current_status}")
                        break
                        
                    time.sleep(2)
                
                st.divider()