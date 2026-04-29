import json
from datetime import date, datetime, time
from pathlib import Path

import requests
import streamlit as st

api_url = "https://my-mongo-api-g4ii.onrender.com/trigger-scraper"
queries_path = Path(__file__).with_name("queries_seguridad_quito.json")

st.set_page_config(page_title="twitter query builder", layout="centered")


@st.cache_data(show_spinner=False)
def load_query_templates():
    with queries_path.open(encoding="utf-8") as file:
        return json.load(file)


def build_date_filters(date_range):
    if not date_range or len(date_range) != 2:
        return ""

    date_since, date_until = date_range
    filters = []

    if date_since:
        unix_since = int(datetime.combine(date_since, time.min).timestamp())
        filters.append(f"since_time:{unix_since}")

    if date_until:
        unix_until = int(datetime.combine(date_until, time.max).timestamp())
        filters.append(f"until_time:{unix_until}")

    return " ".join(filters)


def build_preview_queries(date_range):
    date_filters = build_date_filters(date_range)
    preview_queries = []

    for index, template in enumerate(load_query_templates(), start=1):
        final_query = template["query_generado"].strip()
        if date_filters:
            final_query = f"{final_query} {date_filters}"

        preview_queries.append(
            {
                "index": index,
                "administracion_zonal": template["administracion_zonal"],
                "origen_datos": template["origen_datos"],
                "tipo_query": template["tipo_query"],
                "tipo_zona": template["tipo_zona"],
                "query_generado": template["query_generado"],
                "final_query": " ".join(final_query.split()),
            }
        )

    return preview_queries


def run_scraper(query_item):
    payload = {
        "query": query_item["final_query"],
        "administracionZonal": query_item["administracion_zonal"],
        "origenDatos": query_item["origen_datos"],
        "tipoQuery": query_item["tipo_query"],
        "tipoZona": query_item["tipo_zona"],
    }

    try:
        response = requests.post(api_url, json=payload, timeout=120)
        if not response.ok:
            return {"error": f"{response.status_code}: {response.text}"}
        return response.json()
    except Exception as error:
        return {"error": str(error)}


def execute_batch(queries):
    results = []

    for query_item in queries:
        result = run_scraper(query_item)
        results.append(
            {
                "index": query_item["index"],
                "administracion_zonal": query_item["administracion_zonal"],
                "origen_datos": query_item["origen_datos"],
                "tipo_query": query_item["tipo_query"],
                "tipo_zona": query_item["tipo_zona"],
                "result": result,
            }
        )

    return results


if "preview_queries" not in st.session_state:
    st.session_state.preview_queries = []

if "last_results" not in st.session_state:
    st.session_state.last_results = []

st.title("twitter query builder")
st.caption("La aplicación carga todas las consultas desde el JSON y solo permite ajustar el rango de fechas.")

date_range = st.date_input(
    "selector de fechas",
    value=(date.today(), date.today()),
)

st.divider()

if st.button("generar vista previa", use_container_width=True):
    st.session_state.preview_queries = build_preview_queries(date_range)
    st.session_state.last_results = []

if st.session_state.preview_queries:
    preview_queries = st.session_state.preview_queries

    st.subheader("vista previa de queries")
    st.write(f"Se ejecutarán {len(preview_queries)} consultas, una por una.")

    preview_rows = [
        {
            "administracion_zonal": item["administracion_zonal"],
            "origen_datos": item["origen_datos"],
            "tipo_query": item["tipo_query"],
            "tipo_zona": item["tipo_zona"],
            "query_final": item["final_query"],
        }
        for item in preview_queries
    ]

    st.dataframe(preview_rows, use_container_width=True, hide_index=True)

    with st.expander("ver queries completas"):
        for item in preview_queries:
            st.markdown(
                f"**{item['index']}. {item['administracion_zonal']} | {item['origen_datos']} | {item['tipo_query']} | {item['tipo_zona']}**"
            )
            st.code(item["final_query"], language="text")

    st.divider()

    if st.button("confirmar y ejecutar todas las queries", use_container_width=True):
        with st.spinner("iniciando ejecuciones..."):
            st.session_state.last_results = execute_batch(preview_queries)

        success_count = sum(1 for item in st.session_state.last_results if not item["result"].get("error"))
        error_count = len(st.session_state.last_results) - success_count

        if success_count:
            st.success(f"{success_count} queries enviadas correctamente.")
        if error_count:
            st.error(f"{error_count} queries fallaron.")

        st.write("resultado por query:")
        for item in st.session_state.last_results:
            result = item["result"]
            title = f"{item['index']}. {item['administracion_zonal']}"
            if result.get("error"):
                st.error(f"{title}: {result['error']}")
            else:
                st.success(f"{title}: run_id {result.get('run_id')}")