import json
from datetime import date, datetime, time
from pathlib import Path
import requests
import streamlit as st

api_url = "https://my-mongo-api-g4ii.onrender.com/trigger-scraper"
queries_path = Path(__file__).with_name("queries_seguridad_quito.json")

st.set_page_config(page_title="Generar datos", layout="centered")


@st.cache_data(show_spinner=False)
def load_query_list_from_json():
    with queries_path.open(encoding="utf-8") as file:
        return json.load(file)


def build_date_filters(date_since, date_until):
    filters = []
    unix_since = int(datetime.combine(date_since, time.min).timestamp())
    unix_until = int(datetime.combine(date_until, time.max).timestamp())
    filters.append(f"since_time:{unix_since}")
    filters.append(f"until_time:{unix_until}")
    return " ".join(filters)


def build_query_list(date_since, date_until, max_items):
    date_filters = build_date_filters(date_since, date_until)
    query_list = []

    for index, template in enumerate(load_query_list_from_json(), start=1):
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
        response = requests.post(api_url, json=payload, timeout=60)
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


if "query_list" not in st.session_state:
    st.session_state.query_list = []

if "last_results" not in st.session_state:
    st.session_state.last_results = []

st.title("🔬 Tweet scrapper")

col1, col2 = st.columns(2)
date_since = col1.date_input("Desde", value=date.today(), max_value=date.today())
date_until = col2.date_input("Hasta", value=date.today(), max_value=date.today())

if date_since > date_until:
    st.warning("La fecha de inicio no puede ser mayor a la fecha de fin.")

max_items = st.radio(
    "Máximo de tweets por query",
    options=[50, 100, 500, 1000],
    horizontal=True,
)

st.divider()

# botones rojos, no esencial
st.markdown("""
<style>
div .stButton > button {
    background-color: #FF6B6B;
    color: white;
}
</style>
""", unsafe_allow_html=True)

if st.button("Generar vista previa", use_container_width=True, disabled=date_since > date_until):
    st.session_state.query_list = build_query_list(date_since, date_until, max_items)
    st.session_state.last_results = []

if st.session_state.query_list:
    query_list = st.session_state.query_list

    st.subheader("Vista previa de queries")

    query_labels = [
        f"{item['index']}. {item['administracion_zonal']} | {item   ['origen_datos']} | {item['tipo_query']} | {item['tipo_zona']}"
        for item in query_list
    ]

    selected_labels = st.multiselect(
        "Seleccionar queries a ejecutar",
        options=query_labels,
        default=query_labels,
    )

    st.caption(f"{len(selected_labels)} de {len(query_list)} queries seleccionadas")

    st.dataframe(
        [
            {
                "Zona": item["administracion_zonal"],
                "Origen": item["origen_datos"],
                "Tipo query": item["tipo_query"],
                "Tipo zona": item["tipo_zona"],
                "Query": item["final_query"],
            }
            for item in query_list
        ],
        use_container_width=True,
        hide_index=True,
    )

    st.divider()

    if st.button("Confirmar y ejecutar queries seleccionadas", use_container_width=True):
        selected_queries = [
            query for query, label in zip(query_list, query_labels)
            if label in selected_labels
        ]

        if not selected_queries:
            st.warning("No has seleccionado ninguna query.")
        else:
            with st.spinner(f"Ejecutando {len(selected_queries)} de {len(query_list)} queries..."):
                st.session_state.last_results = execute_batch(selected_queries)

            success_count = sum(1 for item in st.session_state.last_results if not item["result"].get("error"))
            error_count = len(st.session_state.last_results) - success_count

            if success_count:
                st.success(f"{success_count} queries enviadas correctamente.")
            if error_count:
                st.error(f"{error_count} queries fallaron.")

            st.write("Resultado por query:")
            for item in st.session_state.last_results:
                result = item["result"]
                title = f"{item['index']}. {item['administracion_zonal']}"
                if result.get("error"):
                    st.error(f"{title}: {result['error']}")
                else:
                    st.success(f"{title}: Tarea {item['index']} ejecutada")