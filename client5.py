import streamlit as st
import requests
from datetime import datetime
import time
import pandas as pd

api_url = 'https://my-mongo-api-g4ii.onrender.com/trigger-scraper'

queries_data = [
   {
        "administracion_zonal":"calderon",
        "origen_datos":"medios",
        "tipo_query":"palabras clave",
        "tipo_zona":"parroquias rurales",
        "query_generado":"(extorsion OR robo OR sicariato OR asalto OR delincuencia OR inseguridad OR secuestro OR delito OR violencia OR criminal OR delincuentes OR \"lugar seguro\" OR seguridad OR tranquilo OR tranquilidad OR protegido OR paz OR calma OR policía OR patrullaje OR guardia) (\"parroquia de calderon\" OR \"llano chico\") -futbol -gol -filter:retweets filter:links"
    },
    {
        "administracion_zonal":"calderon",
        "origen_datos":"personas",
        "tipo_query":"palabras clave",
        "tipo_zona":"parroquias rurales",
        "query_generado":"(extorsion OR robo OR sicariato OR asalto OR delincuencia OR inseguridad OR secuestro OR delito OR violencia OR criminal OR delincuentes OR \"lugar seguro\" OR seguridad OR tranquilo OR tranquilidad OR protegido OR paz OR calma OR policía OR patrullaje OR guardia) (\"parroquia de calderon\" OR \"llano chico\") -futbol -gol -filter:retweets -filter:links"
    },
    {
        "administracion_zonal":"calderon",
        "origen_datos":"medios",
        "tipo_query":"geolocalizacion",
        "tipo_zona":"geolocalizacion",
        "query_generado":"(extorsion OR robo OR sicariato OR asalto OR delincuencia OR inseguridad OR secuestro OR delito OR violencia OR criminal OR delincuentes OR \"lugar seguro\" OR seguridad OR tranquilo OR tranquilidad OR protegido OR paz OR calma OR policía OR patrullaje OR guardia) geocode:-0.084717,-78.426257,6.87km -futbol -gol -filter:retweets filter:links"
    },
    {
        "administracion_zonal":"calderon",
        "origen_datos":"personas",
        "tipo_query":"geolocalizacion",
        "tipo_zona":"geolocalizacion",
        "query_generado":"(extorsion OR robo OR sicariato OR asalto OR delincuencia OR inseguridad OR secuestro OR delito OR violencia OR criminal OR delincuentes OR \"lugar seguro\" OR seguridad OR tranquilo OR tranquilidad OR protegido OR paz OR calma OR policía OR patrullaje OR guardia) geocode:-0.084717,-78.426257,6.87km -futbol -gol -filter:retweets -filter:links"
    },
    {
        "administracion_zonal":"eloy alfaro",
        "origen_datos":"medios",
        "tipo_query":"palabras clave",
        "tipo_zona":"parroquias urbanas",
        "query_generado":"(extorsion OR robo OR sicariato OR asalto OR delincuencia OR inseguridad OR secuestro OR delito OR violencia OR criminal OR delincuentes OR \"lugar seguro\" OR seguridad OR tranquilo OR tranquilidad OR protegido OR paz OR calma OR policía OR patrullaje OR guardia) (chilibulo OR solanda OR \"la magdalena\" OR \"san bartolo\" OR \"la ferroviaria\" OR chimbacalle OR \"la mena\" OR \"la argelia\") (quito OR pichincha OR ecuador OR uio) -futbol -gol -filter:retweets filter:links"
    },
    {
        "administracion_zonal":"eloy alfaro",
        "origen_datos":"personas",
        "tipo_query":"palabras clave",
        "tipo_zona":"parroquias urbanas",
        "query_generado":"(extorsion OR robo OR sicariato OR asalto OR delincuencia OR inseguridad OR secuestro OR delito OR violencia OR criminal OR delincuentes OR \"lugar seguro\" OR seguridad OR tranquilo OR tranquilidad OR protegido OR paz OR calma OR policía OR patrullaje OR guardia) (chilibulo OR solanda OR \"la magdalena\" OR \"san bartolo\" OR \"la ferroviaria\" OR chimbacalle OR \"la mena\" OR \"la argelia\") (quito OR pichincha OR ecuador OR uio) -futbol -gol -filter:retweets -filter:links"
    },
    {
        "administracion_zonal":"eloy alfaro",
        "origen_datos":"medios",
        "tipo_query":"palabras clave",
        "tipo_zona":"parroquias rurales",
        "query_generado":"(extorsion OR robo OR sicariato OR asalto OR delincuencia OR inseguridad OR secuestro OR delito OR violencia OR criminal OR delincuentes OR \"lugar seguro\" OR seguridad OR tranquilo OR tranquilidad OR protegido OR paz OR calma OR policía OR patrullaje OR guardia) (lloa) -futbol -gol -filter:retweets filter:links"
    },
    {
        "administracion_zonal":"eloy alfaro",
        "origen_datos":"personas",
        "tipo_query":"palabras clave",
        "tipo_zona":"parroquias rurales",
        "query_generado":"(extorsion OR robo OR sicariato OR asalto OR delincuencia OR inseguridad OR secuestro OR delito OR violencia OR criminal OR delincuentes OR \"lugar seguro\" OR seguridad OR tranquilo OR tranquilidad OR protegido OR paz OR calma OR policía OR patrullaje OR guardia) (lloa) -futbol -gol -filter:retweets -filter:links"
    },
    {
        "administracion_zonal":"eloy alfaro",
        "origen_datos":"medios",
        "tipo_query":"geolocalizacion",
        "tipo_zona":"geolocalizacion",
        "query_generado":"(extorsion OR robo OR sicariato OR asalto OR delincuencia OR inseguridad OR secuestro OR delito OR violencia OR criminal OR delincuentes OR \"lugar seguro\" OR seguridad OR tranquilo OR tranquilidad OR protegido OR paz OR calma OR policía OR patrullaje OR guardia) geocode:-0.188790,-78.672753,21.09km -futbol -gol -filter:retweets filter:links"
    },
    {
        "administracion_zonal":"eloy alfaro",
        "origen_datos":"personas",
        "tipo_query":"geolocalizacion",
        "tipo_zona":"geolocalizacion",
        "query_generado":"(extorsion OR robo OR sicariato OR asalto OR delincuencia OR inseguridad OR secuestro OR delito OR violencia OR criminal OR delincuentes OR \"lugar seguro\" OR seguridad OR tranquilo OR tranquilidad OR protegido OR paz OR calma OR policía OR patrullaje OR guardia) geocode:-0.188790,-78.672753,21.09km -futbol -gol -filter:retweets -filter:links"
    },
    {
        "administracion_zonal":"eugenio espejo",
        "origen_datos":"medios",
        "tipo_query":"palabras clave",
        "tipo_zona":"parroquias urbanas",
        "query_generado":"(extorsion OR robo OR sicariato OR asalto OR delincuencia OR inseguridad OR secuestro OR delito OR violencia OR criminal OR delincuentes OR \"lugar seguro\" OR seguridad OR tranquilo OR tranquilidad OR protegido OR paz OR calma OR policía OR patrullaje OR guardia) (\"belisario quevedo\" OR \"iñaquito\" OR rumipamba OR jipijapa OR cochapamba OR \"parroquia de concepcion\" OR \"parroquia kennedy\" OR \"san isidro del inca\" OR \"la mariscal\") -futbol -gol -filter:retweets filter:links"
    },
    {
        "administracion_zonal":"eugenio espejo",
        "origen_datos":"personas",
        "tipo_query":"palabras clave",
        "tipo_zona":"parroquias urbanas",
        "query_generado":"(extorsion OR robo OR sicariato OR asalto OR delincuencia OR inseguridad OR secuestro OR delito OR violencia OR criminal OR delincuentes OR \"lugar seguro\" OR seguridad OR tranquilo OR tranquilidad OR protegido OR paz OR calma OR policía OR patrullaje OR guardia) (\"belisario quevedo\" OR \"iñaquito\" OR rumipamba OR jipijapa OR cochapamba OR \"parroquia de concepcion\" OR \"parroquia kennedy\" OR \"san isidro del inca\" OR \"la mariscal\") -futbol -gol -filter:retweets -filter:links"
    },
    {
        "administracion_zonal":"eugenio espejo",
        "origen_datos":"medios",
        "tipo_query":"palabras clave",
        "tipo_zona":"parroquias rurales",
        "query_generado":"(extorsion OR robo OR sicariato OR asalto OR delincuencia OR inseguridad OR secuestro OR delito OR violencia OR criminal OR delincuentes OR \"lugar seguro\" OR seguridad OR tranquilo OR tranquilidad OR protegido OR paz OR calma OR policía OR patrullaje OR guardia) (zambiza OR nayon OR guayllabamba OR perucho OR chavezpamba OR puellaro OR \"parroquia atahualpa\" OR \"san jose de minas\") -futbol -gol -filter:retweets filter:links"
    },
    {
        "administracion_zonal":"eugenio espejo",
        "origen_datos":"personas",
        "tipo_query":"palabras clave",
        "tipo_zona":"parroquias rurales",
        "query_generado":"(extorsion OR robo OR sicariato OR asalto OR delincuencia OR inseguridad OR secuestro OR delito OR violencia OR criminal OR delincuentes OR \"lugar seguro\" OR seguridad OR tranquilo OR tranquilidad OR protegido OR paz OR calma OR policía OR patrullaje OR guardia) (zambiza OR nayon OR guayllabamba OR perucho OR chavezpamba OR puellaro OR \"parroquia atahualpa\" OR \"san jose de minas\") -futbol -gol -filter:retweets -filter:links"
    },
    {
        "administracion_zonal":"eugenio espejo",
        "origen_datos":"medios",
        "tipo_query":"geolocalizacion",
        "tipo_zona":"geolocalizacion",
        "query_generado":"(extorsion OR robo OR sicariato OR asalto OR delincuencia OR inseguridad OR secuestro OR delito OR violencia OR criminal OR delincuentes OR \"lugar seguro\" OR seguridad OR tranquilo OR tranquilidad OR protegido OR paz OR calma OR policía OR patrullaje OR guardia) geocode:-0.148905,-78.478724,10.02km -futbol -gol -filter:retweets filter:links"
    },
    {
        "administracion_zonal":"eugenio espejo",
        "origen_datos":"personas",
        "tipo_query":"geolocalizacion",
        "tipo_zona":"geolocalizacion",
        "query_generado":"(extorsion OR robo OR sicariato OR asalto OR delincuencia OR inseguridad OR secuestro OR delito OR violencia OR criminal OR delincuentes OR \"lugar seguro\" OR seguridad OR tranquilo OR tranquilidad OR protegido OR paz OR calma OR policía OR patrullaje OR guardia) geocode:-0.148905,-78.478724,10.02km -futbol -gol -filter:retweets -filter:links"
    },
    {
        "administracion_zonal":"la delicia",
        "origen_datos":"medios",
        "tipo_query":"palabras clave",
        "tipo_zona":"parroquias urbanas",
        "query_generado":"(extorsion OR robo OR sicariato OR asalto OR delincuencia OR inseguridad OR secuestro OR delito OR violencia OR criminal OR delincuentes OR \"lugar seguro\" OR seguridad OR tranquilo OR tranquilidad OR protegido OR paz OR calma OR policía OR patrullaje OR guardia) (carcelen OR \"urbanizacion el condado\" OR cotocollao OR ponceano OR \"comite del pueblo\") -futbol -gol -filter:retweets filter:links"
    },
    {
        "administracion_zonal":"la delicia",
        "origen_datos":"personas",
        "tipo_query":"palabras clave",
        "tipo_zona":"parroquias urbanas",
        "query_generado":"(extorsion OR robo OR sicariato OR asalto OR delincuencia OR inseguridad OR secuestro OR delito OR violencia OR criminal OR delincuentes OR \"lugar seguro\" OR seguridad OR tranquilo OR tranquilidad OR protegido OR paz OR calma OR policía OR patrullaje OR guardia) (carcelen OR \"urbanizacion el condado\" OR cotocollao OR ponceano OR \"comite del pueblo\") -futbol -gol -filter:retweets -filter:links"
    },
    {
        "administracion_zonal":"la delicia",
        "origen_datos":"medios",
        "tipo_query":"palabras clave",
        "tipo_zona":"parroquias rurales",
        "query_generado":"(extorsion OR robo OR sicariato OR asalto OR delincuencia OR inseguridad OR secuestro OR delito OR violencia OR criminal OR delincuentes OR \"lugar seguro\" OR seguridad OR tranquilo OR tranquilidad OR protegido OR paz OR calma OR policía OR patrullaje OR guardia) (calacali OR gualea OR nanega OR nanegalito OR \"parroquia nono\" OR \"parroquia pacto\" OR pomasqui OR \"parroquia san antonio\") -futbol -gol -filter:retweets filter:links"
    },
    {
        "administracion_zonal":"la delicia",
        "origen_datos":"personas",
        "tipo_query":"palabras clave",
        "tipo_zona":"parroquias rurales",
        "query_generado":"(extorsion OR robo OR sicariato OR asalto OR delincuencia OR inseguridad OR secuestro OR delito OR violencia OR criminal OR delincuentes OR \"lugar seguro\" OR seguridad OR tranquilo OR tranquilidad OR protegido OR paz OR calma OR policía OR patrullaje OR guardia) (calacali OR gualea OR nanega OR nanegalito OR \"parroquia nono\" OR \"parroquia pacto\" OR pomasqui OR \"parroquia san antonio\") -futbol -gol -filter:retweets -filter:links"
    },
    {
        "administracion_zonal":"la delicia",
        "origen_datos":"medios",
        "tipo_query":"geolocalizacion",
        "tipo_zona":"geolocalizacion",
        "query_generado":"(extorsion OR robo OR sicariato OR asalto OR delincuencia OR inseguridad OR secuestro OR delito OR violencia OR criminal OR delincuentes OR \"lugar seguro\" OR seguridad OR tranquilo OR tranquilidad OR protegido OR paz OR calma OR policía OR patrullaje OR guardia) geocode:0.154952,-78.623598,36.32km -futbol -gol -filter:retweets filter:links"
    },
    {
        "administracion_zonal":"la delicia",
        "origen_datos":"personas",
        "tipo_query":"geolocalizacion",
        "tipo_zona":"geolocalizacion",
        "query_generado":"(extorsion OR robo OR sicariato OR asalto OR delincuencia OR inseguridad OR secuestro OR delito OR violencia OR criminal OR delincuentes OR \"lugar seguro\" OR seguridad OR tranquilo OR tranquilidad OR protegido OR paz OR calma OR policía OR patrullaje OR guardia) geocode:0.154952,-78.623598,36.32km -futbol -gol -filter:retweets -filter:links"
    },
    {
        "administracion_zonal":"los chillos",
        "origen_datos":"medios",
        "tipo_query":"palabras clave",
        "tipo_zona":"parroquias rurales",
        "query_generado":"(extorsion OR robo OR sicariato OR asalto OR delincuencia OR inseguridad OR secuestro OR delito OR violencia OR criminal OR delincuentes OR \"lugar seguro\" OR seguridad OR tranquilo OR tranquilidad OR protegido OR paz OR calma OR policía OR patrullaje OR guardia) (amaguaña OR conocoto OR guangopolo OR alangasi OR \"parroquia la merced\" OR pintag) -futbol -gol -filter:retweets filter:links"
    },
    {
        "administracion_zonal":"los chillos",
        "origen_datos":"personas",
        "tipo_query":"palabras clave",
        "tipo_zona":"parroquias rurales",
        "query_generado":"(extorsion OR robo OR sicariato OR asalto OR delincuencia OR inseguridad OR secuestro OR delito OR violencia OR criminal OR delincuentes OR \"lugar seguro\" OR seguridad OR tranquilo OR tranquilidad OR protegido OR paz OR calma OR policía OR patrullaje OR guardia) (amaguaña OR conocoto OR guangopolo OR alangasi OR \"parroquia la merced\" OR pintag) -futbol -gol -filter:retweets -filter:links"
    },
    {
        "administracion_zonal":"los chillos",
        "origen_datos":"medios",
        "tipo_query":"geolocalizacion",
        "tipo_zona":"geolocalizacion",
        "query_generado":"(extorsion OR robo OR sicariato OR asalto OR delincuencia OR inseguridad OR secuestro OR delito OR violencia OR criminal OR delincuentes OR \"lugar seguro\" OR seguridad OR tranquilo OR tranquilidad OR protegido OR paz OR calma OR policía OR patrullaje OR guardia) geocode:-0.410948,-78.386421,21.48km -futbol -gol -filter:retweets filter:links"
    },
    {
        "administracion_zonal":"los chillos",
        "origen_datos":"personas",
        "tipo_query":"geolocalizacion",
        "tipo_zona":"geolocalizacion",
        "query_generado":"(extorsion OR robo OR sicariato OR asalto OR delincuencia OR inseguridad OR secuestro OR delito OR violencia OR criminal OR delincuentes OR \"lugar seguro\" OR seguridad OR tranquilo OR tranquilidad OR protegido OR paz OR calma OR policía OR patrullaje OR guardia) geocode:-0.410948,-78.386421,21.48km -futbol -gol -filter:retweets -filter:links"
    },
    {
        "administracion_zonal":"manuela saenz",
        "origen_datos":"medios",
        "tipo_query":"palabras clave",
        "tipo_zona":"parroquias urbanas",
        "query_generado":"(extorsion OR robo OR sicariato OR asalto OR delincuencia OR inseguridad OR secuestro OR delito OR violencia OR criminal OR delincuentes OR \"lugar seguro\" OR seguridad OR tranquilo OR tranquilidad OR protegido OR paz OR calma OR policía OR patrullaje OR guardia) (\"centro histórico de quito\" OR \"parroquia la libertad\" or \"parroquia san juan\" OR itchimbia OR puengasi) -futbol -gol -filter:retweets filter:links"
    },
    {
        "administracion_zonal":"manuela saenz",
        "origen_datos":"personas",
        "tipo_query":"palabras clave",
        "tipo_zona":"parroquias urbanas",
        "query_generado":"(extorsion OR robo OR sicariato OR asalto OR delincuencia OR inseguridad OR secuestro OR delito OR violencia OR criminal OR delincuentes OR \"lugar seguro\" OR seguridad OR tranquilo OR tranquilidad OR protegido OR paz OR calma OR policía OR patrullaje OR guardia) (\"centro histórico de quito\" OR \"parroquia la libertad\" or \"parroquia san juan\" OR itchimbia OR puengasi) -futbol -gol -filter:retweets -filter:links"
    },
    {
        "administracion_zonal":"manuela saenz",
        "origen_datos":"medios",
        "tipo_query":"geolocalizacion",
        "tipo_zona":"geolocalizacion",
        "query_generado":"(extorsion OR robo OR sicariato OR asalto OR delincuencia OR inseguridad OR secuestro OR delito OR violencia OR criminal OR delincuentes OR \"lugar seguro\" OR seguridad OR tranquilo OR tranquilidad OR protegido OR paz OR calma OR policía OR patrullaje OR guardia) geocode:-0.195928,-78.511113,7.20km -futbol -gol -filter:retweets filter:links"
    },
    {
        "administracion_zonal":"manuela saenz",
        "origen_datos":"personas",
        "tipo_query":"geolocalizacion",
        "tipo_zona":"geolocalizacion",
        "query_generado":"(extorsion OR robo OR sicariato OR asalto OR delincuencia OR inseguridad OR secuestro OR delito OR violencia OR criminal OR delincuentes OR \"lugar seguro\" OR seguridad OR tranquilo OR tranquilidad OR protegido OR paz OR calma OR policía OR patrullaje OR guardia) geocode:-0.195928,-78.511113,7.20km -futbol -gol -filter:retweets -filter:links"
    },
    {
        "administracion_zonal":"quitumbe",
        "origen_datos":"medios",
        "tipo_query":"palabras clave",
        "tipo_zona":"parroquias urbanas",
        "query_generado":"(extorsion OR robo OR sicariato OR asalto OR delincuencia OR inseguridad OR secuestro OR delito OR violencia OR criminal OR delincuentes OR \"lugar seguro\" OR seguridad OR tranquilo OR tranquilidad OR protegido OR paz OR calma OR policía OR patrullaje OR guardia) (chillogallo OR \"parroquia la ecuatoriana\" OR quitumbe OR turubamba) -futbol -gol -filter:retweets filter:links"
    },
    {
        "administracion_zonal":"quitumbe",
        "origen_datos":"personas",
        "tipo_query":"palabras clave",
        "tipo_zona":"parroquias urbanas",
        "query_generado":"(extorsion OR robo OR sicariato OR asalto OR delincuencia OR inseguridad OR secuestro OR delito OR violencia OR criminal OR delincuentes OR \"lugar seguro\" OR seguridad OR tranquilo OR tranquilidad OR protegido OR paz OR calma OR policía OR patrullaje OR guardia) (chillogallo OR \"parroquia la ecuatoriana\" OR quitumbe OR turubamba) -futbol -gol -filter:retweets -filter:links"
    },
    {
        "administracion_zonal":"quitumbe",
        "origen_datos":"medios",
        "tipo_query":"geolocalizacion",
        "tipo_zona":"geolocalizacion",
        "query_generado":"(extorsion OR robo OR sicariato OR asalto OR delincuencia OR inseguridad OR secuestro OR delito OR violencia OR criminal OR delincuentes OR \"lugar seguro\" OR seguridad OR tranquilo OR tranquilidad OR protegido OR paz OR calma OR policía OR patrullaje OR guardia) geocode:-0.307216,-78.556681,6.75km -futbol -gol -filter:retweets filter:links"
    },
    {
        "administracion_zonal":"quitumbe",
        "origen_datos":"personas",
        "tipo_query":"geolocalizacion",
        "tipo_zona":"geolocalizacion",
        "query_generado":"(extorsion OR robo OR sicariato OR asalto OR delincuencia OR inseguridad OR secuestro OR delito OR violencia OR criminal OR delincuentes OR \"lugar seguro\" OR seguridad OR tranquilo OR tranquilidad OR protegido OR paz OR calma OR policía OR patrullaje OR guardia) geocode:-0.307216,-78.556681,6.75km -futbol -gol -filter:retweets -filter:links"
    },
    {
        "administracion_zonal":"tumbaco",
        "origen_datos":"medios",
        "tipo_query":"palabras clave",
        "tipo_zona":"parroquias rurales",
        "query_generado":"(extorsion OR robo OR sicariato OR asalto OR delincuencia OR inseguridad OR secuestro OR delito OR violencia OR criminal OR delincuentes OR \"lugar seguro\" OR seguridad OR tranquilo OR tranquilidad OR protegido OR paz OR calma OR policía OR patrullaje OR guardia) (\"parroquia checa\" OR cumbaya OR \"el quinche\" OR pifo OR puembo OR yaruqui OR tababela OR tumbaco) -futbol -gol -filter:retweets filter:links"
    },
    {
        "administracion_zonal":"tumbaco",
        "origen_datos":"personas",
        "tipo_query":"palabras clave",
        "tipo_zona":"parroquias rurales",
        "query_generado":"(extorsion OR robo OR sicariato OR asalto OR delincuencia OR inseguridad OR secuestro OR delito OR violencia OR criminal OR delincuentes OR \"lugar seguro\" OR seguridad OR tranquilo OR tranquilidad OR protegido OR paz OR calma OR policía OR patrullaje OR guardia) (\"parroquia checa\" OR cumbaya OR \"el quinche\" OR pifo OR puembo OR yaruqui OR tababela OR tumbaco) -futbol -gol -filter:retweets -filter:links"
    },
    {
        "administracion_zonal":"tumbaco",
        "origen_datos":"medios",
        "tipo_query":"geolocalizacion",
        "tipo_zona":"geolocalizacion",
        "query_generado":"(extorsion OR robo OR sicariato OR asalto OR delincuencia OR inseguridad OR secuestro OR delito OR violencia OR criminal OR delincuentes OR \"lugar seguro\" OR seguridad OR tranquilo OR tranquilidad OR protegido OR paz OR calma OR policía OR patrullaje OR guardia) geocode:-0.205104,-78.305979,20.88km -futbol -gol -filter:retweets filter:links"
    },
    {
        "administracion_zonal":"tumbaco",
        "origen_datos":"personas",
        "tipo_query":"geolocalizacion",
        "tipo_zona":"geolocalizacion",
        "query_generado":"(extorsion OR robo OR sicariato OR asalto OR delincuencia OR inseguridad OR secuestro OR delito OR violencia OR criminal OR delincuentes OR \"lugar seguro\" OR seguridad OR tranquilo OR tranquilidad OR protegido OR paz OR calma OR policía OR patrullaje OR guardia) geocode:-0.205104,-78.305979,20.88km -futbol -gol -filter:retweets -filter:links"
    }
]

def run_scraper(item_data, limit):
    # mapeamos los datos locales a las llaves exactas que el backend espera
    payload = {
        "query": item_data.get("query_exacto_a_enviar", item_data["query_generado"]),
        "administracionZonal": item_data["administracion_zonal"], 
        "tipoZona": item_data["tipo_zona"],
        "origenDatos": item_data["origen_datos"],
        "tipoQuery": item_data["tipo_query"],
        "limit": limit
    }
    
    try:
        response = requests.post(api_url, json=payload)
        if not response.ok:
            return {
                "error": f"API request failed with status code {response.status_code}",
                "status_code": response.status_code,
                "response_text": response.text
            }

        try:
            return response.json()
        except ValueError:
            return {
                "error": "API returned an invalid JSON response",
                "status_code": response.status_code,
                "response_text": response.text
            }
    except Exception as e:
        return {"error": str(e)}

st.set_page_config(page_title="ejecutor masivo de queries", layout="wide")

st.title("ejecutor masivo de queries de seguridad")
st.write("configura las fechas y el límite de resultados. revisa la vista previa antes de ejecutar.")

# controles
col1, col2, col3 = st.columns(3)
with col1:
    date_since = st.date_input("desde", value=None)
with col2:
    date_until = st.date_input("hasta", value=None)
with col3:
    limit_results = st.number_input("límite de resultados por run", min_value=1, value=15, step=1)

st.divider()

if "preview_ready" not in st.session_state:
    st.session_state.preview_ready = False
if "final_queries_list" not in st.session_state:
    st.session_state.final_queries_list = []

if st.button("generar vista previa", use_container_width=True):
    date_filter = ""
    if date_since:
        unix_since = int(datetime.combine(date_since, datetime.min.time()).timestamp())
        date_filter += f" since_time:{unix_since}"
    if date_until:
        unix_until = int(datetime.combine(date_until, datetime.max.time()).timestamp())
        date_filter += f" until_time:{unix_until}"

    preview_list = []
    for item in queries_data:
        final_query = f"{item['query_generado']}{date_filter}"
        preview_list.append({
            "administracion_zonal": item["administracion_zonal"],
            "origen_datos": item["origen_datos"],
            "tipo_query": item["tipo_query"],
            "tipo_zona": item["tipo_zona"],
            "query_exacto_a_enviar": final_query # apify necesita esto obligatoriamente
        })
    
    st.session_state.final_queries_list = preview_list
    st.session_state.preview_ready = True

if st.session_state.preview_ready:
    st.subheader("revisa los queries antes de ejecutar")
    
    # creamos el dataframe y ocultamos la columna del query solo para la parte visual
    df_preview = pd.DataFrame(st.session_state.final_queries_list)
    st.dataframe(df_preview.drop(columns=["query_exacto_a_enviar"]), use_container_width=True)
    
    st.warning(f"se ejecutarán {len(st.session_state.final_queries_list)} corridas. cada una está limitada a un máximo de {limit_results} resultados.")
    
    if st.button("confirmar y ejecutar scraper", type="primary", use_container_width=True):
        progress_bar = st.progress(0)
        status_text = st.empty()
        results = []
        total_queries = len(st.session_state.final_queries_list)

        for i, item in enumerate(st.session_state.final_queries_list):
            status_text.text(f"ejecutando {i+1} de {total_queries}: {item['administracion_zonal']} ({item['origen_datos']})")
            
            result = run_scraper(item, limit_results)
            
            results.append({
                "zona": item["administracion_zonal"], 
                "origen": item["origen_datos"], 
                "resultado": result
            })
            
            progress_bar.progress((i + 1) / total_queries)
            time.sleep(1)
            
        status_text.text("ejecución masiva completada.")
        st.success("todas las consultas fueron procesadas.")
        
        with st.expander("ver resultados de la api"):
            st.json(results)