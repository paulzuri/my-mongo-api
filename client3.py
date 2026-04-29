import streamlit as st
import requests
from datetime import datetime
import time

api_url = 'https://my-mongo-api-g4ii.onrender.com/trigger-scraper'

st.set_page_config(page_title="twitter query builder", layout="centered")

# 1. state management
if "term_options" not in st.session_state:
    st.session_state.term_options = [
        "extorsion", "robo", "sicariato", "asalto", "delincuencia", 
        "inseguridad", "secuestro", "delito", "violencia", "criminal", 
        "delincuentes", "seguridad", "policía", "patrullaje", "guardia"
    ]

def add_custom_term():
    term = st.session_state.new_term_input.strip().lower()
    if term and term not in st.session_state.term_options:
        st.session_state.term_options.append(term)
    st.session_state.new_term_input = ""

ZONE_DATA = {
    "calderon": {
        "parroquias rurales": '("parroquia de calderon" OR "llano chico")',
        "geolocalizacion": 'geocode:-0.084717,-78.426257,6.87km'
    },
    "eloy alfaro": {
        "parroquias urbanas": '(chilibulo OR solanda OR "la magdalena" OR "san bartolo" OR "la ferroviaria" OR chimbacalle OR "la mena" OR "la argelia")',
        "parroquias rurales": '(lloa)',
        "geolocalizacion": 'geocode:-0.188790,-78.672753,21.09km'
    },
    "eugenio espejo": {
        "parroquias urbanas": '("belisario quevedo" OR "iñaquito" OR rumipamba OR jipijapa OR cochapamba OR "parroquia de concepcion" OR "parroquia kennedy" OR "san isidro del inca" OR "la mariscal")',
        "parroquias rurales": '(zambiza OR nayon OR guayllabamba OR perucho OR chavezpamba OR puellaro OR "parroquia atahualpa" OR "san jose de minas")',
        "geolocalizacion": 'geocode:-0.148905,-78.478724,10.02km'
    },
    "la delicia": {
        "parroquias urbanas": '(carcelen OR "urbanizacion el condado" OR cotocollao OR ponceano OR "comite del pueblo")',
        "parroquias rurales": '(calacali OR gualea OR nanega OR nanegalito OR "parroquia nono" OR "parroquia pacto" OR pomasqui OR "parroquia san antonio")',
        "geolocalizacion": 'geocode:0.154952,-78.623598,36.32km'
    },
    "los chillos": {
        "parroquias rurales": '(amaguaña OR conocoto OR guangopolo OR alangasi OR "parroquia la merced" OR pintag)',
        "geolocalizacion": 'geocode:-0.410948,-78.386421,21.48km'
    },
    "manuela saenz": {
        "parroquias urbanas": '("centro histórico de quito" OR "parroquia la libertad" OR "parroquia san juan" OR itchimbia OR puengasi)',
        "geolocalizacion": 'geocode:-0.195928,-78.511113,7.20km'
    },
    "quitumbe": {
        "parroquias urbanas": '(chillogallo OR "parroquia la ecuatoriana" OR quitumbe OR turubamba)',
        "geolocalizacion": 'geocode:-0.307216,-78.556681,6.75km'
    },
    "tumbaco": {
        "parroquias rurales": '("parroquia checa" OR cumbaya OR "el quinche" OR pifo OR puembo OR yaruqui OR tababela OR tumbaco)',
        "geolocalizacion": 'geocode:-0.205104,-78.305979,20.88km'
    }
}

def run_scraper(query, zone, loc_type):
    payload = {"query": query, "administracionZonal": zone, "tipoZona": loc_type}
    try:
        response = requests.post(api_url, json=payload)
        return response.json()
    except Exception as e:
        return {"error": str(e)}

@st.dialog("confirmar ejecución")
def confirm_dialog(query, zone, loc_type):
    st.write("el siguiente query será enviado al scraper:")
    
    # using text_area instead of code to enable text wrapping
    st.text_area("vista previa del query:", value=query, height=150, disabled=True, label_visibility="collapsed")
    
    st.divider()
    
    col1, col2 = st.columns(2)
    with col1:
        if st.button("aceptar y ejecutar", use_container_width=True):
            with st.spinner("iniciando scraper..."):
                result = run_scraper(query, zone, loc_type)
                if result and "error" in result:
                    st.error(f"error: {result['error']}")
                else:
                    st.success(f"éxito - id: {result.get('run_id')}")
                    time.sleep(2)
                    st.rerun()
    with col2:
        if st.button("cancelar", use_container_width=True):
            st.rerun()

# 2. main ui
st.title("twitter query builder")

# terms logic
st.text_input(
    "escribe un término y presiona enter para añadirlo:", 
    key="new_term_input", 
    on_change=add_custom_term
)

selected_terms = st.multiselect(
    "términos seleccionados:",
    options=st.session_state.term_options,
    default=st.session_state.term_options[:5]
)

# location logic - row 1
col_zone, col_type = st.columns(2)
with col_zone:
    zona = st.selectbox("administración zonal:", options=list(ZONE_DATA.keys()))
with col_type:
    loc_type = st.selectbox("tipo de zona:", options=list(ZONE_DATA[zona].keys()))

# filter logic - row 2
col_source, col_dates = st.columns(2)
with col_source:
    source = st.selectbox("tipo de fuente:", options=["personas (sin links)", "medios (con links)"])
with col_dates:
    date_col1, date_col2 = st.columns(2)
    with date_col1:
        date_since = st.date_input("desde", value=None)
    with date_col2:
        date_until = st.date_input("hasta", value=None)

st.divider()

# trigger button
if st.button("generar y revisar query", use_container_width=True):
    if not selected_terms:
        st.warning("selecciona al menos un término")
    else:
        terms_str = "(" + " OR ".join(selected_terms) + ")"
        location_string = ZONE_DATA[zona][loc_type]
        general_location = "" if loc_type == "geolocalizacion" else "(quito OR pichincha OR ecuador OR uio)"
        source_filter = "-filter:links" if "sin links" in source else "filter:links"
        default_filters = "-futbol -gol -filter:retweets"
        
        date_filter = ""
        if date_since:
            unix_since = int(datetime.combine(date_since, datetime.min.time()).timestamp())
            date_filter += f" since_time:{unix_since}"
        if date_until:
            unix_until = int(datetime.combine(date_until, datetime.max.time()).timestamp())
            date_filter += f" until_time:{unix_until}"
            
        final_query = f"{terms_str} {location_string} {general_location} {default_filters} {source_filter}{date_filter}"
        final_query = " ".join(final_query.split())
        confirm_dialog(final_query, zona, loc_type)