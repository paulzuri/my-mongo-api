import streamlit as st
import pandas as pd

# --- data configuration ---
main_query = '(extorsion OR robo OR sicariato OR asalto OR delincuencia OR inseguridad OR secuestro OR delito OR violencia OR criminal OR delincuentes OR "lugar seguro" OR seguridad OR tranquilo OR tranquilidad OR protegido OR paz OR calma OR policía OR patrullaje OR guardia)'

zone_data = {
    "calderon": {
        "parroquias rurales": '("parroquia de calderon" OR "llano chico")',
        "geolocalizacion": 'geocode:-0.084717,-78.426257,6.87km'
    },
    "eloy alfaro": {
        "parroquias urbanas": '(chilibulo OR solanda OR "la magdalena" OR "san bartolo" OR "la ferroviaria" OR chimbacalle OR "la mena" OR "la argelia") (quito OR pichincha OR ecuador OR uio)',
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
        "parroquias urbanas": '("centro histórico de quito" OR "parroquia la libertad" or "parroquia san juan" OR itchimbia OR puengasi)',
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

filters = '-futbol -gol -filter:retweets'
origin_filters = {"medios": "filter:links", "personas": "-filter:links"}

# --- build the dataset ---
rows = []
for zone, content in zone_data.items():
    for z_type, z_val in content.items():
        for origin, o_filter in origin_filters.items():
            rows.append({
                "administracion_zonal": zone,
                "origen_datos": origin,
                "tipo_query": "geolocalizacion" if z_type == "geolocalizacion" else "palabras clave",
                "tipo_zona": z_type,
                "query_generado": f"{main_query} {z_val} {filters} {o_filter}"
            })

df = pd.DataFrame(rows)

# --- streamlit layout ---
st.set_page_config(page_title="visor de queries", layout="wide")

st.title("lista de queries generadas")

# display as an interactive dataframe
# use_container_width makes it fill the screen
st.dataframe(
    df, 
    use_container_width=True,
    column_config={
        "query_generado": st.column_config.TextColumn("query generado", width="large")
    }
)

# optional: download button for the generated list
json_data = df.to_json(orient='records', indent=4, force_ascii=False)
st.download_button(
    label="descargar todas como json",
    data=json_data,
    file_name="queries_seguridad_quito.json",
    mime="application/json",
)