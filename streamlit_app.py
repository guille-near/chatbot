import streamlit as st
from openai import OpenAI
import pandas as pd
import os
from ftplib import FTP

# -----------------------------------------------------------------------------
# 1) LECTURA DE CREDENCIALES DESDE SECRETS
# -----------------------------------------------------------------------------

# Clave de OpenAI
OPENAI_API_KEY = st.secrets["OPENAI_API_KEY"]

# Credenciales y path base del FTP
FTP_HOST = st.secrets["FTP_HOST"]
FTP_USER = st.secrets["FTP_USER"]
FTP_PASS = st.secrets["FTP_PASS"]
FTP_PATH = st.secrets["FTP_PATH"]  # por ej. "/trends"

# -----------------------------------------------------------------------------
# 2) FUNCIÓN PARA DESCARGAR CSV DE SUBCARPETAS YYYYMM (ej. 202501, 202502...)
# -----------------------------------------------------------------------------

def download_ftp_reports(host, user, password, base_path="/trends"):
    """
    1. Se conecta al FTP, entra a 'base_path'.
    2. Busca subdirectorios cuyo nombre tenga 6 dígitos y empiece por '2025'.
    3. En cada subcarpeta, descarga archivos CSV y los concatena en un DataFrame.
    4. Retorna el DF final con todos los datos.
    """
    ftp = FTP(host)
    ftp.login(user, password)

    # Nos ubicamos en /trends o lo que tengas en FTP_PATH
    ftp.cwd(base_path)

    # Listamos todo lo que hay (archivos + carpetas)
    items = ftp.nlst()

    # Filtramos subcarpetas con formato 2025xx (6 dígitos, empieza con "2025")
    subdirs = []
    for item in items:
        if len(item) == 6 and item.isdigit() and item.startswith("2025"):
            # Verificamos que sea realmente un directorio
            try:
                ftp.cwd(item)   # si no da error, es carpeta
                ftp.cwd("..")   
                subdirs.append(item)
            except:
                pass

    # Acumulamos data
    dfs = []

    # Recorremos cada subcarpeta
    for subdir in subdirs:
        ftp.cwd(subdir)
        # Listamos archivos dentro de esa subcarpeta
        files_in_subdir = ftp.nlst()

        for filename in files_in_subdir:
            # Descargamos solo CSV
            if filename.lower().endswith(".csv"):
                with open(filename, 'wb') as local_file:
                    ftp.retrbinary(f"RETR {filename}", local_file.write)

                # Leemos con pandas
                df_temp = pd.read_csv(filename)
                dfs.append(df_temp)

                # Borramos el archivo local
                os.remove(filename)

        # Volvemos a la carpeta padre (base_path)
        ftp.cwd("..")

    ftp.quit()

    if dfs:
        return pd.concat(dfs, ignore_index=True)
    else:
        return pd.DataFrame()

# -----------------------------------------------------------------------------
# 3) INTERFAZ DE USUARIO - STREAMLIT
# -----------------------------------------------------------------------------

st.title("💬 Chatbot con FTP y Subcarpetas AÑO-MES (2025xx)")
st.write("""
Este chatbot usa GPT-3.5 e integra informes descargados desde un FTP.  
**Nota**: Se navega dentro de `base_path` (por defecto `/trends`), buscando subcarpetas `2025xx`.
""")

# Botón para “Descargar informes”
if st.button("Descargar informes"):
    # Llamamos a la función para descargar CSV de subcarpetas
    df = download_ftp_reports(FTP_HOST, FTP_USER, FTP_PASS, FTP_PATH)
    st.session_state["df"] = df

    if not df.empty:
        st.success(f"¡Descargados {len(df)} registros en total!")
    else:
        st.warning("No se encontraron CSV (DataFrame vacío) o no hay subcarpetas 2025xx.")

# -----------------------------------------------------------------------------
# 4) VERIFICAMOS QUE TENGAMOS LA CLAVE DE OPENAI
# -----------------------------------------------------------------------------

if not OPENAI_API_KEY:
    st.info("Falta la clave de OpenAI en secrets. Revisa tu `.streamlit/secrets.toml`.", icon="🗝️")
    st.stop()
else:
    # Creamos el cliente de OpenAI con la clave
    client = OpenAI(api_key=OPENAI_API_KEY)

    # Manejamos el historial de mensajes del chat
    if "messages" not in st.session_state:
        st.session_state.messages = []

    # Renderizamos mensajes previos (usuario y assistant)
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    # Campo de entrada para la pregunta
    if user_input := st.chat_input("Pregúntale algo sobre los datos..."):

        # Obtenemos el DataFrame
        df = st.session_state.get("df", pd.DataFrame())

        # Generamos un pequeño resumen si hay datos
        summary_text = ""
        if not df.empty:
            # Ajusta las columnas según tu CSV real
            if "cancion" in df.columns and "streams" in df.columns:
                top = (
                    df.groupby("cancion")["streams"]
                    .sum()
                    .sort_values(ascending=False)
                    .head(5)
                )
                summary_text = "Top 5 canciones (por streams):\n"
                for cancion, total in top.items():
                    summary_text += f"- {cancion}: {total}\n"
            else:
                summary_text = (
                    "No encontré columnas 'cancion' y 'streams' en el DataFrame. "
                    "Ajusta la lógica según tus columnas reales."
                )

        # Combinamos el resumen con la pregunta
        user_message = f"{summary_text}\n\nPregunta del usuario: {user_input}"

        # Añadimos el mensaje del usuario al historial
        st.session_state.messages.append({"role": "user", "content": user_message})
        with st.chat_message("user"):
            st.markdown(user_message)

        # Llamamos a la API de OpenAI en modo streaming
        stream = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": msg["role"], "content": msg["content"]}
                for msg in st.session_state.messages
            ],
            stream=True,
        )

        # Mostramos la respuesta a medida que se genera
        with st.chat_message("assistant"):
            response = st.write_stream(stream)

        # Guardamos la respuesta en el historial
        st.session_state.messages.append({"role": "assistant", "content": response})
