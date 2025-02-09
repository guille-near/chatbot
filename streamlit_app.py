import streamlit as st
from openai import OpenAI
import pandas as pd
import os
import ssl
from ftplib import FTP_TLS

# ------------------------------------------------------------------------
# 1) Lectura de credenciales desde secrets
# ------------------------------------------------------------------------
OPENAI_API_KEY = st.secrets["OPENAI_API_KEY"]
FTP_HOST = st.secrets["FTP_HOST"]
FTP_USER = st.secrets["FTP_USER"]
FTP_PASS = st.secrets["FTP_PASS"]
DIRECTORY_PATH = st.secrets["DIRECTORY_PATH"]  # "/trends/202502/spo-spotify/weekly-prrt"

# ------------------------------------------------------------------------
# 2) Función para descargar CSV desde la carpeta DIRECTORY_PATH (sin subcarpetas)
# ------------------------------------------------------------------------
def download_ftp_reports_tls(host, user, password, directory):
    """
    1) Conecta a un servidor FTPS (puerto 21, modo explícito) usando TLS 1.2 
       y desactivando verificación de cert (para evitar handshake failures).
    2) Entra a 'directory' (ej: /trends/202502/spo-spotify/weekly-prrt).
    3) Descarga archivos .csv, los lee con pandas, concatena en un DataFrame.
    4) Retorna el DataFrame final (o vacío si no encuentra CSV).
    """
    # Forzamos TLS 1.2 y desactivamos verificación de certificado
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    ftp = FTP_TLS(context=ctx)
    ftp.connect(host, 21)       # Modo explícito, puerto 21
    ftp.login(user, password)
    ftp.prot_p()                # Protege la transferencia de datos

    # Nos ubicamos en el directorio fijo
    ftp.cwd(directory)

    # Listamos los archivos
    files = ftp.nlst()

    dfs = []
    for filename in files:
        if filename.lower().endswith(".csv"):
            # Descargamos localmente
            with open(filename, 'wb') as local_file:
                ftp.retrbinary(f"RETR {filename}", local_file.write)
            # Leemos con pandas
            df_temp = pd.read_csv(filename)
            dfs.append(df_temp)
            # Borramos el archivo local
            os.remove(filename)

    ftp.quit()

    if dfs:
        return pd.concat(dfs, ignore_index=True)
    else:
        return pd.DataFrame()

# ------------------------------------------------------------------------
# 3) Interfaz principal de Streamlit
# ------------------------------------------------------------------------
st.title("💬 Chatbot con FTPS – Carpeta fija /trends/202502/spo-spotify/weekly-prrt")
st.write("""
Esta app se conecta vía **FTPS (puerto 21, modo explícito)** a la ruta 
`/trends/202502/spo-spotify/weekly-prrt`, descarga los CSV 
y concatena todo en un DataFrame. Luego, un chatbot (GPT-3.5) 
puede responder preguntas basándose en esos datos.
""")

# Botón "Descargar informes"
if st.button("Descargar informes"):
    df = download_ftp_reports_tls(FTP_HOST, FTP_USER, FTP_PASS, DIRECTORY_PATH)
    st.session_state["df"] = df

    if not df.empty:
        st.success(f"Descargados {len(df)} registros en total.")
    else:
        st.warning("No se encontraron CSV o el DataFrame quedó vacío.")

# ------------------------------------------------------------------------
# 4) Verificamos la API Key de OpenAI y montamos el chat
# ------------------------------------------------------------------------
if not OPENAI_API_KEY:
    st.info("Falta la clave de OpenAI en secrets. Revisa tu `.streamlit/secrets.toml`.", icon="🗝️")
    st.stop()
else:
    # Cliente de OpenAI
    client = OpenAI(api_key=OPENAI_API_KEY)

    # Manejo del historial de mensajes
    if "messages" not in st.session_state:
        st.session_state.messages = []

    # Renderizamos los mensajes previos
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    # Campo para la nueva pregunta
    if user_input := st.chat_input("Pregúntale algo sobre los datos..."):
        df = st.session_state.get("df", pd.DataFrame())
        summary_text = ""

        # Generamos un resumen si detecta columnas "cancion" y "streams"
        if not df.empty:
            if "cancion" in df.columns and "streams" in df.columns:
                top = df.groupby("cancion")["streams"].sum().sort_values(ascending=False).head(5)
                summary_text = "Top 5 canciones (por streams):\n"
                for c, s in top.items():
                    summary_text += f"- {c}: {s}\n"
            else:
                summary_text = (
                    "No encontré columnas 'cancion' y 'streams' en el DataFrame.\n"
                    "Ajusta la lógica según tus columnas."
                )

        # Combinamos el resumen con la pregunta
        user_message = f"{summary_text}\n\nPregunta del usuario: {user_input}"

        # Añadimos el mensaje al historial
        st.session_state.messages.append({"role": "user", "content": user_message})
        with st.chat_message("user"):
            st.markdown(user_message)

        # Llamada a la API de OpenAI con streaming
        stream = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": m["role"], "content": m["content"]} for m in st.session_state.messages],
            stream=True,
        )

        # Mostramos la respuesta en tiempo real
        with st.chat_message("assistant"):
            response = st.write_stream(stream)

        # Guardamos la respuesta
        st.session_state.messages.append({"role": "assistant", "content": response})
