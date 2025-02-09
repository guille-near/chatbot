import streamlit as st
import ssl
from openai import OpenAI
import pandas as pd
import os
from ftplib import FTP_TLS

# ------------------------------------------------------------------------
# 1) Lectura de credenciales desde secrets
# ------------------------------------------------------------------------
OPENAI_API_KEY = st.secrets["OPENAI_API_KEY"]
FTP_HOST = st.secrets["FTP_HOST"]
FTP_USER = st.secrets["FTP_USER"]
FTP_PASS = st.secrets["FTP_PASS"]
DIRECTORY_PATH = st.secrets["DIRECTORY_PATH"]  # Ej: "/trends/202502/spo-spotify/weekly-prrt"

# ------------------------------------------------------------------------
# 2) Función para conectar con AUTH SSL en lugar de AUTH TLS
# ------------------------------------------------------------------------
def connect_ftps_auth_ssl(host, user, password, port=21):
    """
    Conecta vía FTP_TLS a 'host':21, 
    evita la llamada AUTH TLS por defecto y fuerza AUTH SSL.
    Desactiva la verificación de certificado y fuerza TLS 1.2.
    """
    # Forzamos TLS 1.2, sin verificar el cert (para descartar problemas).
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    # Creamos el objeto FTP_TLS con ese contexto
    ftp = FTP_TLS(context=ctx)

    # Conectamos al puerto 21
    ftp.connect(host, port)

    # Llamamos login con secure=False para que no haga AUTH TLS automático
    ftp.login(user=user, passwd=password, secure=False)

    # Forzamos AUTH SSL
    ftp.voidcmd('AUTH SSL')

    # Protegemos la transferencia de datos
    ftp.prot_p()
    return ftp

# ------------------------------------------------------------------------
# 3) Función para descargar CSV de un directorio fijo (sin subcarpetas)
# ------------------------------------------------------------------------
def download_ftp_reports_ssl(host, user, password, directory):
    """
    1) Conecta usando connect_ftps_auth_ssl (AUTH SSL).
    2) Entra al directorio especificado.
    3) Descarga archivos .csv, concatena en un DF y lo regresa.
    """
    ftp = connect_ftps_auth_ssl(host, user, password)

    # Ir a la carpeta deseada
    ftp.cwd(directory)

    files = ftp.nlst()
    dfs = []

    for filename in files:
        if filename.lower().endswith(".csv"):
            with open(filename, 'wb') as local_file:
                ftp.retrbinary(f"RETR {filename}", local_file.write)

            df_tmp = pd.read_csv(filename)
            dfs.append(df_tmp)

            os.remove(filename)

    ftp.quit()

    if dfs:
        return pd.concat(dfs, ignore_index=True)
    else:
        return pd.DataFrame()

# ------------------------------------------------------------------------
# 4) Interfaz de Streamlit
# ------------------------------------------------------------------------
st.title("💬 Chatbot con FTPS Explicito (AUTH SSL) – Carpeta Fija")
st.write("""
Esta app intenta conectarse a un servidor FTP con TLS (puerto 21), 
usando **AUTH SSL** en vez de AUTH TLS, y desactiva la verificación de certificado.
Descarga los archivos .csv de la carpeta que definas en `DIRECTORY_PATH`.
""")

# Botón "Descargar informes"
if st.button("Descargar informes"):
    df = download_ftp_reports_ssl(FTP_HOST, FTP_USER, FTP_PASS, DIRECTORY_PATH)
    st.session_state["df"] = df

    if not df.empty:
        st.success(f"Descargados {len(df)} registros en total.")
    else:
        st.warning("No se encontraron CSV o el DataFrame quedó vacío.")

# ------------------------------------------------------------------------
# 5) Verificamos la API Key de OpenAI y montamos el chat
# ------------------------------------------------------------------------
if not OPENAI_API_KEY:
    st.info("Falta la clave de OpenAI en secrets. Revisa tu `.streamlit/secrets.toml`.", icon="🗝️")
    st.stop()
else:
    client = OpenAI(api_key=OPENAI_API_KEY)

    if "messages" not in st.session_state:
        st.session_state.messages = []

    # Renderizar mensajes previos (user / assistant)
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    # Campo de texto para preguntar
    if user_input := st.chat_input("Pregúntale algo sobre los datos..."):
        df = st.session_state.get("df", pd.DataFrame())
        summary_text = ""

        # Pequeño resumen si las columnas son "cancion" y "streams"
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

        # Unir el resumen con la pregunta
        user_message = f"{summary_text}\n\nPregunta del usuario: {user_input}"

        # Añadir al historial
        st.session_state.messages.append({"role": "user", "content": user_message})
        with st.chat_message("user"):
            st.markdown(user_message)

        # Llamar a la API de OpenAI (modelo gpt-3.5-turbo) con streaming
        stream = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": m["role"], "content": m["content"]}
                for m in st.session_state.messages
            ],
            stream=True,
        )

        # Mostrar la respuesta en tiempo real
        with st.chat_message("assistant"):
            response = st.write_stream(stream)

        # Guardar la respuesta en el historial
        st.session_state.messages.append({"role": "assistant", "content": response})
