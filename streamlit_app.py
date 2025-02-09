import streamlit as st
from openai import OpenAI
import pandas as pd
import os
from ftplib import FTP_TLS

# ------------------------------------------------------------------------
# 1) Lectura de credenciales desde Streamlit Secrets
# ------------------------------------------------------------------------
OPENAI_API_KEY = st.secrets["OPENAI_API_KEY"]
FTP_HOST = st.secrets["FTP_HOST"]
FTP_USER = st.secrets["FTP_USER"]
FTP_PASS = st.secrets["FTP_PASS"]
FTP_PATH = st.secrets["FTP_PATH"]  # e.g. "/trends"

# ------------------------------------------------------------------------
# 2) Función para conectarse vía FTPS y descargar subcarpetas 2025xx
# ------------------------------------------------------------------------
def download_ftp_reports_tls(host, user, password, base_path="/trends"):
    """
    - Conecta al servidor FTPS (modo explícito) en puerto 21.
    - Hace login con user/password.
    - Llama a prot_p() para cifrar la transferencia de datos.
    - Entra a 'base_path', identifica subcarpetas '2025xx' (6 dígitos, inicia "2025").
    - Descarga CSV, los lee con pandas, concatena en un DataFrame.
    - Retorna el DF final o vacío si no hay archivos.
    """
    ftp = FTP_TLS()             # Creamos objeto FTP_TLS
    ftp.connect(host, 21)       # Si tu server usa puerto distinto, cámbialo aquí
    ftp.login(user, password)   # Autenticación
    ftp.prot_p()                # Activa transferencia de datos cifrada

    # Vamos a la carpeta base (por defecto "/trends")
    ftp.cwd(base_path)

    # Listamos todo (subcarpetas / archivos) en base_path
    items = ftp.nlst()

    # Filtramos subcarpetas con formato 2025xx
    subdirs = []
    for item in items:
        # 6 dígitos, todos numéricos, inicia con "2025"
        if len(item) == 6 and item.isdigit() and item.startswith("2025"):
            try:
                # Verificamos que sea carpeta (cd ok)
                ftp.cwd(item)
                ftp.cwd("..")
                subdirs.append(item)
            except:
                pass

    dfs = []
    # Recorremos cada subcarpeta 2025xx
    for subdir in subdirs:
        ftp.cwd(subdir)
        files_in_subdir = ftp.nlst()

        for filename in files_in_subdir:
            if filename.lower().endswith(".csv"):
                # Descargamos CSV localmente
                with open(filename, 'wb') as local_file:
                    ftp.retrbinary(f"RETR {filename}", local_file.write)

                # Leemos en DataFrame
                df_temp = pd.read_csv(filename)
                dfs.append(df_temp)

                # Borramos el archivo local tras usarlo
                os.remove(filename)

        # Volvemos a la carpeta padre (base_path)
        ftp.cwd("..")

    ftp.quit()

    # Unimos DataFrames
    if dfs:
        return pd.concat(dfs, ignore_index=True)
    else:
        return pd.DataFrame()

# ------------------------------------------------------------------------
# 3) Interfaz principal de Streamlit
# ------------------------------------------------------------------------
st.title("💬 Chatbot con FTP con TLS (FTPS)")
st.write("""
Esta app se conecta vía FTPS (modo explícito) a tu servidor FTP,
descarga subcarpetas con formato 2025xx, concatena los CSV y te permite 
consultarlos a través de GPT-3.5.
""")

# Botón para descargar informes y guardarlos en st.session_state["df"]
if st.button("Descargar informes"):
    df = download_ftp_reports_tls(FTP_HOST, FTP_USER, FTP_PASS, FTP_PATH)
    st.session_state["df"] = df

    if not df.empty:
        st.success(f"Descargados {len(df)} registros en total.")
    else:
        st.warning("No se encontraron CSV en subcarpetas 2025xx o el DF quedó vacío.")

# ------------------------------------------------------------------------
# 4) Verificamos clave de OpenAI y montamos el chat
# ------------------------------------------------------------------------
if not OPENAI_API_KEY:
    st.info("Falta la clave de OpenAI en secrets. Revisa tu `.streamlit/secrets.toml`.", icon="🗝️")
    st.stop()
else:
    client = OpenAI(api_key=OPENAI_API_KEY)

    if "messages" not in st.session_state:
        st.session_state.messages = []

    # Mostrar el historial
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    # Campo para la nueva pregunta
    if user_input := st.chat_input("Pregúntale algo sobre los datos..."):
        df = st.session_state.get("df", pd.DataFrame())
        summary_text = ""

        # Generamos un resumen si hay DF y las columnas "cancion" y "streams"
        if not df.empty:
            if "cancion" in df.columns and "streams" in df.columns:
                top = df.groupby("cancion")["streams"].sum().sort_values(ascending=False).head(5)
                summary_text = "Top 5 canciones (por streams):\n"
                for c, s in top.items():
                    summary_text += f"- {c}: {s}\n"
            else:
                summary_text = (
                    "No encontré columnas 'cancion' y 'streams' en el DataFrame.\n"
                    "Ajusta la lógica según tus columnas reales."
                )

        # Combinamos resumen + pregunta del usuario
        user_message = f"{summary_text}\n\nPregunta del usuario: {user_input}"

        # Añadimos el mensaje al historial
        st.session_state.messages.append({"role": "user", "content": user_message})
        with st.chat_message("user"):
            st.markdown(user_message)

        # Llamada a la API de OpenAI en modo streaming
        stream = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": m["role"], "content": m["content"]} for m in st.session_state.messages],
            stream=True,
        )

        # Mostramos respuesta en tiempo real
        with st.chat_message("assistant"):
            response = st.write_stream(stream)

        # Guardamos la respuesta en el historial
        st.session_state.messages.append({"role": "assistant", "content": response})
