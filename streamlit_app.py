import streamlit as st
from openai import OpenAI
import pandas as pd
from ftplib import FTP
import os



# Título y descripción
st.title("💬 Chatbot con FTP (Credenciales en variables)")
st.write(
    "Este chatbot usa GPT-3.5 e integra informes descargados desde un FTP.\n"
    "**Nota**: En un proyecto real, evita hardcodear credenciales. Usa [Streamlit Secrets]"
    "(https://docs.streamlit.io/streamlit-community-cloud/deploying-apps/connecting-to-data-sources/secrets-management) "
    "o variables de entorno."
)

# Función para descargar reportes CSV desde el FTP y unirlos en un DataFrame
def download_ftp_reports(host, user, password, path="/"):
    ftp = FTP(host)
    ftp.login(user, password)
    ftp.cwd(path)

    files = ftp.nlst()
    dfs = []
    for file in files:
        # Solo descargamos CSV
        if file.lower().endswith(".csv"):
            with open(file, 'wb') as f:
                ftp.retrbinary(f"RETR " + file, f.write)
            df_tmp = pd.read_csv(file)
            dfs.append(df_tmp)
            # Borramos el CSV local
            os.remove(file)

    ftp.quit()

    if dfs:
        return pd.concat(dfs, ignore_index=True)
    else:
        return pd.DataFrame()

# Botón para “Descargar informes” desde el FTP
if st.button("Descargar informes"):
    df = download_ftp_reports(FTP_HOST, FTP_USER, FTP_PASS, FTP_PATH)
    st.session_state["df"] = df
    if not df.empty:
        st.success(f"Descargados {len(df)} registros en total.")
    else:
        st.warning("No se encontraron CSV o el DataFrame quedó vacío.")

# Verifica la API Key
if not OPENAI_API_KEY:
    st.info("Falta la clave de OpenAI. Se detiene la app.")
    st.stop()
else:
    # Creamos el cliente de OpenAI
    client = OpenAI(api_key=OPENAI_API_KEY)

    # Iniciamos la lista de mensajes si no existe
    if "messages" not in st.session_state:
        st.session_state.messages = []

    # Renderizamos los mensajes previos
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    # Campo de entrada del chat (aparece abajo)
    if user_input := st.chat_input("Pregúntale algo sobre los datos..."):

        # Preparamos el texto que enviará el usuario
        df = st.session_state.get("df", pd.DataFrame())
        summary_text = ""
        if not df.empty:
            # Ajusta estas columnas a tu CSV real
            if "cancion" in df.columns and "streams" in df.columns:
                top = (
                    df.groupby("cancion")["streams"]
                    .sum()
                    .sort_values(ascending=False)
                    .head(5)
                )
                summary_text = "Top 5 canciones (según streams):\n"
                for cancion, total in top.items():
                    summary_text += f"- {cancion}: {total}\n"
            else:
                summary_text = "No encontré columnas 'cancion' y 'streams' en el DataFrame."

        # Combinamos el resumen con la pregunta
        user_message = f"{summary_text}\n\nPregunta del usuario: {user_input}"

        # Agregamos el mensaje a la conversación
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

        # Mostramos la respuesta en tiempo real
        with st.chat_message("assistant"):
            response = st.write_stream(stream)

        # Almacena la respuesta
        st.session_state.messages.append({"role": "assistant", "content": response})
