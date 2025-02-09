import streamlit as st
import ssl
from openai import OpenAI
import pandas as pd
import os
from ftplib import FTP_TLS

# -----------------------------------------------------------------------------
# 1) LECTURA DE CREDENCIALES DESDE SECRETS
# -----------------------------------------------------------------------------
OPENAI_API_KEY = st.secrets["OPENAI_API_KEY"]
FTP_HOST = st.secrets["FTP_HOST"]
FTP_USER = st.secrets["FTP_USER"]
FTP_PASS = st.secrets["FTP_PASS"]
FTP_PATH = st.secrets["FTP_PATH"]  # Ej: "/trends"

# -----------------------------------------------------------------------------
# 2) FUNCIÓN PARA CONECTARSE VÍA FTPS (EXPLÍCITO) Y DESCARGAR CSV DE SUBCARPETAS 2025xx
# -----------------------------------------------------------------------------
def download_ftp_reports_tls(host, user, password, base_path="/trends"):
    """
    Se conecta a un servidor FTPS (modo explícito, en puerto 21), 
    usando TLS 1.2 y sin verificar el certificado (ctx.verify_mode=CERT_NONE).
    Luego:
      - Entra a 'base_path'
      - Busca subcarpetas con 6 dígitos que empiecen con '2025'
      - Descarga los .csv en cada subcarpeta, concatena en un DataFrame
      - Retorna el DF final
    """

    # Creamos un SSLContext que fuerce TLS 1.2 y desactive verificación de certificado
    # (para evitar handshake failures con servidores que no tengan certificado válido).
    # En producción, deberías ajustar esto para verificar el cert.
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    # Creamos el objeto FTP_TLS con ese contexto
    ftp = FTP_TLS(context=ctx)
    # Conectamos al puerto 21 (FTP explícito). Si tu server usa puerto distinto, ajusta aquí.
    ftp.connect(host, 21)

    # Login
    ftp.login(user, password)
    # Asegura que la transferencia de datos también vaya cifrada
    ftp.prot_p()

    # Entramos a la ruta base
    ftp.cwd(base_path)

    # Listamos items en base_path (archivos y/o carpetas)
    items = ftp.nlst()

    # Filtramos subcarpetas tipo 2025xx
    subdirs = []
    for item in items:
        # 6 dígitos, empieza con '2025'
        if len(item) == 6 and item.isdigit() and item.startswith("2025"):
            try:
                # Verificar que sea carpeta (cwd no falla)
                ftp.cwd(item)
                ftp.cwd("..")
                subdirs.append(item)
            except:
                pass

    # Para cada subcarpeta, bajamos CSV
    dfs = []
    for subdir in subdirs:
        ftp.cwd(subdir)
        files_in_subdir = ftp.nlst()

        for filename in files_in_subdir:
            if filename.lower().endswith(".csv"):
                # Descargamos localmente
                with open(filename, 'wb') as local_file:
                    ftp.retrbinary(f"RETR {filename}", local_file.write)
                # Leemos al DF
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
# 3) INTERFAZ PRINCIPAL DE STREAMLIT
# -----------------------------------------------------------------------------
st.title("💬 Chatbot con FTPS (Explícito, TLS) – Subcarpetas 2025xx")
st.write("""
Esta app se conecta vía **FTPS en puerto 21** (modo explícito) a tu servidor,
descarga las subcarpetas con formato 2025xx y concatena los CSV en un DataFrame.
Luego, ChatGPT (GPT-3.5) responderá tus preguntas con un pequeño resumen.
**Nota**: el SSLContext actual ignora el certificado, útil para descartar problemas de handshake.
""")

# Botón: "Descargar informes"
if st.button("Descargar informes"):
    df = download_ftp_reports_tls(FTP_HOST, FTP_USER, FTP_PASS, FTP_PATH)
    st.session_state["df"] = df

    if not df.empty:
        st.success(f"Descargados {len(df)} registros en total.")
    else:
        st.warning("No se encontraron CSV (DataFrame vacío) o no hay subcarpetas 2025xx.")

# -----------------------------------------------------------------------------
# 4) VERIFICAMOS CLAVE DE OPENAI Y MONTAMOS EL CHAT
# -----------------------------------------------------------------------------
if not OPENAI_API_KEY:
    st.info("Falta la clave de OpenAI en secrets. Revisa tu `.streamlit/secrets.toml`.", icon="🗝️")
    st.stop()
else:
    # Creamos cliente de OpenAI
    client = OpenAI(api_key=OPENAI_API_KEY)

    # Manejo del historial de mensajes
    if "messages" not in st.session_state:
        st.session_state.messages = []

    # Renderizamos mensajes previos
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    # Campo para la nueva pregunta
    if user_input := st.chat_input("Pregúntale algo sobre los datos..."):
        df = st.session_state.get("df", pd.DataFrame())
        summary_text = ""

        # Generamos un resumen si existen columnas "cancion" y "streams"
        if not df.empty:
            if "cancion" in df.columns and "streams" in df.columns:
                top = (
                    df.groupby("cancion")["streams"]
                    .sum()
                    .sort_values(ascending=False)
                    .head(5)
                )
                summary_text = "Top 5 canciones (por streams):\n"
                for c, s in top.items():
                    summary_text += f"- {c}: {s}\n"
            else:
                summary_text = (
                    "No encontré columnas 'cancion' y 'streams' en el DataFrame.\n"
                    "Ajusta la lógica de resumen según tus columnas reales."
                )

        # Unimos el resumen con la pregunta del usuario
        user_message = f"{summary_text}\n\nPregunta del usuario: {user_input}"

        # Añadimos el mensaje al historial
        st.session_state.messages.append({"role": "user", "content": user_message})
        with st.chat_message("user"):
            st.markdown(user_message)

        # Llamamos a la API de OpenAI con streaming
        stream = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": m["role"], "content": m["content"]}
                for m in st.session_state.messages
            ],
            stream=True,
        )

        # Mostramos la respuesta en tiempo real
        with st.chat_message("assistant"):
            response = st.write_stream(stream)

        # Guardamos la respuesta en el historial
        st.session_state.messages.append({"role": "assistant", "content": response})
