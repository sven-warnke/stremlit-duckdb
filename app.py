import datetime
import io
import pathlib as pl
import time

import duckdb
import streamlit as st

DATABASE = pl.Path("data/data.duckdb")


@st.cache_resource
def get_db_connection() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(DATABASE))


def handle_user_login(
    username: str, camera_input: io.BytesIO, conn: duckdb.DuckDBPyConnection
) -> None:
    if not username:
        st.error("Invalid username")

    elif not camera_input:
        st.error("Invalid camera input")

    else:
        st.session_state.username = username

        query = "INSERT INTO users VALUES ($username, $image)"
        conn.execute(
            query,
            parameters={
                "username": st.session_state.username,
                "image": camera_input.read(),
            },
        )


def login(conn: duckdb.DuckDBPyConnection) -> None:
    form = st.form(key="login_form")

    with form:
        st.text_input(
            "Username",
            key="input_username",
        )
        st.camera_input("Camera", key="camera_input")

        st.form_submit_button(
            "Login",
            on_click=lambda: handle_user_login(
                st.session_state.input_username,
                st.session_state.camera_input,
                conn=conn,
            ),
        )


def insert_message(conn: duckdb.DuckDBPyConnection, message: str) -> None:
    query = "INSERT INTO messages VALUES ($username, $message, $timestamp)"
    timestamp = datetime.datetime.now()
    conn.execute(
        query,
        parameters={
            "username": st.session_state.username,
            "message": message,
            "timestamp": timestamp,
        },
    )


def chat(conn: duckdb.DuckDBPyConnection) -> None:
    st.write("## Chat")

    st.chat_input(
        "message",
        key="message",
        on_submit=lambda: insert_message(conn, st.session_state.message),
    )

    message_container = st.container(height=300)

    df = (
        conn.execute("""
        SELECT * FROM messages AS m JOIN users AS u on m.username = u.username ORDER BY timestamp DESC LIMIT 10
        """)
        .fetchdf()
        .sort_values(by="timestamp", ascending=True)
    )
    for _, line in df.iterrows():
        message_container.chat_message(
            line["username"], avatar=io.BytesIO(line["image"])
        ).write(line["message"])


def main():
    conn = get_db_connection()
    conn.execute(
        "CREATE TABLE IF NOT EXISTS messages (username VARCHAR, message VARCHAR, timestamp TIMESTAMP)"
    )

    conn.execute("CREATE TABLE IF NOT EXISTS users (username VARCHAR, image BLOB)")
    st.title("DuckDB Streamlit Example")

    if "username" not in st.session_state or not st.session_state.username:
        login(conn)
    else:
        chat(conn)
        time.sleep(10)
        st.rerun()


if __name__ == "__main__":
    main()
