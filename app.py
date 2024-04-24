import pathlib as pl

import duckdb
import streamlit as st
import datetime

DATABASE = pl.Path("data/data.duckdb")


@st.cache_resource
def get_db_connection():
    return duckdb.connect(str(DATABASE))


def handle_username(username: str) -> None:
    if not username:
        st.error("Invalid username")

    else:
        st.session_state.username = username


def login() -> None:
    st.text_input(
        "Username",
        key="input_username",
        on_change=lambda: handle_username(st.session_state.input_username),
    )


def chat() -> None:
    conn = get_db_connection()
    conn.execute(
        "CREATE TABLE IF NOT EXISTS messages (username VARCHAR, message VARCHAR, timestamp TIMESTAMP)"
    )

    st.write("## Chat")

    message = st.chat_input("message")
    timestamp = datetime.datetime.now()

    message_container = st.container(height=300)

    if message:
        query = "INSERT INTO messages VALUES ($username, $message, $timestamp)"
        conn.execute(
            query,
            parameters={
                "username": st.session_state.username,
                "message": message,
                "timestamp": timestamp,
            },
        ).fetchdf()
        df = conn.execute("SELECT * FROM messages").fetchdf()
        for _, line in df.iterrows():
            message_container.chat_message(line["username"]).write(line["message"])


def main():
    st.title("DuckDB Streamlit Example")

    if "username" not in st.session_state or not st.session_state.username:
        login()
    else:
        chat()


if __name__ == "__main__":
    main()
