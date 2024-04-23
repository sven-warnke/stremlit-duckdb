import pathlib as pl

import duckdb
import streamlit as st

DATABASE = pl.Path("data.duckdb")


@st.cache_resource
def get_db_connection():
    return duckdb.connect(str(DATABASE))


def main():
    st.title("DuckDB Streamlit Example")

    conn = get_db_connection()

    st.write("## Table")
    st.write(conn.execute("SELECT * FROM data").fetchdf())

    st.write("## Query")
    query = st.text_area("Query", "SELECT * FROM data")
    st.write(conn.execute(query).fetchdf())

    st.write("## SQL Editor")
    query = st.text_area("SQL Editor", "SELECT * FROM data")
    if st.button("Run"):
        st.write(conn.execute(query).fetchdf())


if __name__ == "__main__":
    main()
