import pathlib as pl

import duckdb
import streamlit as st

import plotly.express as px

DATABASE = pl.Path("data/data.duckdb")


@st.cache_resource
def get_db_connection():
    return duckdb.connect(str(DATABASE))


def main():
    st.title("DuckDB Streamlit Example")

    conn = get_db_connection()
    conn.execute("CREATE TABLE IF NOT EXISTS data (a INTEGER, b VARCHAR)")

    st.write("## Table")
    st.write(conn.execute("SELECT * FROM data").fetchdf())

    st.write("## SQL Editor")
    query = st.text_area("SQL Editor", "SELECT * FROM data")
    if st.button("Run"):
        st.write(conn.execute(query).fetchdf())
        df = conn.execute("SELECT * FROM DATA").fetchdf()

        st.plotly_chart(px.scatter(df, x='a', y='b'))




if __name__ == "__main__":
    main()
