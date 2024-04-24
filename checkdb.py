import duckdb
import app


conn = duckdb.connect(str(app.DATABASE))

df = conn.execute("""
            SELECT * FROM messages AS m JOIN users AS u on m.username = u.username ORDER BY timestamp DESC LIMIT 10
            """).fetch_df()

print(df)

# image = df.image[0]
#
# with open("image.jpg", "wb") as f:
#     f.write(image)
