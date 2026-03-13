import psycopg2

conn = psycopg2.connect(
    dbname="database_name",              # replace on local end
    user="database_username",            # replace on local end
    password="your_database_password",   # replace on local end
    host="localhost",
    port="5432"
)

cur = conn.cursor()

with open("../bgg_processed.csv", "r") as f:
    cur.copy_expert(
        "COPY staging_games FROM STDIN WITH CSV HEADER DELIMITER ','",
        f
    )

conn.commit()

cur.close()
conn.close()
