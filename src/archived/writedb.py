import psycopg2
import pandas as pd


conn = psycopg2.connect("host=localhost dbname=dwh user=crawler password=123456")

cur = conn.cursor()


def insert_db():
    cur = conn.cursor()

    truncate_sql = """DROP TABLE restaurants """
    cur.execute(truncate_sql)

    cur.execute(
        """
        CREATE TABLE restaurants(
            id BIGSERIAL PRIMARY KEY,
            name NVARCHAR,
            stars NVARCHAR,
            review NVARCHAR,
            price NVARCHAR,
            note NVARCHAR, 
            open_time NVARCHAR,
            restaurant_type NVARCHAR, 
            address NVARCHAR, 
            ward NVARCHAR, 
            district NVARCHAR, 
            city NVARCHAR
        )
    """

    )
    conn.commit()

    with open(data_path, "r") as f:
        # Notice that we don't need the csv module.
        next(f) # Skip the header row.
        cur.copy_from(f, 'restaurants', sep='\t')

    conn.commit()

    cur.execute(""" select * from restaurants LIMIT 10;""")
    conn.commit()


if __name__ == "__main__":
    data_path = 'dags/restaurant/data.csv'
    
    insert_db()
