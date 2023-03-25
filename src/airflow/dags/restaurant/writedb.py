import psycopg2
import pandas as pd 


conn = psycopg2.connect("host=localhost dbname=dwh user=crawler password=123456" )

cur = conn.cursor()


def insert_db(): 


    cur = conn.cursor()


    truncate_sql = '''DROP TABLE restaurants '''
    cur.execute(truncate_sql)


    cur.execute("""
        CREATE TABLE restaurants(
        name VARCHAR,
        stars VARCHAR,
        review VARCHAR,
        price VARCHAR,
        note VARCHAR, 
        open_time VARCHAR,
        restaurant_type VARCHAR, 
        address VARCHAR, 
        ward VARCHAR, 
        district VARCHAR, 
        city VARCHAR
    )
    """)
    conn.commit()
     
    
    with open(data_path, 'r') as f:
        # Notice that we don't need the csv module.
        next(f) # Skip the header row.
        cur.copy_from(f, 'restaurants', sep='\t')
    
    conn.commit()


if __name__ == "__main__":
    
    data_path = 'dags/restaurant/data.csv'
    df = pd.read_csv(data_path, index_col=False, sep = '\t')
    df = df.loc[:, ~df.columns.isin(['Unnamed: 0'])]
    
    print(df[20:22]) 
    print(df.shape)
    print(df.head(5))
    print(df.columns)
    insert_db()
    

