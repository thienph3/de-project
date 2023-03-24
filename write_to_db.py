from sqlalchemy import create_engine


DB_URI = "postgresql://datafeed:datafeed@db/datafeed"
engine = create_engine(DB_URI);

# Write to db
...

df.to_sql(
        "table_name",
        engine, 
        schema      = "schema_name",
        if_exists   = "append");
