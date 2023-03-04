import vnstock
from datetime import date
import pandas as pd
import time

from sqlalchemy import create_engine


DB_URL = "sqlite+pysqlite:///:memory:";

def load_stock_price(
        sym             : str,
        start_date      : str,
        end_date        : str) -> pd.DataFrame:

    
    df = vnstock.stock_historical_data(
            sym,
            start_date  = start_date    ,
            end_date    = end_date      )

    df["symbol"]        = sym;
    df["created_at"]    = time.time();

    # Write to db
    engine              = create_engine(
            DB_URL,
            echo        = True,
            future      = True,)

    df.to_sql(
            "daily_stock_price",
            con         = engine.connect());
    return 0;





if __name__ == "__main__":
    _start  = str(date(2023,1,1));
    _end    = str(date(2023,3,1));
    _sym    = "GMD";

    exit_status = load_stock_price(_sym, _start, _end);


