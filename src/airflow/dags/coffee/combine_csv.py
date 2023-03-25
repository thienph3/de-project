import os 
import pandas as pd 
 # data structure: city >> district >> ward 
data_path = 'dags/restaurant/data/'

def read_folder(data_path):
    df = pd.DataFrame()
    cities = [i for i in os.listdir(data_path) \
            if not i.startswith('.')] 

    for city in cities:
        districts = [i for i in os.listdir(os.path.join(data_path, city)) \
                if not i.startswith('.')]

        for district in districts:
            wards = [i for i in os.listdir(os.path.join(data_path, city, district)) \
                if not i.startswith('.')]

            for ward in wards:
                ward_path = os.path.join(data_path, city, district, ward)
                df_ward = pd.read_csv(ward_path)
                if df.shape[0] > 0:
                    df = pd.concat([df, df_ward], ignore_index=True, sort=False)
                else:
                    df = df_ward
    print(df.head())
    print(df.shape)

if __name__ == "__main__":
    read_folder(data_path)
    
