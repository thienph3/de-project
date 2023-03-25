import os 
import pandas as pd 
 # data structure: city >> district >> ward 
crawl_path = 'dags/restaurant/data/'
data_path = 'dags/restaurant/data.csv'

def read_folder(crawl_path, save_path):
    df = pd.DataFrame()
    cities = [i for i in os.listdir(crawl_path) \
            if not i.startswith('.')] 

    for city in cities:
        districts = [i for i in os.listdir(os.path.join(crawl_path, city)) \
                if not i.startswith('.')]

        for district in districts:
            wards = [i for i in os.listdir(os.path.join(crawl_path, city, district)) \
                if not i.startswith('.')]

            for ward in wards:
                ward_path = os.path.join(crawl_path, city, district, ward)
                df_ward = pd.read_csv(ward_path)
                df['ward'] = ward.split('.csv')[0]
                df['district'] = district 
                df['city'] = city
                if df.shape[0] > 0:
                    df = pd.concat([df, df_ward], ignore_index=True, sort=False)
                else:
                    df = df_ward
    

    
#    df = df[['name', 'stars', 'review', 'price', 'note', 'open_time',
#      'restautant_type', 'address']]
    df['note'] = df['note'].apply(lambda x: '; '.join(
        x.lstrip('[').rstrip(']').split(',')))
    df['open_time'] = df['open_time'].apply(lambda x: '; '.join(
        x.lstrip('[').rstrip(']').split(',')))
    
    df = df.drop_duplicates()
    print(df.shape)

    df = df.loc[:, ~df.columns.isin(['Unnamed: 0'])]
    print(df.head(3))
    df.to_csv(data_path, index=False, sep ='\t')
    print("complete saving")


if __name__ == "__main__":
    read_folder(crawl_path, data_path)
    
