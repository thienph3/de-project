import os 
import pandas as pd 
from search import search


def ward_district_search_term(city = None):
    root = './dags/restaurant'
    location_path = root + '/cfg/city_district_ward.csv' 
    data = pd.read_csv(location_path)
    cities = data['CityName'].unique()
    assert city in cities, 'can not find this city name'

    df = data.query("CityName == '{}' and Level == 'Phường'".format(city)
                    )[['CityName','CityID','District', 'DistrictID','Ward', 'WardID']]
    
    city_path = root + '/data/{}'.format(city)

    print(os.path.exists(city_path))
    if os.path.exists(city_path)== False:
        os.mkdir(city_path)

    for district in df['District']: 
        district_path = root + '/data/{}/{}'.format(city, district)
        if os.path.exists(district_path) :
             os.mkdir(district_path)

        for ward in df.query(
                'CityName == "{}" and District == "{}"'.
                    format(city, district)
                    )['Ward']:

            search_term = 'restaurant at {}, {}, {}'.format(
                        city, district, ward)
            print(search_term)
            file_path = root + '/data/{}/{}.csv'.format(
                        city, district, ward)

            yield (search_term, file_path)
    
    


def check_available_data():
    ''' just crawing city and ward that have not been crawled yet'''
    pass  


def main():
    pass 


