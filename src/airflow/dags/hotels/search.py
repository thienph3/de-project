from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
# from selenium.webdriver.support import expected_conditions as EC
# from selenium.webdriver.support.wait import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
import time 

import os
import pandas as pd 
import random  
import subprocess
from tqdm import tqdm
import argparse 
import numpy as np


SCROLLING_NUMBER = 10
def ward_district_search_term(city = None, chunk=True, chunk_number=1, run_only_chunk=1):
    root = './dags/restaurant'
    location_path = root + '/cfg/city_district_ward.csv' 
    data = pd.read_csv(location_path)
    cities = data['CityName'].unique()
    assert city in cities, 'can not find this city name'

    df = data.query("CityName == '{}' and Level in ('Phường', 'Xã')".format(city)
                    )[['CityName','CityID','District', 'DistrictID','Ward', 'WardID']]
    
    city_path = root + '/data/{}'.format(city)


    if not os.path.exists(city_path):
        os.mkdir(city_path)
    df.sort_values("District", inplace=True) 
    districts =  df['District'].unique()
    chunked_district = chunking(districts, chunk_number) 
    _districts = chunked_district[run_only_chunk]
    print(_districts)


    for district in _districts: 
        district_path = root + '/data/{}/{}'.format(city, district)
        if not os.path.exists(district_path):
             os.mkdir(district_path)

        for ward in df.query(
                'CityName == "{}" and District == "{}"'.
                    format(city, district)
                    )['Ward']:

            search_term = 'restaurant at {}, {}, {}'.format(
                        city, district, ward)
            file_path = root + '/data/{}/{}/{}.csv'.format(
                        city, district, ward)
            yield (search_term, file_path)
    

def find_value(web_element, css):
    web_elements = web_element.find_elements(By.CSS_SELECTOR, css)
    if len(web_elements) > 0:
        return web_elements[0].get_attribute("innerHTML")
    return None


def find_values(web_element, css):
    web_elements = web_element.find_elements(By.CSS_SELECTOR, css)
    return [we.get_attribute("innerHTML") for we in web_elements]


def read_element(driver, fpath):
    if os.path.exists(fpath):
        print("----> not read")
        return 

    datas = [] 
    try:
        result_div = driver.find_element(
            By.CSS_SELECTOR, "div.m6QErb.DxyBCb.kA9KIf.dS8AEf.ecceSd"
            )
        all_result = result_div.find_elements(
             By.CSS_SELECTOR, "div.bfdHYd.Ppzolf.OFBs3e")
        print("Number of restautant: {}".format(len(all_result)))

        for result in tqdm(all_result):
            try: 
                name = find_value(
                        result, "div.qBF1Pd.fontHeadlineSmall")
                stars = find_value(
                        result, "span.MW4etd")
                reviews = find_value(
                        result, "span.UY7F9")
                price = find_value(
                        result, "span[aria-label='Price: Expensive']")
                #print(name, stars, reviews, price)
       
                note = find_values(
                        result, "div.ah5Ghc > span")
                #print(note)
                open_time = find_values(
                        result, "div.W4Efsd > div.W4Efsd > span > span > span")
                #print(open_time)
                
                b = find_values(result, "div.W4Efsd > div.W4Efsd > span > span")
                if b and len(b) > 3:
                    restaurant_type = b[0]
                    address = b[2]
                else: 
                    restaurant_type = None
                    address = None
   
   
                _data =  {'name': name, 
                        'stars': stars, 
                        'review': reviews, 
                        'price': price,
                        'note': note, 
                        'open_time': open_time, 
                        'restautant_type': restaurant_type, 
                        'address': address}
                
                datas.append(_data)  
            except Exception as e:
                print(e)
        df_temp = pd.DataFrame(datas)
        df_temp = df_temp.astype(str)

        if os.path.exists(fpath):
            df_old = pd.read_csv(fpath, index_col=0)
            #df_old = df_old.astype(str)
            print("file exists, with {} rows".format(df_old.shape[0]))
            print(df_old.columns)
            print(df_old.head(3))
            df = pd.concat(
                    [df_old, df_temp],ignore_index=True
                    ).drop_duplicates().reset_index(drop=True)

            print("-----> append to have total {}".format(df.shape[0]))
        else:
            df = df_temp
            print("----> file not exists, append new")

        df.to_csv(fpath)

        print("done saving with len {} at path {}".format(
            len(datas), 
            fpath
            ))
            # TODO: save result here
    except Exception as e:
        print(e)
    return 


def scrolling_next(driver):
    driver.find_element(
            'xpath', 
            '/html/body/div[3]/div[9]/div[9]/div/div/div[1]/div[2]/div/div[1]/div/div/div[2]/div[1]').click()
#start scrolling your sidebar
    html = driver.find_element(
            'xpath', 
            '/html/body/div[3]/div[9]/div[9]/div/div/div[1]/div[2]/div/div[1]/div/div/div[2]/div[1]')
    html.send_keys(Keys.END)

def search(search_term, fpath):

    # TODO: loop here to change search-value
    search_value = search_term 
    search_bar.clear()
    search_bar.send_keys(search_value)
    search_bar.send_keys(Keys.RETURN)
    
    driver.implicitly_wait(5)  # wait 5s

    try:
# selecting scroll body
        for _ in range(SCROLLING_NUMBER):
            try:
                scrolling_next(driver)
            except Exception as e:
                pass   

        print('----done scrolling, start read element')
        read_element(driver, fpath)
    

    except Exception as e:
        print(e)

    return 

def main(args):
    city =  'Thành phố Hà Nội'  #'Thành phố Hồ Chí Minh' 
    chunk_number = int(args.num_splitted)
    run_only_chunk = int(args.num_run)
    #searching all restaurant in a ward of a city
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    driver.get("https://www.google.com/maps")
    print(driver.title)
    search_bar = driver.find_element(By.ID, "searchboxinput")

    #city = 'Thành phố Hồ Chí Minh' # ['Thành phố Hà Nội'] 
    for search_term, fpath in ward_district_search_term(
            city = city, chunk=True, chunk_number=chunk_number, run_only_chunk=run_only_chunk):
        print("search term:", search_term, "\n", "fpath: ", fpath)
           # TODO: loop here to change search-value
        search_value = search_term 
        search_bar.clear()
        search_bar.send_keys(search_value)
        search_bar.send_keys(Keys.RETURN)
        
        driver.implicitly_wait(5)  # wait 5s

        try:
#     selecting scroll body
            for _ in range(SCROLLING_NUMBER):
                try:
                    scrolling_next(driver)
                except Exception as e:
                    pass   

            print('----done scrolling, start read element')
            read_element(driver, fpath)
        

        except Exception as e:
            print(e)
            time.sleep(random.randint(1, 3))
                
    driver.close()
    return


def chunking(l, n):
    idxs = np.arange(len(l))
    chunks = np.array_split(idxs, n)
    r = []
    for chunk in chunks:
        r.append(l[chunk[0]: chunk[-1]+1])
    return r 
        
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    # parser.add_argument("city")
    
    parser.add_argument("num_splitted")
    parser.add_argument("num_run")

    args = parser.parse_args()
    main(args)

