from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service

from webdriver_manager.chrome import ChromeDriverManager
import time

import datetime
import os
import pandas as pd
import random
from tqdm import tqdm
import numpy as np

from airflow import DAG
from airflow.decorators import task


DAG_ID = "crawl_data_dag"

with DAG(
    dag_id=DAG_ID,
    schedule=None,
    start_date=datetime.datetime(2020, 2, 2),
    catchup=False,
) as dag:
    SCROLLING_NUMBER = 10

    def _chunking(l, n):
        idxs = np.arange(len(l))
        chunks = np.array_split(idxs, n)
        r = []
        for chunk in chunks:
            r.append(l[chunk[0] : chunk[-1] + 1])
        return r

    def _ward_district_search_term(
        city=None, chunk=True, chunk_number=1, run_only_chunk=1
    ):
        root = "./dags/restaurant"
        location_path = root + "/cfg/city_district_ward.csv"
        data = pd.read_csv(location_path)
        cities = data["CityName"].unique()
        assert city in cities, "can not find this city name"

        df = data.query("CityName == '{}' and Level in ('Phường', 'Xã')".format(city))[
            ["CityName", "CityID", "District", "DistrictID", "Ward", "WardID"]
        ]

        city_path = root + "/data/{}".format(city)

        if not os.path.exists(city_path):
            os.mkdir(city_path)
        df.sort_values("District", inplace=True)
        districts = df["District"].unique()
        chunked_district = _chunking(districts, chunk_number)
        _districts = chunked_district[run_only_chunk]
        print(_districts)

        for district in _districts:
            district_path = root + "/data/{}/{}".format(city, district)
            if not os.path.exists(district_path):
                os.mkdir(district_path)

            for ward in df.query(
                'CityName == "{}" and District == "{}"'.format(city, district)
            )["Ward"]:
                search_term = "restaurant at {}, {}, {}".format(city, district, ward)
                file_path = root + "/data/{}/{}/{}.csv".format(city, district, ward)
                yield (search_term, file_path)

    def _scrolling_next(driver):
        driver.find_element(
            "xpath",
            "/html/body/div[3]/div[9]/div[9]/div/div/div[1]/div[2]/div/div[1]/div/div/div[2]/div[1]",
        ).click()
        # start scrolling your sidebar
        html = driver.find_element(
            "xpath",
            "/html/body/div[3]/div[9]/div[9]/div/div/div[1]/div[2]/div/div[1]/div/div/div[2]/div[1]",
        )
        html.send_keys(Keys.END)

    def _find_value(web_element, css):
        web_elements = web_element.find_elements(By.CSS_SELECTOR, css)
        if len(web_elements) > 0:
            return web_elements[0].get_attribute("innerHTML")
        return None

    def _find_values(web_element, css):
        web_elements = web_element.find_elements(By.CSS_SELECTOR, css)
        return [we.get_attribute("innerHTML") for we in web_elements]

    def _read_element(driver):
        datas = []

        try:
            result_div = driver.find_element(
                By.CSS_SELECTOR, "div.m6QErb.DxyBCb.kA9KIf.dS8AEf.ecceSd"
            )
            all_result = result_div.find_elements(
                By.CSS_SELECTOR, "div.bfdHYd.Ppzolf.OFBs3e"
            )
            print("Number of restautant: {}".format(len(all_result)))

            for result in tqdm(all_result):
                try:
                    name = _find_value(result, "div.qBF1Pd.fontHeadlineSmall")
                    stars = _find_value(result, "span.MW4etd")
                    reviews = _find_value(result, "span.UY7F9")
                    price = _find_value(result, "span[aria-label='Price: Expensive']")
                    # print(name, stars, reviews, price)

                    note = _find_values(result, "div.ah5Ghc > span")
                    # print(note)
                    open_time = _find_values(
                        result, "div.W4Efsd > div.W4Efsd > span > span > span"
                    )
                    # print(open_time)

                    b = _find_values(result, "div.W4Efsd > div.W4Efsd > span > span")
                    if b and len(b) > 3:
                        restaurant_type = b[0]
                        address = b[2]
                    else:
                        restaurant_type = None
                        address = None

                    _data = {
                        "name": name,
                        "stars": stars,
                        "review": reviews,
                        "price": price,
                        "note": note,
                        "open_time": open_time,
                        "restautant_type": restaurant_type,
                        "address": address,
                    }

                    datas.append(_data)
                except Exception as e:
                    print(e)

            return datas
        except Exception as e:
            print(e)

        return datas

    @task(task_id="crawl_data")
    def crawl_data_and_load_to_postgres(ds=None, **kwargs):
        datas = []

        cities = ["Thành phố Hồ Chí Minh", "Thành phố Hà Nội"]  #'Thành phố Hồ Chí Minh'
        chunk_number = 1
        run_only_chunk = 1
        # searching all restaurant in a ward of a city

        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
        driver.get("https://www.google.com/maps")
        search_bar = driver.find_element(By.ID, "searchboxinput")

        for city in cities:
            for search_term, fpath in _ward_district_search_term(
                city=city,
                chunk=True,
                chunk_number=chunk_number,
                run_only_chunk=run_only_chunk,
            ):
                print("search term:", search_term, "\n", "fpath: ", fpath)
                search_value = search_term
                search_bar.clear()
                search_bar.send_keys(search_value)
                search_bar.send_keys(Keys.RETURN)

                driver.implicitly_wait(5)  # wait 5s

                try:
                    # selecting scroll body
                    for _ in range(SCROLLING_NUMBER):
                        try:
                            _scrolling_next(driver)
                        except Exception as e:
                            pass

                    temp = _read_element(driver)
                    datas += temp

                except Exception as e:
                    print(e)
                    time.sleep(random.randint(1, 3))

        driver.close()

        df = pd.DataFrame(datas)
        df = df.astype(str)
        df.to_csv("temp__")
        return

    crawl_data = crawl_data_and_load_to_postgres()
