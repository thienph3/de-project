import pandas as pd
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, ElementNotInteractableException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait

import random 
import time 

WEBDRIVER_DRIVER_DELAY_TIME_INT = random.randint(1, 4)
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--headless=new')
chrome_options.add_argument('--no-sandbox')
chrome_options.headless = True
driver = webdriver.Chrome('chromedriver', options=chrome_options)
driver.implicitly_wait(4)
wait = WebDriverWait(driver, WEBDRIVER_DRIVER_DELAY_TIME_INT)


url = f'https://www.lazada.vn/dien-thoai-di-dong/?page=1'
driver.get(url)

def get_data(url):
    start = time.time()
    #create an empty placeholder for dataset
    data = pd.DataFrame()
    data['category_url'] = str(url)
    

    driver.get(url)
    elements = driver.find_elements(By.CSS_SELECTOR, '.RfADt [href]') 
    # all class RfADt with attribute [href]
    titles = [elem.get_attribute('title') for elem in elements]
    links  = [elem.get_attribute('href') for elem in elements]
    print(titles[:10])
    print(len(titles))

   
    data['titles'] = titles
    data['links'] = links

    # get price
    #elements_price = driver.find_elements(By.CSS_SELECTOR, ".aBrPO")
    #prices = [elem_price.text for elem_price in elements_price]
    
    #data['prices'] = prices

    print(data.head(10))
    print(data.shape)
    print((time.time()-start)/60)
if __name__ == "__main__":
    get_data(url)








