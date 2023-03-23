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
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
driver.get("https://www.google.com/maps")
print(driver.title)
search_bar = driver.find_element(By.ID, "searchboxinput")

# TODO: loop here to change search-value
search_value = "restaurant district 3 hochiminh vn"
search_bar.clear()
search_bar.send_keys(search_value)
search_bar.send_keys(Keys.RETURN)

driver.implicitly_wait(5)  # wait 5s


def find_value(web_element, css):
    web_elements = web_element.find_elements(By.CSS_SELECTOR, css)
    if len(web_elements) > 0:
        return web_elements[0].get_attribute("innerHTML")
    return None


def find_values(web_element, css):
    web_elements = web_element.find_elements(By.CSS_SELECTOR, css)
    return [we.get_attribute("innerHTML") for we in web_elements]


def read_element(driver):

    try:
        result_div = driver.find_element(
            By.CSS_SELECTOR, "div.m6QErb.DxyBCb.kA9KIf.dS8AEf.ecceSd"
    )
        all_result = result_div.find_elements(
             By.CSS_SELECTOR, "div.bfdHYd.Ppzolf.OFBs3e")
        print("Number of restautant: {}".format(len(all_result)))
        for result in all_result:
            name = find_value(
                    result, "div.qBF1Pd.fontHeadlineSmall")
            stars = find_value(
                    result, "span.MW4etd")
            reviews = find_value(
                    result, "span.UY7F9")
            price = find_value(
                    result, "span[aria-label='Price: Expensive']")
            print(name, stars, reviews, price)
            note = find_values(
                    result, "div.ah5Ghc > span")
            print(note)
            open_time = find_values(
                    result, "div.W4Efsd > div.W4Efsd > span > span > span")
            print(open_time)
            b = find_values(result, "div.W4Efsd > div.W4Efsd > span > span")
            restaurant_type = b[0]
            address = b[2]
            print(restaurant_type, address)
        print("done")
            # TODO: save result here
    except Exception as e:
        print(e)


def scrolling_next(driver):
    time.sleep(1)
    driver.find_element(
            'xpath', 
            '/html/body/div[3]/div[9]/div[9]/div/div/div[1]/div[2]/div/div[1]/div/div/div[2]/div[1]').click()


#start scrolling your sidebar
    html = driver.find_element(
            'xpath', 
            '/html/body/div[3]/div[9]/div[9]/div/div/div[1]/div[2]/div/div[1]/div/div/div[2]/div[1]')
    html.send_keys(Keys.END)


try:
# selecting scroll body
    for _ in range(10):
        scrolling_next(driver)
        print(">> continue scrolling-------")
 
    read_element(driver)

except Exception as e:
    print(e)


while True:
    pass

driver.close()
