from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
import time
import datetime
import pandas as pd
from bs4 import BeautifulSoup
import urllib.parse

def fillIn(dictionary, expected_keys):
    
    keys, values = zip(*dictionary.items())
    keys = list(keys)
    values = list(values)

    if len(keys) == len(values):
        for key in expected_keys:
            if key not in keys:
                keys.append(key)
                values.append("null")


    return dict(zip(keys,values))

def max_page_number(url):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    driver = webdriver.Chrome(options=chrome_options)
    driver.get(url)
    xpath = '//*[@id="search-results"]/div/div[1]/div[4]/div/div/ul/li[8]/a'
    max_page = WebDriverWait(driver, 10).until(
                 EC.presence_of_element_located((By.XPATH, xpath))
                )

    driver.quit()

    return max_page.text

def scrape_wine_links(base_url, min_page_number, max_page_number):

    wine_pages_to_mine = []

    chrome_options = Options()
    chrome_options.add_argument("--headless")
    driver = webdriver.Chrome(options=chrome_options)

    for page_number in range(min_page_number, max_page_number):
        url = base_url + str(page_number)
        
        try:
            driver.get(url)
            class_="review-listing"
            all_wine_links = WebDriverWait(driver, 10).until(
                 EC.presence_of_all_elements_located((By.CLASS_NAME, class_))
                )

            for link in all_wine_links:
                wine_pages_to_mine.append(link.get_attribute('href'))

        except:
            continue
        driver.quit()

    series_wine_pages = pd.Series(wine_pages_to_mine)
    series_wine_pages.to_csv('data/wine_pages_to_mine.csv')

    return wine_pages_to_mine

class WineInfoScraper:

    def __init__(self, wine_page_to_mine):
        self.page = wine_page_to_mine


    def get_soup_wine_page(self):

        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--blink-settings=imagesEnabled=false")
        driver = webdriver.Chrome(options=chrome_options)

        driver.get(self.page)

        try:
            xpath = '//*[@id="email-gate"]/div/div[2]/form/input'
            popup = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, xpath))
            )
            
            popup.send_keys("john.doe@gmail.com")
            popup.submit()

            time.sleep(3)
            driver.refresh()

        except NoSuchElementException:
            pass

        time.sleep(1)

        page_source = driver.page_source
        wine_review_soup = BeautifulSoup(page_source, 'html.parser')

        driver.quit()

        return wine_review_soup


    def get_wine_name(self, soup): # checked
        wine_name_raw = soup.find(class_='header__title')
        wine_name_clean = wine_name_raw.text
        return wine_name_clean


    def get_vintage(self, wine_name_clean): # checked
        name_strings = wine_name_clean.split(' ')
        number_strings = [i for i in name_strings if (i.isnumeric())]
        for n in number_strings:
            if 1900 < int(n) < datetime.datetime.now().year:
                vintage = n
            else:
                vintage = 0
            return vintage
                


    def get_wine_rating(self, soup): # checked
        try:
            wine_rating_raw = soup.find(class_='rating')
            wine_rating_text = wine_rating_raw.text
            wine_rating_list = wine_rating_text.split('\n')
            wine_rating_clean = wine_rating_list[1]
        except:
            wine_rating_clean = ''

        return wine_rating_clean


    def get_wine_description(self, soup): # checked
        try:
            wine_description_raw = soup.find(class_='description')
            wine_description_clean = wine_description_raw.text
        except:
            wine_description_clean = ''
        return wine_description_clean


    def chunks(self, l, n):
        for i in range(0, len(l), n):
            yield l[i:i + n]


    def get_wine_info(self, soup, primary_secondary):

        wine_info_raw = soup.find(class_=primary_secondary)
        wine_info_text = wine_info_raw.text
        wine_info_list = wine_info_text.split('\n')
        wine_info_list_no_blanks = [w for w in wine_info_list if len(w) > 1]

        # Break the list of wine information up into chunks of two
        wine_info_list_chunked = list(self.chunks(wine_info_list_no_blanks, 2))

        # Each chunk will consist of a label and a value. Put these into a dictionary format for easier navigating
        wine_info_dict = {}
        for w in wine_info_list_chunked:
            # When extracting the price make sure to eliminate the 'noise' in the text string
            if w[0] == 'Price':
                clean_price_list = str(w[1]).split(',')
                wine_info_dict['Price'] = clean_price_list[0]
                continue

            # when extracting hte appellation then ma
            if w[0] == 'Appellation':
                appellation_split = w[1].split(',')
                wine_info_dict['Country'] = appellation_split[-1]
                if len(appellation_split) > 1:
                    wine_info_dict['Province'] = appellation_split[-2]
                if len(appellation_split) > 2:
                    wine_info_dict['Region'] = appellation_split[-3]
                if len(appellation_split) > 3:
                    wine_info_dict['Subregion'] = appellation_split[-4]

            if len(w) >= 2:
                wine_info_dict[w[0]] = w[1]

            else:
                continue

        return wine_info_dict


    def get_reviewer_name(self, soup):
        wine_reviewer_raw = soup.find(class_='taster-area')
        wine_reviewer_clean = wine_reviewer_raw.text
        wine_reviewer_info_list = wine_reviewer_clean.split('\n')
        wine_reviewer_info_list_no_blanks = [w for w in wine_reviewer_info_list if len(w) > 1]
        wine_reviewer_clean = wine_reviewer_info_list_no_blanks[0]
        return wine_reviewer_clean

    def get_reviewer_twitter_handle(self, soup):
        wine_reviewer_twitter_raw = soup.find(class_='twitter-handle')
        try:
            wine_reviewer_twitter_clean = wine_reviewer_twitter_raw.text
            return wine_reviewer_twitter_clean
        except:
            return None

    def scrape_all_info(self):

        complete_criteria = ['Alcohol', 'Appellation', 'Bottle Size', 'Category', 'Country',
                        'Date Published', 'Description', 'Designation', 'Importer',
                        'Name', 'Price', 'Province', 'Rating', 'Region', 'Reviewer',
                        'Reviewer Twitter Handle', 'Subregion', 'User Avg Rating',
                        'Variety', 'Vintage', 'Winery']

        wine_info_dict = {}
        wine_review_soup = self.get_soup_wine_page()

        wine_info_dict['Name'] = self.get_wine_name(wine_review_soup)
        wine_info_dict['Vintage'] = self.get_vintage(wine_info_dict['Name'])
        wine_info_dict['Rating'] = self.get_wine_rating(wine_review_soup)
        wine_info_dict['Description'] = self.get_wine_description(wine_review_soup)
        wine_info_dict.update(self.get_wine_info(wine_review_soup, primary_secondary='primary-info'))
        wine_info_dict.update(self.get_wine_info(wine_review_soup, primary_secondary='secondary-info'))
        wine_info_dict['Reviewer'] = self.get_reviewer_name(wine_review_soup)
        wine_info_dict['Reviewer Twitter Handle'] = self.get_reviewer_twitter_handle(wine_review_soup)


        return fillIn(wine_info_dict,complete_criteria)

def mine_all_wine_info(variety, max_page_number):

    st = time.time()

    variety = urllib.parse.quote(variety)

    all_wine_links = scrape_wine_links(base_url='https://www.winemag.com/?s=&drink_type=wine&varietal='+variety+'&page=',
                                       min_page_number=1,
                                       max_page_number=max_page_number)

    all_wine_info = []
    for link in all_wine_links:
        try:
            scraper = WineInfoScraper(wine_page_to_mine=link)
            wine_info = scraper.scrape_all_info()
            all_wine_info.append(wine_info)
        except:
            continue

    
    full_wine_info_dataframe = pd.DataFrame(all_wine_info)
    full_wine_info_dataframe = full_wine_info_dataframe[['Alcohol', 'Appellation', 'Bottle Size', 'Category', 'Country',
                                                         'Date Published', 'Description', 'Designation', 'Importer',
                                                         'Name', 'Price', 'Province', 'Rating', 'Region', 'Reviewer',
                                                         'Reviewer Twitter Handle', 'Subregion', 'User Avg Rating',
                                                         'Variety', 'Vintage', 'Winery']]

    et = time.time()
    res = et - st

    raw_variety = "".join(c for c in variety if c.isalpha())
    path = "data/full_wine_info_" + raw_variety + ".csv"
    full_wine_info_dataframe.to_csv(path)

    print(full_wine_info_dataframe)
    print('\n\nTotal Execution Time:', res, 'seconds')
    print('Execution Time / Wine:', res/len(full_wine_info_dataframe), 'second\n\n')

if __name__ == '__main__':
    max_page = max_page_number('https://www.winemag.com/?s=&varietal=Pinot%20Noir&drink_type=wine&page=1')
    print(max_page)