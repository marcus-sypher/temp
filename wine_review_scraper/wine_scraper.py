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
from wine_review_scraper_functions import scrape_wine_links, WineInfoScraper, mine_all_wine_info, max_page_number
import multiprocessing as mp
from multiprocessing import Process


wine_varieties = ['Pinot Noir', 'Chardonnay', 'Cabernet Sauvignon', 'Red Blend', 'Bordeaux-style Red Blend', 'Shiraz/Syrah',
'Sauvignon Blanc', 'Riesling', 'Sparkling', 'Merlot', 'White Blend', 'Sangiovese', 'Zinfandel', 'Rose',
'Tempranillo', 'Pinot Grigio/Gris', 'Italian Red', 'Italian White', 'Nebbiolo', 'Portuguese Red', 'Malbec',
'Rhone-style Red Blend', 'Cabernet Franc', 'Other White', 'Portuguese White', 'Other Red', 'Gruner Veltliner',
'Viognier', 'Gamay', 'Cabernet Franc', 'Portuguese White', 'Other White', 'Other Red', 'Grenache']

base_url = 'https://www.winemag.com/?s=&drink_type=wine&varietal='

cores = mp.cpu_count() - 1
chunks = [ # this is wine_varieties broken into available core-sized groups
            wine_varieties[i * cores : (i + 1) * cores]
            for i in range((len(wine_varieties) + cores - 1) // cores)
        ]

processes = []
final_args = [] # this contains available core-sized lists of arguments to be passed into scraper; args as tuple of wine name and max page n
for group in chunks:
    args = []
    for wine in group:
        url = base_url+wine

        # max_page = max_page_number(url)
        max_page = 2

        args.append([wine,max_page])
    final_args.append(args)

for group in final_args:
    for arg in group:
        processes.append(Process(target=mine_all_wine_info, args=tuple(arg)))

processes = [ 
            processes[i * cores : (i + 1) * cores]
            for i in range((len(processes) + cores - 1) // cores)
        ]

# wine_execution_load = input("There are " + str(len(chunks)) + " groups of " + str(cores) + " wines for a total of " + str(len(chunks*cores)) + " wines.\nHow many groups do you want to execute? " )
# page_execution_load = input("\nEnter a maximum page execution value or enter '@' to execute all. ")

# if __name__ == "__main__":
#     for processes in 
#         ft = datetime.datetime.now()
#         for process in processes:
#             process.start()
#         for process in processes:
#             process.join()
#         gt = datetime.datetime.now()

#         duration = gt - ft
#         print("\n\nBlock completed in", duration, "\n")

for processx in processes:
    print(len(processx))
    print(processx,"\n\n")


# stop = cores
# for x in range(0, stop):
#     print(processes[x])
#     if x == stop:
#         print("\nBreak")
#         stop+=cores