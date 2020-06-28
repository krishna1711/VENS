# -*- coding: utf-8 -*-
"""
Created on Fri Jun 26 20:35:32 2020

@author: Office
"""

import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
import pandas as pd
import dateutil.parser as dparser
import datetime
#from nltk.sentiment.vader import SentimentIntensityAnalyzer
#sentAnalyzer = SentimentIntensityAnalyzer()
from multiprocessing import Pool
from multiprocessing import freeze_support
import multiprocessing
import datetime
import json
from bs4 import BeautifulSoup
import requests

url = "https://timesofindia.indiatimes.com"
soup = BeautifulSoup(requests.get(url).text,"lxml")
###Get all city Links
Citylinks = [liinui.a.get('href') if liinui.a.get('href').__contains__("http") else url+liinui.a.get('href') for li in soup.find_all("li",attrs={"class":"nav-City"}) for divli in li.find_all("div") for ul in divli.find_all("ul") for liinui in ul.find_all("li")]
### store city and link as key value pair
CityLinksDictionary = {links.split("/")[-1]:links for links in Citylinks}
## The following function returns all latest page links of each city page and its only for a sigle page, pagination is not handeled here.
def getCityLatesPageLinks(citylink,url):
    ###subsoup is for single link
    subsoup = BeautifulSoup(requests.get(citylink).text,'lxml')
    ##data for one page
#    cityLatestNewsTitle = [li.find("span",attrs={"class":"w_tle"}).a.get("title") for li in subsoup.find("ul",attrs={"class":"list5 clearfix"}).find_all("li") if li.find("span",attrs={"class":"w_tle"}) is not None]
    pageLink = [url+li.find("span",attrs={"class":"w_tle"}).a.get("href") for li in subsoup.find("ul",attrs={"class":"list5 clearfix"}).find_all("li") if li.find("span",attrs={"class":"w_tle"}) is not None]
    return pageLink

### all city page links are extracted here
allCityLatestPageLinks = list(map(getCityLatesPageLinks,Citylinks,[url]*len(Citylinks)))


def getDriver():
    chromeoptions = ChromeOptions()
    chromeoptions.add_argument("--headless")
    driver_ = webdriver.Chrome(options=chromeoptions)
    return driver_

def getData(link, driver):
    try:
        driver.get(link)
        title = driver.find_element_by_xpath("/html/body/div/div/div[4]/div[1]/div[2]/div[1]/h1").get_attribute('innerHTML')
        
        datetime_ = driver.find_element_by_xpath("/html/body/div/div/div[4]/div[1]/div[2]/div[1]/div/div[1]").get_attribute('innerHTML').split("|")[-1]
        datetime_ = datetime_.strip()
        _datetime = dparser.parse(datetime_,fuzzy=True)
        date = _datetime.strftime("%d/%m/%Y")
        time_ = _datetime.strftime("%H:%M")
        location = link.split("city/")[-1].split("/")[0]
        Content = driver.find_element_by_xpath("/html/body/div/div/div[4]/div[1]/div[2]/div[3]/div[2]").get_attribute('innerText')
        
        dataDict = {"Title":title,
                            "Description":Content,
                            "Link":link,
                            "Location":location,
                            "Date": date,
                            "Time": time_}
#        print(f"{title} : {link}")
        return dataDict
    except Exception as e:
        now = datetime.datetime.now()
        with open("TOILog.log","a+") as toif:
            toif.write(f"{now} [Error] [While processing Page <{link}>] [Exception : {e}]")
        pass
if __name__ == '__main__':
    start = time.time()
#    freeze_support()
    with open("AllCityLatestPageLinks.json") as f:
        iinput = json.load(f)
    allinput = [i for ai in iinput for i in ai]
#    p=Pool(multiprocessing.cpu_count())
    driver = getDriver()
    cityData = list(map(getData,allinput,[driver] * len(allinput)))
    cityData = [i for i in cityData if i != None]
    df = pd.DataFrame(cityData)
    df.dropna(thresh=5,inplace=True)
#    df["Sentiment"] = df["Content"].apply(lambda x: "Positive" if sentAnalyzer.polarity_scores(x)['compound']>0.5 else "Negative" if sentAnalyzer.polarity_scores(x)['compound']<0.0 else "Neutral")
    df.to_csv(f"..\\ExtractedData\\TOICurrentCityData.csv")
    datetimeStamp = datetime.datetime.now()
    datetimeStamp = str(datetimeStamp).replace(":","_").replace(" ","_").split(".")[0] 
    df.to_csv(f"..\\BackupExtractedData\\TOICityData_{datetimeStamp}.csv")
    end = time.time()
    print("Time Taken: " + str(end-start))
