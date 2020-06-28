# -*- coding: utf-8 -*-
"""
Created on Fri May 22 00:55:52 2020

@author: V Krishna Mohan
"""

from bs4 import BeautifulSoup
import requests

url = "https://timesofindia.indiatimes.com"

soup = BeautifulSoup(requests.get(url).text,"lxml")

###Get all city Links
Citylinks = [liinui.a.get('href') if liinui.a.get('href').__contains__("http") else url+liinui.a.get('href') for li in soup.find_all("li",attrs={"class":"nav-City"}) for divli in li.find_all("div") for ul in divli.find_all("ul") for liinui in ul.find_all("li")]
### store city and link as key value pair
CityLinksDictionary = {links.split("/")[-1]:links for links in Citylinks}

###Get all State Links in India
IndiaLinks = [liinui.a.get('href') if liinui.a.get('href').__contains__("http") else url+liinui.a.get('href') for li in soup.find_all("li",attrs={"class":"nav-India"}) for divli in li.find_all("div") for ul in divli.find_all("ul") for liinui in ul.find_all("li")]
### store State and link as key value pair
IndiaLinksDictonary = {links.split("/")[-1]:links for links in IndiaLinks}
'''
The above dictonary is further stored in the json file which can be used at 
anypoint of time as these are the pages link, as template no need to do 
requests for them

The following can be uncommented when to save if site is updated
'''

###############################################################################
### saving into json
#import json
#with open("TOICityLinks.json","w+") as f:
#    f.write(json.dumps(CityLinksDictionary))
#with open("TOIStateLinks.json","w+") as f:
#    f.write(json.dumps(IndiaLinksDictonary))
###############################################################################

#### testing to get the location type
#import spacy
#nlp = spacy.load('en_core_web_sm')
#
#list(IndiaLinksDictonary.keys())[5]
#labels = [{ent.text:ent.label_} for ent in nlp(",".join(list(IndiaLinksDictonary.keys())[5:7])).ents]
###############################################################################

## The following function returns all latest page links of each city page and its only for a sigle page, pagination is not handeled here.
def getCityLatesPageLinks(citylink,url):
    ###subsoup is for single link
    subsoup = BeautifulSoup(requests.get(citylink).text)
    ##data for one page
#    cityLatestNewsTitle = [li.find("span",attrs={"class":"w_tle"}).a.get("title") for li in subsoup.find("ul",attrs={"class":"list5 clearfix"}).find_all("li") if li.find("span",attrs={"class":"w_tle"}) is not None]
    pageLink = [url+li.find("span",attrs={"class":"w_tle"}).a.get("href") for li in subsoup.find("ul",attrs={"class":"list5 clearfix"}).find_all("li") if li.find("span",attrs={"class":"w_tle"}) is not None]
    return pageLink

### all city page links are extracted here
allCityLatestPageLinks = list(map(getCityLatesPageLinks,Citylinks,[url]*len(Citylinks)))

###getting data from pages i.e respective page link where each news is opened in a new page
import dateutil.parser as dparser
from nltk.sentiment.vader import SentimentIntensityAnalyzer
sentAnalyzer = SentimentIntensityAnalyzer()

def getData(link):
    try:
        datasoup = BeautifulSoup(requests.get(link).text)
        ## get title of the news page
        failed = ""
        try:
            title = datasoup.find("div",attrs={"class","_38kVl"}).find("h1",attrs={"class":"K55Ut"}).text
        except:
            failed +="@Title" 
            title = None
        ## get updated/posted date of the news
        try:
            date = datasoup.find("div",attrs={"class","_38kVl"}).find("div").find("div").text
            _datetime = dparser.parse(date,fuzzy=True)
            date = _datetime.strftime("%d/%m/%Y")
            time = _datetime.strftime("%H:%M")
        except:
            failed +="@Date and Time"
            date,time=None,None
        try:
            location = link.split("city/")[-1].split("/")[0]
        except:
            failed +="@location"
            location = None
        try:
            Content = datasoup.find("div",attrs={"class":"_3WlLe clearfix"}).text
        except:
            failed +="@Content"
            Content=None
        try:
            sentimentScore = sentAnalyzer.polarity_scores(Content)['compound']
            sentiment ="Positive" if sentimentScore>0.5 else "Negative" if sentimentScore<0.0 else "Neutral"
        except:
            failed +="@Sentiment"
            sentiment = None
        dataDict = {"Title":title,
                    "Content":Content,
                    "Location":location,
                    "Date": date,
                    "Time": time,
                    "Sentiment":sentiment}
        return dataDict
        if failed != "":
            raise
    except Exception as e:
        with open("Log.txt","a+") as f:
            now = datetime.datetime.now()
            f.write(f"{now} :: Error -> <{e}> {link} > Failed To Process  due to <<{failed}>>\n")
        pass

import datetime
import json
print()

for i in range(10):
    with open("log.txt","a+") as f:
        f.writelines(f"{datetime.datetime.now()} Line{i}\n")
####temperory save allCityPages though it gets updated
with open("AllCityLatestPageLinks.json","w+") as f:
    f.write(json.dumps(allCityLatestPageLinks))
timestamp = f"{datetime.datetime.now()}".replace(" ","_").replace(":","_").replace(".","_")
with open(f"BackupCityLinks/AllCityLatestPageLinks_{timestamp}.json","w+") as f:
    f.write(json.dumps(allCityLatestPageLinks))    
#with open("AllCityLatestPageLinks.json") as f:
#    tk = json.load(f)
#InitialCityData = list(map(getData,allCityLatestPageLinks[0][:10])) 

