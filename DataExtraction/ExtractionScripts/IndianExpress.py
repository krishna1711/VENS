# -*- coding: utf-8 -*-
"""
Created on Thu May 21 14:44:23 2020

@author: ajinkya
"""
import requests
from bs4 import BeautifulSoup
from collections import defaultdict
import pandas as pd
import json
import datetime




def extractData():
    url='https://indianexpress.com/section/cities/'
    cities=['Pune','Mumbai','Bangalore','Delhi','Kolkata','Chandigarh','Ahmedabad','Lucknow','Hyderabad','Chennai']
    data=defaultdict(list)
    for city in cities:
        print("\nCity: "+str(city))
        ######################### Page 1 #####################################
        ############################################################################
        page=requests.get(url+str(city)+'/')
        soup=BeautifulSoup(page.content,'html5lib')
        pages=soup.find(class_='cities-stories')
        
        for i in pages.find_all('div',attrs={'class':'story'}):
            links=i.h6.a['href']
            content=i.find_all('p')
            con=[]
            for i in content:
                desc=i.text
                con.append(desc)
            data['Description'].append(con[1])
            data['Link'].append(links)
            
        if(city!='Pune'):
            l=data['Link'][-20:]
        else:
            l=data['Link']
    
        for i in l:
            soup=BeautifulSoup((requests.get(i)).content,'html5lib')
            title=soup.h1.text
            date_and_time=soup.find('span',attrs={'itemprop':'dateModified'}).text.strip().strip('Updated: ').strip('Published: ')
            location=city
            data['Title'].append(title)
            data['FullDate'].append(date_and_time)
            data['Location'].append(location)
        ###########################################################################
        
        for i in range(2,26):
            page_url=url+str(city)+'/page/'+str(i)+'/'
            page=requests.get(page_url)
            print("\nPage "+str(i)+" started\n")
            soup=BeautifulSoup(page.content,'html5lib')
            pages=soup.find(class_='nation') 
            c=0
            try:
                for j in pages.find_all('div',attrs={'class':'articles'}):
                    titles=j.h2.text.strip()
                    links=j.h2.a['href']
                    dates=j.find('div',attrs={'class':'date'}).text.replace(',',' ')
                    contents=j.find('p').text
                    location=city
                    data['Title'].append(titles)
                    data['Link'].append(links)
                    data['FullDate'].append(dates)
                    data['Description'].append(contents)
                    data['Location'].append(location)
                    c=c+1
                    print("Record "+str(c)+" done")
            except Exception as e:
                with open("IndianExpressLog.log",'a+') as logf:
                    now = datetime.datetime.now()
                    logf.write(f"{now} INFO [Error] While processing  {page_url} : \n")               
                pass
            print("\nPage "+str(i)+" done!!!")
        print("\nCity "+str(city)+" completed")
        
    ie_data=pd.DataFrame.from_dict(data)
    ie_data['FullDate']=ie_data['FullDate'].str.replace(","," ")
    ie_data['FullDate']=pd.to_datetime(data['FullDate'])
    ie_data['Date']=ie_data['FullDate'].dt.date
    ie_data['Time']=ie_data['FullDate'].dt.time
    ie_data=ie_data.drop('FullDate',axis=1)
    ie_data.to_csv('../ExtractedData/IndianExpressDataLatest.csv',index=False)
    datetime_stamp=datetime.datetime.now()
    datetime_stamp=str(datetime_stamp).replace(' ','_').replace(':','_').split('.')[0]
    ie_data.to_csv(f'../BackupExtractedData/IndianExpressData_{datetime_stamp}.csv',index=False)
















































































    




