# -*- coding: utf-8 -*-
"""
Created on Thu Jun 25 16:31:01 2020

@author: user
"""

import psycopg2
from psycopg2 import Error
import pandas as pd
import configparser
import datetime
import os



config = configparser.RawConfigParser()
config.read('vensConfig.properties')
config.sections()

def enterLog(logMessage):
    with open("vensLog.log",'a+') as logf:
        now = datetime.datetime.now()
        logf.write(f"{now} INFO " + logMessage +"\n")

def connectDatabase(database):
    try:
        connection = psycopg2.connect(user = config.get('VENS','DBUser'),
                                      password = config.get('VENS','DBPassword'),
                                      host = "127.0.0.1",
                                      port = "5432",
                                      database = database)
        
        cursor = connection.cursor()
        
        logmsg = f"[Success] : Connected to database '{database}'..."
        enterLog(logmsg)
        return connection,cursor
    
    except Exception as error:
        logmsg = f"[Error] : While connecting database '{database}' >>> [DBError] : {error}"
        print(f"Error while connecting database '{database}'")


def deleteTable(tableName):
    try:
        drop_temporary_table=f''' DELETE FROM {tableName} '''
        connection,cursor=connectDatabase("VENS")
        cursor.execute(drop_temporary_table)
        connection.commit()
        logmsg = "[Success] : Deleted table '{tableName}'..."
        enterLog(logmsg)
        
        return 1

    except Exception as error:
        error=" ".join(str(error).replace('^','').splitlines()).strip()
        logmsg = f"[Error] : While deleting table '{tableName}' >>> [DBError] : {error}"
        enterLog(logmsg)
        print(f"Error while deleting table '{tableName}'")
    finally:
        connection.close()
    
def createTable(tableName,primaryKeyStatus="PRIMARY KEY NOT NULL"):
    try:
        create_temp_table_query = f'''CREATE TABLE IF NOT EXISTS {tableName}
                                      (Description TEXT NOT NULL,
                                      Link VARCHAR(500) {primaryKeyStatus},
                                      Title TEXT NOT NULL,
                                      Location VARCHAR(20) NOT NULL,
                                      Date DATE NOT NULL,
                                      Time TIME NOT NULL) '''
          
        connection,cursor=connectDatabase("VENS")
        cursor.execute(create_temp_table_query)
        connection.commit()
        
        logmsg = f"[Success] : Created table '{tableName}'..."
        enterLog(logmsg)
               
        return 1
        
    except Exception as error:
        error=" ".join(str(error).replace('^','').splitlines()).strip()
        logmsg = f"[Error] : While creating table '{tableName}' >>> [DBError] : {error}"
        enterLog(logmsg)
        
        print(f"Error while creating table '{tableName}'")
    finally:
        connection.close()
        

def insertIntoDB(data,tableName,temptableName):
    try:
        postgres_temp_insert_query = f""" INSERT INTO {temptableName} VALUES (%s,%s,%s,%s,%s,%s)"""
        connection,cursor=connectDatabase("VENS")
        cursor.executemany(postgres_temp_insert_query, data.values)    
        connection.commit()
        logmsg = f"[Success] : Inserted values into table '{temptableName}'..."
        enterLog(logmsg)
          
        postgres_insert_query = f""" INSERT INTO {tableName} SELECT * FROM {temptableName} ON CONFLICT DO NOTHING"""
        cursor.execute(postgres_insert_query)
        connection.commit()
        
        logmsg = f"[Success] : Inserted values into table '{tableName}'..."
        enterLog(logmsg)      
    except Exception as error:
        error=" ".join(str(error).replace('^','').splitlines()).strip()
        logmsg = f"[Error] : While inserting values into table >>> [DBError] : {error}"
        enterLog(logmsg)
        
        print(f"Error while inserting values into table")
    finally:
         connection.close()

def dumpFileToDB(csvInputPath,tableName,temptableName,databaseName="VENS"):
    filename=os.path.basename(csvInputPath)
    try:
        data=pd.read_csv(csvInputPath,engine='python')
    except Exception as e:
        logmsg = f"[Error] : While reading the file '{csvInputPath}' >>> {e}"
        enterLog(logmsg)  
        raise f"Error while reading the file {csvInputPath} : {e}"
        
    connectDatabase(databaseName)
    deleteTable(temptableName)
    createTable(tableName)
    createTable(temptableName,primaryKeyStatus='NOT NULL')
    insertIntoDB(data, tableName, temptableName)
    
    logmsg = f"[Success] : File {filename} successfully inserted into database {databaseName}"
    enterLog(logmsg)  
    
    return f"File {filename} successfully inserted into database {databaseName}"


if __name__=="__main__":
    dumpFileToDB(csvInputPath, tableName, temptableName)