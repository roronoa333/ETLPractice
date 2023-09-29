import os
import sys
from sqlalchemy import create_engine, URL
import configparser
import requests
import datetime
import json
import decimal
import openpyxl
import pandas as pd
import psycopg2

# get data from configuration file

def extract(config):
    startDate = config['CONFIG']['startDate']
    url = config['CONFIG']['url']
# request data from URL
    try:
        BOCResponse = requests.get(url+startDate)
        return BOCResponse
    except Exception as e:
        print('could not make request:' + str(e))
        sys.exit()

def transform(BOCResponse):
    BOCDates = []
    BOCRates = []

    if BOCResponse.status_code == 200:
        BOCRaw = json.loads(BOCResponse.text)
    # extract observation data into column arrays
        for row in BOCRaw['observations']:
            BOCDates.append(datetime.datetime.strptime(row['d'],'%Y-%m-%d'))
            BOCRates.append(decimal.Decimal(row['FXUSDCAD']['v']))
    # create petl table from column arrays and rename the columns
        exchangeRates = pd.DataFrame({'date':BOCDates,'rate':BOCRates})
    # load expense document
        try:
            expenses = pd.read_excel('Expenses.xlsx')
        except Exception as e:
            print('could not open expenses.xlsx:' + str(e))
            sys.exit()
    expenses = exchangeRates.merge(expenses, on ='date')
    rates = []
    for i in expenses['rate']:
        rates.append(float(i))
    CAD = rates*expenses['USD']
    expenses['cad'] = CAD.round(2)
    return expenses

def load(transformedData, config):

    destServer = config['CONFIG']['server']
    destDatabase = config['CONFIG']['database']
    dbPassword = config['CONFIG']['password']
    try:
       engine = create_engine(URL.create( drivername="postgresql",
                                           username=destDatabase,
                                           password=dbPassword,
                                           host=destServer,
                                           database="postgres"))
    except Exception as e:
        print('could not connect to database:' + str(e))
        sys.exit()
    try:
        transformedData.rename(columns={"USD": "usd"}, inplace=True)
        transformedData.to_sql('expenses',engine, if_exists='append',index=False)
    except Exception as e:
        print('could not write to database:' + str(e))
    print (transformedData)

config = configparser.ConfigParser()
try:
    config.read('config.ini')
    extractedData = extract(config)
    transformedData = transform(extractedData)
    load(transformedData, config)

except Exception as e:
    print('could not read configuration file:' + str(e))
    sys.exit()


