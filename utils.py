from io import BytesIO
from urllib.request import urlopen
from zipfile import ZipFile
import csv
import mysql.connector
import pandas as pd
import mysql.connector

def use_database():
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="123456",
        database="mydatabase"
    )
    return mydb

def fetch_equity():
    new_url = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
    df = pd.read_csv(new_url)
    df.to_csv('equity.csv')

def fetch_bhavcopy():
    zipurl = 'https://archives.nseindia.com/content/historical/EQUITIES/2022/OCT/cm21OCT2022bhav.csv.zip'
    with urlopen(zipurl) as zipresp:
        with ZipFile(BytesIO(zipresp.read())) as zfile:
            zfile.extractall()

def create_database():
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="123456"
    )
    cursor = mydb.cursor()
    cursor.execute("create database mydatabase")
    print('DataBase created Successfully')

def create_table_equity():
    mydb = use_database()

    cursor = mydb.cursor()
    query = """create table equity(
        id int,
        Symbol varchar(255) primary key,
        Name_of_company varchar(255),
        Series varchar(255),
        date_of_list varchar(255),
        paid_up_values int,
        market_lot int,
        isin_num varchar(255),
        face_value int
    );"""
    cursor.execute(query)
    print('table inserted successfully')

def create_table_bhavcopy():
    mydb = use_database()
    cursor = mydb.cursor()
    query = """create table bhavcopy(
        Symbol varchar(255),
        Series varchar(255),
        open float,
        high float,
        low float,
        close float,
        last float,
        prevclose float,
        tottrdq float,
        tottrdv float,
        timestamp varchar(255),
        totaltrades int,
        isin varchar(255),
        zero char(12)
    );"""

    cursor.execute(query)
    print('table inserted successfully')

def insert_data_into_equity():
    mydb = use_database()
    cursor = mydb.cursor()
    data = csv.reader(open('equity.csv'))
    header = next(data)
    print('importing files')
    query = """insert into equity(id,Symbol,Name_of_company,Series,date_of_list,paid_up_values,market_lot,isin_num,face_value)
    values(%s,%s,%s,%s,%s,%s,%s,%s,%s);"""
    for row in data:
        cursor.execute(query,row)
    mydb.commit()
    print('DONE')

def insert_data_into_bhavcopy():
    mydb = use_database()
    cursor = mydb.cursor()
    data = csv.reader(open('cm21OCT2022bhav.csv'))
    header = next(data)
    print('start')
    query = """insert into bhavcopy(Symbol,Series,open,high,low,close,last,prevclose,tottrdq,tottrdv,timestamp,totaltrades,isin,zero)
    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""
    for row in data:
        cursor.execute(query,row)
    mydb.commit()
    print('DONE')












