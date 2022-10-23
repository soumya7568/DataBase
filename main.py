from utils import *
from io import BytesIO
from urllib.request import urlopen
from zipfile import ZipFile
import csv
import mysql.connector
import pandas as pd
import mysql.connector

def create_and_insert():
    fetch_equity()
    fetch_bhavcopy()
    create_database()
    create_table_equity()
    create_table_bhavcopy()
    insert_data_into_equity()
    insert_data_into_bhavcopy()

#create_and_insert()

db = use_database()
cursor = db.cursor()
query = """select equity.* from equity inner join (select Symbol from bhavcopy order by (close-open)/open desc limit 25) as e 
where equity.Symbol=e.Symbol;"""

cursor.execute(query)
result = cursor.fetchall()
for row in result:
    print(row)


