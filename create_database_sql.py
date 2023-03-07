from sqlalchemy import Table, Column, Integer, String, MetaData
from sqlalchemy import create_engine
from sqlalchemy import insert
import csv
import sqlite3

engine = create_engine('sqlite:///cleanbase.db', echo=True)

meta = MetaData()

# create table cleanstations
cleanstations = Table(
   'cleanstations', meta,
   Column('station', String, primary_key=True),
   Column('latitude', String),
   Column('longitude', String),
   Column('elevation', String),
   Column('name', String),
   Column('country', String),
   Column('state', String)
)

meta.create_all(engine)

# create table cleanmeasure
cleanmeasure = Table(
   'cleanmeasure', meta,
   Column('station', String, primary_key=True),
   Column('date', String),
   Column('precip', String),
   Column('tobs', Integer),

)
meta.create_all(engine)

#insert data to database cleandatabase from csvfile clean_stations.csv
with open('clean_stations.csv', 'r') as f:
    reader = csv.reader(f)
    conn = engine.connect()
    for row in reader:
        conn.execute(cleanstations.insert().values(
            station=row[0],
            latitude=row[1],
            longitude=row[2],
            elevation=row[3],
            name=row[4],
            country=row[5],
            state=row[6]
        ))
    conn.close()

#insert data to database cleandatabase from csvfile clean_measure.csv
with open('clean_measure.csv', 'r') as f:
    reader = csv.reader(f)
    conn = engine.connect()
    for row in reader:
        conn.execute(cleanmeasure.insert().values(
            station=row[0],
            date=row[1],
            precip=row[2],
            tobs=row[3],
        ))
    conn.close()

#select data from database cleanbase
def select_data():
    with sqlite3.connect('cleanbase.db') as db:
        cursor = db.cursor()
        query = '''SELECT *
        FROM cleanstations'''
        cursor.execute(query)
        results = cursor.fetchall()
        return results