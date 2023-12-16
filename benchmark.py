import conf_.config as conf
import time
import os
import statistics
import psycopg2
import sqlite3
import duckdb
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float, func, desc, extract
from sqlalchemy.orm import sessionmaker, declarative_base


def read_file(engine):
    df = pd.read_csv(conf.DataBase, usecols=[i for i in range(0, 19)], sep=',')
    df.rename(columns={'Unnamed: 0': 'id'}, inplace=True)
    df.rename(columns={'VendorID': 'cab_type'}, inplace=True)
    df.rename(columns={'tpep_pickup_datetime': 'pickup_datetime'}, inplace=True)
    df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    df.to_sql(
        'trips',
        con=engine,
        if_exists='replace',
        index=False,
        chunksize=500000
    )


def write_file(lib, times):
    if not os.path.exists('time'):
        os.makedirs('time')
    left = 0
    for i in reversed(range(len(conf.DataBase) - 4)):
        if conf.DataBase[i] == "/":
            left = i + 1
    direct = 'time/' + lib + conf.DataBase[left:-4] + '.txt'
    with open(direct, "w") as file:
        for i in range(4):
            file.write("Q" + str(i + 1) + ": " + str(statistics.median(times[i])) + '\n')


def psycopg2_bench():
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/postgres', echo=False)
    read_file(engine)
    connection = psycopg2.connect(user="postgres", password="postgres", host="localhost", port="5432")
    cursor = connection.cursor()
    times = [[0] * conf.NumberOfTests for smt in range(4)]
    print("Start test with psycopg2")
    for i in range(conf.NumberOfTests):
        print("Test number:", i)
        start_time = time.time()
        cursor.execute('SELECT cab_type, count(*) FROM trips GROUP BY 1;')
        times[0][i] = time.time() - start_time
        start_time = time.time()
        cursor.execute('SELECT passenger_count, avg(total_amount) FROM trips GROUP BY 1;')
        times[1][i] = time.time() - start_time
        start_time = time.time()
        cursor.execute('SELECT passenger_count, extract(year from pickup_datetime), count(*) FROM trips GROUP BY 1, 2;')
        times[2][i] = time.time() - start_time
        start_time = time.time()
        cursor.execute('SELECT passenger_count, extract(year from pickup_datetime), round(trip_distance),count(*) FROM trips GROUP BY 1, 2, 3 ORDER BY 2, 4 desc;')
        times[3][i] = time.time() - start_time
    cursor.close()
    connection.close()
    write_file("psycopg2_", times)


def sqlite_bench():
    connection = sqlite3.connect('data/sqlite.db')
    cursor = connection.cursor()
    read_file(connection)
    times = [[0] * conf.NumberOfTests for smt in range(4)]
    print("Start test with SQLite")
    for i in range(conf.NumberOfTests):
        print("Test number:", i)
        start_time = time.time()
        cursor.execute('SELECT cab_type, count(*) FROM trips GROUP BY 1;')
        times[0][i] = time.time() - start_time
        start_time = time.time()
        cursor.execute('SELECT passenger_count, avg(total_amount) FROM trips GROUP BY 1;')
        times[1][i] = time.time() - start_time
        start_time = time.time()
        cursor.execute('SELECT passenger_count, strftime("%Y", pickup_datetime), count(*) FROM trips GROUP BY 1, 2;')
        times[2][i] = time.time() - start_time
        start_time = time.time()
        cursor.execute('SELECT passenger_count, strftime("%Y", pickup_datetime), round(trip_distance),count(*) FROM trips GROUP BY 1, 2, 3 ORDER BY 2, 4 desc;')
        times[3][i] = time.time() - start_time
    cursor.close()
    connection.close()
    write_file("sqlite_", times)


def duckdb_bench():
    connection = duckdb.connect('data/duckdb.db')
    cursor = connection.cursor()
    cursor.execute("DROP TABLE IF EXISTS trips; CREATE TABLE trips AS SELECT * FROM read_csv_auto('%(database)s');" % {"database": conf.DataBase})
    cursor = connection.cursor()
    times = [[0] * conf.NumberOfTests for smt in range(4)]
    print("Start test with DuckDB")
    for i in range(conf.NumberOfTests):
        print("Test number:", i)
        start_time = time.time()
        cursor.execute('SELECT "VendorID", count(*) FROM trips GROUP BY 1;')
        times[0][i] = time.time() - start_time
        start_time = time.time()
        cursor.execute('SELECT passenger_count, avg(total_amount) FROM trips GROUP BY 1;')
        times[1][i] = time.time() - start_time
        start_time = time.time()
        cursor.execute('SELECT passenger_count, extract(year from tpep_pickup_datetime), count(*) FROM trips GROUP BY 1, 2;')
        times[2][i] = time.time() - start_time
        start_time = time.time()
        cursor.execute('SELECT passenger_count, extract(year from tpep_pickup_datetime), round(trip_distance),count(*) FROM trips GROUP BY 1, 2, 3 ORDER BY 2, 4 desc;')
        times[3][i] = time.time() - start_time
    cursor.close()
    connection.close()
    write_file("duckdb_", times)


def pandas_bench():
    trips = pd.read_csv(conf.DataBase)
    trips.rename(columns={'VendorID': 'cab_type'}, inplace=True)
    trips.rename(columns={'tpep_pickup_datetime': 'pickup_datetime'}, inplace=True)
    trips['pickup_datetime'] = pd.to_datetime(trips['pickup_datetime'])
    trips['tpep_dropoff_datetime'] = pd.to_datetime(trips['tpep_dropoff_datetime'])
    times = [[0] * conf.NumberOfTests for smt in range(4)]
    print("Start test with Pandas")
    for i in range(conf.NumberOfTests):
        print("Test number:", i)
        start_time = time.time()
        trips.groupby('cab_type').size()
        times[0][i] = time.time() - start_time
        start_time = time.time()
        trips.groupby('passenger_count')['total_amount'].mean()
        times[1][i] = time.time() - start_time
        start_time = time.time()
        trips.assign(date=trips["pickup_datetime"].dt.year).groupby(["passenger_count", "date"]).size()
        times[2][i] = time.time() - start_time
        start_time = time.time()
        trips.assign(date=trips["pickup_datetime"].dt.year, dist=trips["trip_distance"].round()).groupby(["passenger_count", "date", "dist"]).size().to_frame('size').reset_index().sort_values(['date', 'size'], ascending=[True,  False])
        times[3][i] = time.time() - start_time
    write_file("pandas_", times)


def sqlalchemy_bench():
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/postgres', echo=False)
    base = declarative_base()

    class trips(base):
        __tablename__ = 'trips'

        id = Column(Integer, primary_key=True)
        cab_type = Column(Integer)
        pickup_datetime = Column(DateTime)
        tpep_dropoff_datetime = Column(DateTime)
        passenger_count = Column(Float)
        trip_distance = Column(Float)
        RatecodeID = Column(Float)
        store_and_fwd_flag = Column(String)
        PULocationID = Column(Integer)
        DOLocationID = Column(Integer)
        payment_type = Column(Integer)
        fare_amount = Column(Float)
        extra = Column(Float)
        mta_tax = Column(Float)
        tip_amount = Column(Float)
        tolls_amount = Column(Float)
        improvement_surcharge = Column(Float)
        total_amount = Column(Float)
        congestion_surcharge = Column(Float)
        airport_fee = Column(Float)
        Airport_fee = Column(Float)

    read_file(engine)
    Session = sessionmaker(autoflush=False, autocommit=False, bind=engine)
    trip = Session()
    times = [[0] * conf.NumberOfTests for smt in range(4)]
    print("Start test with SQLAlchemy")
    for i in range(conf.NumberOfTests):
        print("Test number:", i)
        start_time = time.time()
        trip.query(trips.cab_type, func.count().label('count')).group_by(trips.cab_type).all()
        times[0][i] = time.time() - start_time
        start_time = time.time()
        trip.query(trips.passenger_count, func.avg(trips.total_amount)).group_by(trips.passenger_count).all()
        times[1][i] = time.time() - start_time
        start_time = time.time()
        trip.query(trips.passenger_count, extract('year', trips.pickup_datetime), func.count().label('count')).group_by(trips.passenger_count, extract('year', trips.pickup_datetime)).all()
        times[2][i] = time.time() - start_time
        start_time = time.time()
        trip.query(trips.passenger_count, extract('year', trips.pickup_datetime), func.round(trips.trip_distance), func.count().label('count')).group_by(trips.passenger_count, extract('year', trips.pickup_datetime), func.round(trips.trip_distance)).order_by(extract('year', trips.pickup_datetime), desc(func.count().label('count'))).all()
        times[3][i] = time.time() - start_time
    trip.close()
    write_file("sqlalchemy_", times)


df = {}
if not os.path.exists('data'):
    os.makedirs('data')
if conf.psycopg2_using:
    print("Psycopg2 runs")
    psycopg2_bench()
    print("Psycopg2 done")
if conf.SQLite_using:
    print("SQLite runs")
    sqlite_bench()
    print("SQLite done")
if conf.DuckDB_using:
    print("DuckDB runs")
    duckdb_bench()
    print("DuckDB done")
if conf.Pandas_using:
    print("Pandas runs")
    pandas_bench()
    print("Pandas done")
if conf.SQLAlchemy_using:
    print("SQLAlchemy runs")
    sqlalchemy_bench()
    print("SQLAlchemy done")
