import pyodbc
import smtplib
import sys
import urllib
import requests
import json
from datetime import datetime
import pandas as pd
from pandas import DataFrame
from time import time
from time import sleep
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from sqlalchemy import exc
from sqlalchemy import create_engine
from sqlalchemy.dialects import registry


def main():
    stop_information = []

    # Arriva Bus routes
    lineNames = ["158", "259", "N279", "121", "279", "313", "349", "394", "347", "W6", "102", "141", "329", "340",
                 "675", "N102", "19", "41", "123", "149", "230", "318", "341", "N19", "N41"]

    lineNames = ','.join([str(elem) for elem in lineNames])

    # calling eta's for filtered by Abellio routes
    api = "http://countdown.api.tfl.gov.uk/interfaces/ura/instant_V1?LineID=" + lineNames + "&ReturnList=StopID," \
                                                                                            "StopPointName,LineName," \
                                                                                            "DirectionID," \
                                                                                            "RegistrationNumber," \
                                                                                            "EstimatedTime,StopCode2," \
                                                                                            "TripID,Latitude,Longitude"
    retry_strategy = Retry(
        total=250,
        backoff_factor=20,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)
    http = requests.Session()
    http.mount("https://", adapter)
    http.mount("http://", adapter)
    data = http.get(api).text
    #print(data)

    # data wrangling to create list from json response
    for line in data.splitlines():
        line = line[1:]
        line = line[:-1]
        line = line.replace("\"", "")
        stop_info = list(line.split(","))
        print(stop_info)
        stop_information.append(stop_info)

    # formatting UTC  to time
    for row in stop_information[1:]:
        predicted_time = int(row[10]) / 1000.0

        predicted_time = datetime.fromtimestamp(predicted_time)
        row[10] = predicted_time
        # row[6] is the TripID , #row[7] is the registrationNumber
        row[0] = row[2] + row[3] + row[6] + row[7] + row[8] + row[9]

    OutputDF = DataFrame(stop_information,
                         columns=['UniqueID', 'StopPointName', 'StopID', 'StopCode2', 'Latitude',
                                  'Longitude', 'LineID', 'DirectionID', 'TripID',
                                  'VehicleID', 'EstimatedTime'])

    # Adding TimeOfRequest
    OutputDF['TimeOfRequest'] = datetime.now()
    # getting rid of first row
    OutputDF = OutputDF[1:]
    OutputDF.to_csv('testing.csv')

    # SQL database connection string
    server = 'embracent-pbsa-sqlserver.database.windows.net'
    database = 'Bus Operators'
    user = 'embracentPBSA'
    password = 'embracent_PBSA21'
    driver = '{ODBC Driver 17 for SQL Server}'
    ipaddress = '192.168.0.88'  # this is db IP address
    port = 5439

    conn = f"""Driver={driver};Server=tcp:{server},1433;Database={database};
    Uid={user};Pwd={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"""

    params = urllib.parse.quote_plus(conn)
    conn_str = 'mssql+pyodbc:///?autocommit=true&odbc_connect={}'.format(params)
    engine = create_engine(conn_str, echo=True)
    print('connection is ok')

    # everything 30s will call api again and sink into staging datatable then execute sql command
    # first clear staging table
    engine.execute('delete from EstimatedTimeStaging')
    # Loading data into Staging table
    OutputDF.to_sql('EstimatedTimeStaging', con=engine, index=False, if_exists='append', chunksize=100, method='multi')

    # Remove Duplicates from API response
    sql_dedupe = 'WITH cte AS ( SELECT UniqueID,StopPointName,StopID,StopCode2,Latitude,Longitude,' \
                 'LineID,DirectionID,TripID,VehicleID,' \
                 'EstimatedTime,TimeOfRequest, ROW_NUMBER() OVER ( PARTITION BY UniqueID ORDER BY UniqueID ) row_num ' \
                 'FROM EstimatedTimeStaging ) DELETE FROM cte WHERE row_num > 1; '

    engine.execute(sql_dedupe)
    print("dedpupe success")

    # Snowsql insert/ update code
    sql_put = ('Merge into EstimatedTime aS v using EstimatedTimeStaging as src on v.UniqueID = '
               'src.UniqueID WHEN MATCHED AND v.EstimatedTime <>src.EstimatedTime THEN UPDATE SET '
               'v.EstimatedTime= src.EstimatedTime,v.TimeOfRequest = src.TimeOfRequest when not '
               'matched then insert values( UniqueID,StopPointName,StopID,StopCode2,Latitude,'
               'Longitude,LineID,DirectionID,TripID,VehicleID,EstimatedTime,TimeOfRequest);')

    engine.execute(sql_put)
    print("insert success")

    # After every merge, remove records in Temp table that are there due to glitch in TFL tracking system
    sql_validate = 'DELETE FROM EstimatedTimeTemp where UniqueID in (select ' \
                   'UniqueID from EstimatedTime) '
    engine.execute(sql_validate)

    print("validate success")

    # checking which vehicles have disappeared from the QSI point and inserting into a temp table
    sql_check = ('insert into EstimatedTimeTemp select * from '
                 'EstimatedTime where (UniqueID not in(select UniqueID '
                 'from EstimatedTimeStaging) and UniqueID not in(select '
                 'UniqueID from EstimatedTimeTemp))')

    engine.execute(sql_check)
    print("check success")

    # remove record form the master table
    engine.execute('delete from EstimatedTime where UniqueID in(select UniqueID '
                   'from EstimatedTimeTemp)')

    print("remove record success")

    # checking if the current time is 3 mins after EstimatedTime
    sql_insert_arrived = 'MERGE INTO EstimatedTimeArrived AS v USING EstimatedTimeTemp AS src ON ' \
                         'v.UniqueID = src.UniqueID WHEN MATCHED THEN UPDATE SET v.TimeOfRequest = ' \
                         'src.TimeOfRequest when not matched AND DATEDIFF(MINUTE,src.EstimatedTime,TODATETIMEOFFSET(' \
                         'CURRENT_TIMESTAMP, -60))>3 then insert values( UniqueID, ' \
                         'StopPointName, StopID, StopCode2,Latitude, Longitude,LineID, DirectionID,TripID,VehicleID,' \
                         'EstimatedTime,TimeOfRequest); '

    engine.execute(sql_insert_arrived)
    print("insert into temp table success")

    # remove records from temp table that have arrived already
    engine.execute('delete from EstimatedTimeTemp where UniqueID in(select '
                   'UniqueID from EstimatedTimeArrived)')

    print("remove records that have arrived success")

    # clear staging table
    engine.execute('delete from EstimatedTimeStaging')
    print("delete success")


while True:
    main()
