import sqlite3
import pandas as pd
from pandas.tseries.offsets import DateOffset

def func(x):
    try:
        import datetime as dt
        return x.date().strftime('%Y/%m/%d')
    except:
        return None

def getSQLiteConn():
    lite_conn = sqlite3.connect("hawaii.sqlite", check_same_thread=False)
    return lite_conn

def getFrameFromSqlLite(query):
    lite_conn = getSQLiteConn()
    results = pd.read_sql_query(query, lite_conn)

    if lite_conn:
        lite_conn.close()
    return results

def measurement_data(query):
    # query = "Select date, prcp from measurement where date >= date((select max(date) from measurement), '-1 year')"
    measurement_data = getFrameFromSqlLite(query)
    return measurement_data

def precipitation_data():
    query = "Select date, prcp from measurement where date >= date((select max(date) from measurement), '-1 year')"
    precipitation_data = measurement_data(query)
    precipitation_data = precipitation_data.groupby('date').sum().reset_index()
    # precipitation_data = precipitation_data.set_index(precipitation_data.date)
    # precipitation_data = precipitation_data.drop('date', axis=1)
    # # Sort the dataframe by date
    # df_date_prcp = df_date_prcp.sort_values('date', ascending=0)
    df_date_prcp = precipitation_data.rename(columns={'prcp': 'precipitation'})
    return df_date_prcp

def stations():
    query = "Select distinct s.name from measurement m inner join station s on m.station = s.station"
    stations_data = measurement_data(query)
    return stations_data,stations_data.count()

def tobs():
    query = "Select station, count(1) As Count from measurement group by station order by count(1) desc"
    frequency_of_stations = measurement_data(query)
    query_2 ="Select station, tobs, date from measurement"
    station_with_max_frequency=measurement_data(query_2)
    most_active_station = station_with_max_frequency[station_with_max_frequency['station'] == frequency_of_stations['station'][0]]
    most_active_station['date'] = pd.to_datetime(most_active_station['date'], errors='ignore')
    # Get max date
    dt_max = most_active_station.date.max()
    # Get 12 months old date
    dt_prev12Months = dt_max - DateOffset(months=12)
    # Get station data for 12 latest months
    station_data = most_active_station[most_active_station.date >= dt_prev12Months]
    station_data = station_data[['date','tobs']]
    station_data['date'] = station_data['date'].apply(func)
    return station_data,frequency_of_stations['station'][0]


def calc_temps(start_date, end_date):
    query = "Select date,tobs from measurement"
    frequency_of_stations = measurement_data(query)
    frequency_of_stations['date'] = pd.to_datetime(frequency_of_stations['date'], errors='ignore')
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)
    frequency_of_stations = frequency_of_stations[(frequency_of_stations.date >= start_date) & (frequency_of_stations.date <= end_date)]
    temp_calculation = {"Min":frequency_of_stations.tobs.min(),"Avg":frequency_of_stations.tobs.mean(),"Max":frequency_of_stations.tobs.max()}
    return temp_calculation,start_date.date(),end_date.date()

def calc_temps_with_oneDate(start_date):
    query = "Select date,tobs from measurement"
    frequency_of_stations = measurement_data(query)
    frequency_of_stations['date'] = pd.to_datetime(frequency_of_stations['date'], errors='ignore')
    start_date = pd.to_datetime(start_date)
    # end_date = pd.to_datetime(end_date)
    frequency_of_stations = frequency_of_stations[(frequency_of_stations.date >= start_date)]
    Single_date_calcul = {"Min":frequency_of_stations.tobs.min(),"Avg":frequency_of_stations.tobs.mean(),"Max":frequency_of_stations.tobs.max()}
    return Single_date_calcul,start_date.date()


if __name__== "__main__":
    precipitation_data = precipitation_data()
    print(dict(zip(precipitation_data.date,precipitation_data.precipitation)))
    print(precipitation_data.to_dict(orient='records'))
    stations_data,stations_count = stations()
    print(stations_data.to_dict('r'))
    print(stations_count[0])
    tobs_1,frequency_of_stations = tobs()
    print("frequency_of_stations",tobs_1.to_dict('r'))
    calc_temps ,a,b= calc_temps('2012-02-28', '2012-03-05')
    print(calc_temps,a,b)



