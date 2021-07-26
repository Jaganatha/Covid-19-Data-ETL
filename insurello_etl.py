import os
import json
import glob
import psycopg2
import pandas as pd
import datetime
from glom import glom
import re
import numpy as np
from insurello_sql_queries import *


def process_json(filepath):
    with open(filepath,'r') as f:
        data = json.loads(f.read())
        data_keys = data.keys()
    return data, data_keys


def create_dataframe_country_info(data_keys, data_df):
    data_df_list_of_dict = []
    for i in data_keys:
        temp_dict = {"country": None, "population": None, "sq_km_area": None, "life_expectancy": None, "elevation_in_meters": None, "continent":None, "abbreviation": None,
                    "location": None, "iso": None, "capital_city": None}
        temp_dict['country'] = str(i)
        if "population" in data_df[i]["All"]:
            temp_dict["population"] = data_df[i]['All']["population"]
        if "sq_km_area" in data_df[i]["All"]:
            temp_dict["sq_km_area"] = data_df[i]['All']["sq_km_area"]
        if "life_expectancy" in data_df[i]["All"]:
            temp_dict["life_expectancy"] = data_df[i]['All']["life_expectancy"]
        if "elevation_in_meters" in data_df[i]["All"]:
            temp_dict["elevation_in_meters"] = data_df[i]['All']["elevation_in_meters"]
        if "continent" in data_df[i]["All"]:
            temp_dict["continent"] = data_df[i]['All']["continent"]
        if "abbreviation" in data_df[i]["All"]:
            temp_dict["abbreviation"] = data_df[i]['All']["abbreviation"]
        if "location" in data_df[i]["All"]:
            temp_dict["location"] = data_df[i]['All']["location"]
        if "iso" in data_df[i]["All"]:
            temp_dict["iso"] = data_df[i]['All']["iso"]
        if "capital_city" in data_df[i]["All"]:
            temp_dict["capital_city"] = data_df[i]['All']["capital_city"]
        data_df_list_of_dict.append(temp_dict)
    return pd.DataFrame(data = data_df_list_of_dict, columns = ["country", "population", "sq_km_area", "life_expectancy", "elevation_in_meters", "continent", "abbreviation", "location", "iso", "capital_city"])


def create_dataframe_vaccined_info(data_keys, data_df):
    data_df_list_of_dict = []
    for i in data_keys:
        temp_dict = {'country': None,'administered': None, 'people_vaccinated': None, 'people_partially_vaccinated': None, 'updated': None}
        temp_dict['country'] = str(i)
        if "administered" in data_df[i]["All"]:
            temp_dict["administered"] = data_df[i]['All']["administered"]
        if "people_vaccinated" in data_df[i]["All"]:
            temp_dict["people_vaccinated"] = data_df[i]['All']["people_vaccinated"]
        if "people_partially_vaccinated" in data_df[i]["All"]:
            temp_dict["people_partially_vaccinated"] = data_df[i]['All']["people_partially_vaccinated"]
        if "updated" in data_df[i]["All"]:
            temp_dict["updated"] = data_df[i]['All']["updated"]
        data_df_list_of_dict.append(temp_dict)
    return pd.DataFrame(data = data_df_list_of_dict, columns = ["country", "administered", "people_vaccinated", "people_partially_vaccinated", "updated"])


def create_dataframe_dates(data_keys, data_df):
    data_df_list_of_list = []
    for i in data_keys:
        date_keys = data_df[i]["All"]["dates"].keys()
        for j in date_keys:
            date_list = []
            date_list.append(i)
            date_list.append(j)
            date_list.append(data_df[i]["All"]["dates"][j])
            data_df_list_of_list.append(date_list)
    return pd.DataFrame(data = data_df_list_of_list, columns = ["country", "dates", "confirmed_cases"])
         
    
def clean_dataframes(data_df):
    data_df["country"] = data_df["country"].apply(lambda x: re.sub('[^A-Za-z0-9]+', ' ', x))
    return data_df

def process_country_info_etl(cur, data_df):
    for i, row in data_df.iterrows():
        cur.execute(country_table_insert, (i, row[0], row[1], row[2], row[3], str(row[4]), row[5], row[6], row[7], row[8], row[9]))
    print("{0} rows inserted in country_info_tables".format(len(data_df)))

    
    
def process_vaccined_info_etl(cur, data_df):
    for i, row in data_df.iterrows():
        cur.execute(vaccine_country_select.format((row[0])))
        results = cur.fetchone()
        
        if results:
            country_id = results[0]
        else:
            country_id = None
        
        cur.execute(vaccine_table_insert, (i, country_id, row[1], row[2], row[3], row[4]))
    print("{0} rows inserted in vaccined_info_tables".format(len(data_df)))

    
def process_confirmed_cases_etl(cur, data_df):
    for i, row in data_df.iterrows():
    
        cur.execute(dates_country_select.format((row[0])))
        results = cur.fetchone()
    
        if results:
            country_id = results[0]
        else:
            country_id = None
    
        cur.execute(confirmed_table_insert, (i, country_id, row[1], row[2]))
    print("{0} rows inserted in confirmed_cases_tables".format(len(data_df)))
      
          
def main():
    con = psycopg2.connect("host=127.0.0.1 dbname=jagan_insurello_etl user=postgres password='@Jgun220'")
    con.set_session(autocommit=True)
    cur = con.cursor()
    
    data, data_keys = process_json('covid_19_confirmed.json')
    data_vac, data_keys_vac = process_json('covid_19_vaccined.json')
    
    data_df_country_info = create_dataframe_country_info(data_keys, data)
    data_df_vaccined_info = create_dataframe_vaccined_info(data_keys_vac, data_vac)
    data_df_confirmed_cases = create_dataframe_dates(data_keys,data)
    
    clean_dataframes_list = [data_df_country_info, data_df_vaccined_info, data_df_confirmed_cases]
    
    for i in clean_dataframes_list:
        i = clean_dataframes(i)
    
    process_country_info_etl(cur, data_df_country_info)
    process_vaccined_info_etl(cur, data_df_vaccined_info)
    process_confirmed_cases_etl(cur, data_df_confirmed_cases)
    print("ETL Complete")

    con.close()


if __name__ == "__main__":
    main()