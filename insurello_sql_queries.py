

country_info_drop = ("""DROP TABLE IF EXISTS country_info CASCADE;""")

vaccined_info_drop = ("""DROP TABLE IF EXISTS vaccined_info""")

confirmed_cases_drop = ("""DROP TABLE IF EXISTS confirmed_cases""")


# CREATE TABLES

country_table_create = ("""CREATE TABLE IF NOT EXISTS country_info (country_id int PRIMARY KEY, country varchar, population numeric null, sq_km_area numeric null, life_expectancy numeric null, elevation_in_meters varchar null,
continent char(100) null, abbreviation char(2) null, location char(100) null, iso numeric null, capital_city char(50) null);""")

vaccine_table_create = ("""CREATE TABLE IF NOT EXISTS vaccined_info (vaccine_id int PRIMARY KEY , country_id int, administered bigint null, people_vaccinated int null, people_partially_vaccinated int null, updated timestamp null, 
CONSTRAINT fk_country FOREIGN KEY(country_id) REFERENCES country_info(country_id));""")

confirmed_cases_table_create = ("""CREATE TABLE IF NOT EXISTS confirmed_cases (case_id int PRIMARY KEY, country_id int, date date null, confirmed_cases int null, CONSTRAINT fk_confirm_country
FOREIGN KEY(country_id) REFERENCES country_info(country_id));""")


# INSERT RECORDS

country_table_insert = ("""INSERT INTO country_info (country_id, country, population, sq_km_area, life_expectancy, elevation_in_meters,
continent, abbreviation, location, iso, capital_city) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s) """)

vaccine_table_insert = ("""INSERT INTO vaccined_info (vaccine_id, country_id, administered, people_vaccinated, people_partially_vaccinated, updated) VALUES (%s,%s,%s,%s,%s,%s) """)

confirmed_table_insert = ("""INSERT INTO confirmed_cases (case_id, country_id, date, confirmed_cases) VALUES (%s,%s,%s,%s)""")


# FIND COUNTRY_ID SELECT FOR ETL

dates_country_select = ("""select country_id from country_info where country_info.country = '{0}'""")

vaccine_country_select = ("""select country_id from country_info where country_info.country = '{0}'""")


# QUERY LISTS

create_table_queries = [country_table_create, vaccine_table_create, confirmed_cases_table_create]
drop_table_queries = [country_info_drop, vaccined_info_drop, confirmed_cases_drop]
select_country_queries = [dates_country_select, vaccine_country_select]