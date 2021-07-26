import psycopg2
from insurello_sql_queries import *

def create_database():
    """
    - Creates and connects to the sparkifydb
    - Returns the connection and cursor to sparkifydb
    """
    
    # connect to postgres
    con = psycopg2.connect("user=postgres password='@Jgun220'");
    con.set_session(autocommit=True)
    cur = con.cursor()
    
    # create ETL database
    cur.execute("DROP DATABASE IF EXISTS jagan_insurello_etl")
    cur.execute("CREATE DATABASE jagan_insurello_etl")

    # close connection
    con.close()    
    
    # connect to ETL database
    con = psycopg2.connect("host=127.0.0.1 dbname=jagan_insurello_etl user=postgres password='@Jgun220'");
    con.set_session(autocommit=True)
    cur = con.cursor()
    
    return cur, con


def drop_tables(cur, con):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        cur.execute(query)
        con.commit()


def create_tables(cur, con):
    """
    Creates each table using the queries in `create_table_queries` list. 
    """
    for query in create_table_queries:
        cur.execute(query)
        con.commit()


def main():
    """
    - Drops (if exists) and Creates the sparkify database. 
    
    - Establishes connection with the sparkify database and gets
    cursor to it.  
    
    - Drops all the tables.  
    
    - Creates all tables needed. 
    
    - Finally, closes the connection. 
    """
    cur, con = create_database()
    
    drop_tables(cur, con)
    create_tables(cur, con)

    con.close()


if __name__ == "__main__":
    main()
