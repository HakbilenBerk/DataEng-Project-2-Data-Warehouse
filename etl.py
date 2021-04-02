import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Transfer JSON the data from S3 buckets to staging tables.
    args:
        cur (object): cursor instance for the database
        conn (object): connection instance for the database
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Insert the data from staging tables to fact and dimension tables.
    args:
        cur (object): cursor instance for the database
        conn (object): connection instance for the database
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Main function: Parse the configuration file (dwh.cfg), establish connection to the database using the information from config file. 
    Run the functions to create staging and afterwards fact and dimension tables. 
    Close the connection.
    """
    #Parse the config file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    #establish connection to the database using data from config file
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    #Call function to setup staging tables
    load_staging_tables(cur, conn)
    #Call function to setup fact and dimension tables
    insert_tables(cur, conn)
    
    #close the connection to the database
    conn.close()


if __name__ == "__main__":
    main()