import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Run the drop tables queries imported above
    args:
        cur (object): cursor instance for the database
        conn (object): connection instance for the database
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Run the create tables queries imported above
    args:
        cur (object): cursor instance for the database
        conn (object): connection instance for the database
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Main function: Parse the configuration file (dwh.cfg), establish connection to the database using the information from config file. 
    Run the functions to drop the tables if any exists. Then run the function to create the tables.
    Close the connection.
    """
    
    #Parse the config file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    #Establish connection to the database using the information from config file
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    #drop the tables in any exists
    drop_tables(cur, conn)
    #Create the tables
    create_tables(cur, conn)
    
    #Close the connection to the database
    conn.close()


if __name__ == "__main__":
    main()