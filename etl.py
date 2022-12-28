import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Description: This function is responsible for load data from AWS S3 storage
    Arguments:
        cur: curser from connection object.
        conn: psycopg2 connection object
    Returns:
        don't return any things. 
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Description: This function is responsible for insert loaded data into tables on AWS Redshift
    Arguments:
        cur: curser from connection object.
        conn: psycopg2 connection object
    Returns:
        don't return any things. 
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Description: This function is responsible for load data and insert it into AWS Redshift tables
    Arguments:
        empty_arguments
    Returns:
        don't return any things. 
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()