import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Description: This function is responsible for dropping tables on AWS Redshift
    Arguments:
        cur: curser from connection object.
        conn: psycopg2 connection object
    Returns:
        don't return any things. 
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Description: This function is responsible for creation tables on AWS Redshift
    Arguments:
        cur: curser from connection object.
        conn: psycopg2 connection object
    Returns:
        don't return any things. 
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Description: This function is responsible for dropping and creation tables on AWS Redshift
    Arguments:
        empty arguments
    Returns:
        don't return any things. 
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()