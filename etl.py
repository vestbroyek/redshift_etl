import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load data into staging tables in Redshift as defined in sql_queries.py.

    :param cur: A cursor object that allows Python code to execute PostgreSQL commands.
    :param conn: A PostgreSQL database connection object that represents the database session.

    :returns: None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Load raw data from staging tables in Redshift as defined in sql_queries.py.

    :param cur: A cursor object that allows Python code to execute PostgreSQL commands.
    :param conn: A PostgreSQL database connection object that represents the database session.

    :returns: None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Read in the configs, connect to Redshift, load staging tables and perform transformations as defined in sql_queries.py."""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['REDSHIFT'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()