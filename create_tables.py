import configparser
import os
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

config = configparser.ConfigParser()
cfg_path = os.path.join(os.getcwd(),'dwh.cfg')
config.read(cfg_path)

def drop_tables(cur, conn):
    """
    Drop tables in Redshift as defined in sql_queries.py.

    :param cur: A cursor object that allows Python code to execute PostgreSQL commands.
    :param conn: A PostgreSQL database connection object that represents the database session.

    :returns: None
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Create tables in Redshift as defined in sql_queries.py.

    :param cur: A cursor object that allows Python code to execute PostgreSQL commands.
    :param conn: A PostgreSQL database connection object that represents the database session.

    :returns: None
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Read in the configs, connect to Redshift, drop any tables and create tables as defined in sql_queries.py."""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['REDSHIFT'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()