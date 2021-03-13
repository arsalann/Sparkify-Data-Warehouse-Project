  
import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries



# Drop tables if they already exist in AWS Redshift
# Tables: songplays, users, artists, songs, time
def drop_tables(cur, conn):
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error - drop_table_queries: " + query)
            print(e)
    print("Drop Table Query: Successful!")


# Create tables in AWS Redshift for sparkifydb
# Tables: songplays, users, artists, songs, time
def create_tables(cur, conn):
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error - create_table_queries: " + query)
            print(e)
    print("Create Table Query: Successful!")


# Connect to AWS Redshit and create new database (sparkifydb)
# Drop existing tables and create new tables
# Read AWS credentials from the configuration file
    # host: Redshift Cluster address
    # dbname: database name
    # user: database username
    # password: database password
    # port: database connection port
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # conn: psql database connection (sparkifydb)
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    # cur: database cursor
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()