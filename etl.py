#!/usr/bin/env python
# coding: utf-8

# In[1]:


import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, most_played_song


# In[2]:


def load_staging_tables(cur, conn,q):
    cur.execute(q)
    for query in copy_table_queries:
        print("Staging table: ", query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn, q):
    cur.execute(q)
    for query in insert_table_queries:
        print("Loading table: ", query)
        cur.execute(query)
        conn.commit()
        
def find_most_played_song(cur, conn):
    rows = cur.execute(most_played_song)
    rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=['p_song_id', 'play_count'])
    
    return df


# In[3]:


def main():
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))
    
    DWH_DB                 = config.get("CLUSTER","DB_NAME")
    DWH_DB_USER            = config.get("CLUSTER","DB_USER")
    DWH_DB_PASSWORD        = config.get("CLUSTER","DB_PASSWORD")
    DWH_PORT               = config.get("CLUSTER","DB_PORT")
    DWH_ENDPOINT           = config.get("CLUSTER","HOST")
    
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(DWH_ENDPOINT, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT))
    cur = conn.cursor()
    
    q = "SET search_path TO dist_sparkify;"
    
    load_staging_tables(cur, conn, q)
    insert_tables(cur, conn, q)
    df = find_most_played_song(cur, conn)
    print(df)

    conn.close()


if __name__ == "__main__":
    main()


# In[ ]:





# In[ ]:




