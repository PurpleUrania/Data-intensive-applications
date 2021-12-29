import mysql.connector as connector
import mysql.connector.pooling as pooling
from concurrent.futures import ThreadPoolExecutor
import time

CONFIG = {
  'user': 'root',
  'password': 'poiu',
  'host': '127.0.0.1',
  'database': 'db',
}

TOTAL = 100_000
SQL = "INSERT INTO posts (id, text) VALUES (%s, %s)"

def measure_time(func, name):
    start = time.time()
    func()
    end = time.time()
    print(name, ":", end - start)

def drop_table(conn):
    cur = conn.cursor()
    cur.execute("DELETE FROM posts")
    conn.commit()
    cur.close()

def insert(conn, cursor, i):
    try:
        cursor.execute(SQL, (i, f"Post {i}"))
        conn.commit()
    except Exception as e:
        print(e)

def insert_and_close(conn, i):
    cursor = conn.cursor()
    insert(conn, cursor, i)
    cursor.close()
    conn.close()
def insert_and_close_with_pool(pool, i):
    insert_and_close(pool.get_connection(), i)
    
## SYNC INSERT OF 100_000 entries
def run_sync_and_measure():
    conn = connector.connect(**CONFIG)
    drop_table(conn)
    measure_time(lambda: run_sync(conn), "sync")
    conn.close()

def run_sync(conn):
    cursor = conn.cursor()
    for i in range(TOTAL):
        insert(conn, cursor, i)
    cursor.close()

## ASYNC INSERT OF 100_000 entries
def run_async_and_measure(n_threads):
    conn = connector.connect(**CONFIG)
    drop_table(conn)
    conn.close()

    measure_time(lambda: run_async(n_threads), f"async {n_threads}")

def run_async(n_threads):
    with ThreadPoolExecutor(max_workers=n_threads) as executor:
        for i in range(TOTAL):
            conn = connector.connect(**CONFIG)
            executor.submit(insert_and_close, conn, i)
            

# ASYNC WITH CONNECTION POOL
def run_async_with_pool(n_threads):
    pool = pooling.MySQLConnectionPool(pool_size = n_threads,**CONFIG)
    with ThreadPoolExecutor(max_workers=n_threads) as executor:
        for i in range(TOTAL): 
            executor.submit(insert_and_close_with_pool, pool, i)
           

def run_async_with_pool_and_measure(n_threads):
    conn = connector.connect(**CONFIG)
    drop_table(conn)
    conn.close()
    measure_time(lambda: run_async_with_pool(n_threads), f"async & pool {n_threads}")

if __name__ == "__main__":
    #run_sync_and_measure() # sync : 1129.7238166332245

    #run_async_and_measure(20)
    #run_async_and_measure(50)
    #run_async_and_measure(100)


    #run_async_with_pool_and_measure(20) 
    #run_async_with_pool_and_measure(32) 
