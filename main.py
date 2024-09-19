import sqlite3
from multiprocessing import Process
import os
import time
import random

TABLE_NAME = 'trx_dispatcher'
TABLE_DEFINITION = f""" CREATE TABLE {TABLE_NAME} (
                        record_id INTEGER PRIMARY KEY,
                        pacs008_trx_ref CHAR(40) NOT NULL UNIQUE,
                        camt056_trx_ref CHAR(40) NOT NULL UNIQUE
                    ); """


def create_db_structure(conn):
    print(f"Drop db table {TABLE_NAME}")
    conn.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    print(f"Create db table {TABLE_NAME}")
    conn.execute(TABLE_DEFINITION)


def main():
    with sqlite3.connect('test.db') as conn:
        try:
            conn.execute(f'select * from {TABLE_NAME} limit 1')
        except sqlite3.OperationalError:
            print("Creating db structure")
            create_db_structure(conn)


def insert_trx():
    data_person_name = [(x, x) for x in range(100)]
    for x, y in data_person_name:
        time.sleep(random.choice([0.1, 0.3]))
        with sqlite3.connect('test.db', timeout=3*60) as conn:
            print(f'PID: {os.getpid()}, INSERTING ({x}, {y})')
            try:
                conn.execute(
                    f'INSERT INTO {TABLE_NAME}(pacs008_trx_ref, camt056_trx_ref) VALUES ({x},{y})'
                )
            except sqlite3.IntegrityError as err:
                print(f"!!! ERROR PID: {os.getpid()}, {err}")


if __name__ == '__main__':
    main()
    # insert_trx()
    p1 = Process(target=insert_trx)
    p2 = Process(target=insert_trx)
    p1.start()
    p2.start()
    p1.join()
    p2.join()
