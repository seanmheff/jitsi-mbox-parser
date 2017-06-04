import psycopg2

conn_str = """
    dbname='scraper'
    host='localhost'
    user='postgres'
    password='postgres'
"""

try:
    conn = psycopg2.connect(conn_str)
    print(conn.encoding)
except Exception as e:
    print("I am unable to connect to the database.")
    print(e)
    exit(0)


def __perform__(*args, method=None):
    cursor = conn.cursor()
    try:
        result = cursor.execute(*args)
        conn.commit()
        if method is not None:
            return getattr(cursor, method)()
        else:
            return result
    except Exception as e:
        conn.rollback()
        print("ERROR performing query: " + str(args))
        print(e)
    finally:
        cursor.close()


def create_table():
    __perform__("""CREATE TABLE messages (
        id              VARCHAR         PRIMARY KEY,
        email           VARCHAR         NOT NULL,
        name            VARCHAR         NOT NULL,
        date            TIMESTAMPTZ     NOT NULL,
        subject         VARCHAR         NOT NULL,
        in_reply_to     VARCHAR,
        content         TEXT            NOT NULL
    )""")


def drop_table():
    __perform__('DROP TABLE IF EXISTS messages')


drop_table()
create_table()
