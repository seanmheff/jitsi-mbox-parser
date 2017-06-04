import psycopg2
import datetime

conn_str = """
    dbname='scraper'
    host='localhost'
    user='postgres'
    password='postgres'
"""

try:
    conn = psycopg2.connect(conn_str)
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


def create_message(id, email, name, date, subject, in_reply_to, content):
    query = """INSERT INTO messages (id, email, name, date, subject, in_reply_to, content) VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING"""
    __perform__(query, (id, email, name, date, subject, in_reply_to, content))
