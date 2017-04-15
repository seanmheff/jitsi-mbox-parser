import psycopg2
import datetime

conn_str = """
    dbname='jitsi_mail_dev'
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


#####################
### Create Tables ###
#####################

def create_tables():
    create_people_table()
    create_threads_table()
    create_messages_table()

def create_people_table():
    __perform__("""CREATE TABLE people (
        email           VARCHAR         PRIMARY KEY,
        name            VARCHAR
    )""")

def create_threads_table():
    __perform__("""CREATE TABLE threads (
        id              VARCHAR         PRIMARY KEY,
        title           VARCHAR         NOT NULL,
        person_id       VARCHAR         NOT NULL,
        date            TIMESTAMPTZ     NOT NULL,
        FOREIGN KEY     (person_id)     REFERENCES people (email)
    )""")

def create_messages_table():
    __perform__("""CREATE TABLE messages (
        id              VARCHAR         PRIMARY KEY,
        date            TIMESTAMPTZ     NOT NULL,
        in_reply_to     VARCHAR,
        content         TEXT            NOT NULL,
        person_id       VARCHAR         NOT NULL,
        thread_id       VARCHAR         NOT NULL,
        FOREIGN KEY     (person_id)     REFERENCES people (email),
        FOREIGN KEY     (thread_id)     REFERENCES threads (id)
    )""")



##########################
### Insert Sample Data ###
##########################

def insert_sample_data():
    query = 'INSERT INTO people (name, email) VALUES (%s, %s) ON CONFLICT DO NOTHING'
    data = ('rick', 'rick@rick.com') 
    __perform__(query, data)
    data = ('morty', 'morty@morty.com') 
    __perform__(query, data)

    query = 'INSERT INTO threads (id, title, person_id, date) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING'
    data = ('message_1_id', 'subject', 'rick@rick.com', datetime.datetime.now()) 
    __perform__(query, data)

    query = 'INSERT INTO messages (id, date, in_reply_to, content, person_id, thread_id) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING'
    data = ('message_1_id', datetime.datetime.now(), 'bad_id', 'well what is the craic', 'rick@rick.com', 'message_1_id') 
    __perform__(query, data)
    data = ('message_2_id', datetime.datetime.now(), 'message_1_id', 'fuck all now', 'morty@morty.com', 'message_1_id') 
    __perform__(query, data)
    data = ('message_3_id', datetime.datetime.now(), 'message_2_id', '3 deep', 'rick@rick.com', 'message_1_id') 
    __perform__(query, data)



###################
### Drop Tables ###
###################

def drop_tables():
    __perform__('DROP TABLE IF EXISTS people, threads, messages CASCADE')



######################
### Tables Getters ###
######################

def get_thread_for_message(id):
    '''
    query = """
        WITH RECURSIVE tree AS (
            SELECT *
            FROM messages
            WHERE messages.id = %s
        UNION ALL
            SELECT e.*
            FROM messages e
            INNER JOIN tree tree ON e.id = tree.in_reply_to
        )
        SELECT * FROM tree
    """
    return __perform__(query, (id,), method='fetchall')[-1]
    '''
    query = 'SELECT thread_id from messages WHERE id like %s'
    return __perform__(query, (id,), method='fetchone')

def get_message_by_id(id):
    query = 'SELECT * from messages WHERE id like %s'
    return __perform__(query, (id,), method='fetchone')

def get_person(email):
    query = 'SELECT * from people WHERE email like %s'
    return __perform__(query, (email,), method='fetchone')



######################
### Tables Setters ###
######################

def create_message(id, date, in_reply_to, content, person_id, thread_id):
    query = 'INSERT INTO messages (id, date, in_reply_to, content, person_id, thread_id) VALUES (%s, %s, %s, %s, %s, %s)'
    __perform__(query, (id, date, in_reply_to, content, person_id, thread_id))

def create_person(name, email):
    query = 'INSERT INTO people (name, email) VALUES (%s, %s) ON CONFLICT DO NOTHING'
    __perform__(query, (name, email))

def create_thread(message_id, title, person_id, date):
    query = 'INSERT INTO threads (id, title, person_id, date) VALUES (%s, %s, %s, %s)'
    __perform__(query, (message_id, title, person_id, date))
