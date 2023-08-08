import psycopg2
import logging


from contextlib import contextmanager
from psycopg2 import OperationalError, DatabaseError
from random import randint

from faker import Faker

SUBJECTS = ["Математичний аналіз","Історія","Алхімія","Загальна алгебра","Квантова механіка"]


@contextmanager
def create_connection():
    try:
        conn = psycopg2.connect(host="localhost", database="hw6", user="postgres", password="112233")
        yield conn
        conn.close()
    except OperationalError as err:
        raise RuntimeError(f"Failed to connect: {err}")

def create_table(conn, sql_expression):
    c = conn.cursor()
    try:
        c.execute(sql_expression)
        conn.commit()
    except DatabaseError as e:
        logging.error(e)
        conn.rollback()
    finally:
        c.close()


def get_query_result(conn, sql_expression):
    cursor = conn.cursor()
    try:
        cursor.execute(sql_expression)
        rows = cursor.fetchall()

        return rows

    except DatabaseError as e:
        logging.error(e)
        conn.rollback()
    finally:
        cursor.close()


def create_all_tables():

    try:
        with create_connection() as conn:
            if conn is not None:

                # table creation
                # groups
                sql_expression = """CREATE TABLE IF NOT EXISTS groups (
                                        id INT PRIMARY KEY,
                                        name VARCHAR(10));"""

                create_table(conn, sql_expression)


                # students
                sql_expression = """CREATE TABLE IF NOT EXISTS students (
                id SERIAL PRIMARY KEY,
                name VARCHAR(50),
                group_id INT,
                FOREIGN KEY (group_id) REFERENCES groups (id)
                        ON DELETE SET NULL
                        ON UPDATE CASCADE);"""

                create_table(conn, sql_expression)


                # teachers
                sql_expression = """CREATE TABLE IF NOT EXISTS teachers (
                      id SERIAL PRIMARY KEY,
                      name VARCHAR(50));"""

                create_table(conn, sql_expression)

                # subjects
                sql_expression = """CREATE TABLE IF NOT EXISTS subjects (
                            id INT PRIMARY KEY,
                            name VARCHAR(30),
                            teacher_id INT,
                            FOREIGN KEY (teacher_id) REFERENCES teachers (id)
                                 ON DELETE SET NULL
                                 ON UPDATE CASCADE);"""

                create_table(conn, sql_expression)

                # points
                sql_expression = """CREATE TABLE IF NOT EXISTS points (
                            id SERIAL PRIMARY KEY,
                            student_id INT,
                            subjects_id INT,
                            point NUMERIC CHECK(point > 0 and point <= 100),
                            exam_date TIMESTAMP,
                            FOREIGN KEY (student_id) REFERENCES students (id)
                                ON DELETE SET NULL
                                ON UPDATE CASCADE,
                            FOREIGN KEY (subjects_id) REFERENCES subjects (id)
                                ON DELETE SET NULL
                                ON UPDATE CASCADE);"""

                create_table(conn, sql_expression)

            else:
                print("Error! cannot create the database connection.")
    except RuntimeError as e:
        logging.error(e)



if __name__ == '__main__':
    create_all_tables()