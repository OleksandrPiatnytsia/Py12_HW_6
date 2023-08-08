import psycopg2
import logging

from datetime import datetime, timedelta
from contextlib import contextmanager
from psycopg2 import OperationalError, DatabaseError
from random import randint, choice

from faker import Faker

fake = Faker("uk_UA")

MIN_POINTS_COUNT = 7
MAX_POINTS_COUNT = 20

TEACHERS_COUNT = 5

STUDENTS_COUNT = 50

SUBJECTS = [
    "Математичний аналіз",
    "Історія",
    "Алхімія",
    "Загальна алгебра",
    "Квантова механіка",
    "Чисельні методи",
    "Теорія імовірності",
]

GROUPS = ["group 1", "group 2", "group 3"]




def generate_random_workday():

    random_day = datetime(datetime.now().year, 1, 1) + timedelta(
        days=randint(1, datetime.now().timetuple().tm_yday))

    # Перевірка, чи день не вихідний
    while random_day.weekday() >= 5:
        random_day += timedelta(days=1)

    random_time = timedelta(hours=randint(8, 17), minutes=randint(0, 59))
    random_datetime = random_day + random_time

    # Перетворення на формат TIMESTAMP
    formatted_datetime = random_datetime.strftime("%Y-%m-%d %H:%M:%S")

    return formatted_datetime


@contextmanager
def create_connection():
    try:
        conn = psycopg2.connect(
            host="localhost", database="hw6", user="postgres", password="112233"
        )
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


def insert_data(conn, sql_expression: str, params: tuple):
    c = conn.cursor()
    try:
        c.execute(sql_expression, params)
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
                                        id SERIAL PRIMARY KEY,
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
                            id SERIAL PRIMARY KEY,
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
                            subject_id INT,
                            point NUMERIC CHECK(point > 0 and point <= 100),
                            exam_date TIMESTAMP,
                            FOREIGN KEY (student_id) REFERENCES students (id)
                                ON DELETE SET NULL
                                ON UPDATE CASCADE,
                            FOREIGN KEY (subject_id) REFERENCES subjects (id)
                                ON DELETE SET NULL
                                ON UPDATE CASCADE);"""

                create_table(conn, sql_expression)

            else:
                print("Error! cannot create the database connection.")
    except RuntimeError as e:
        logging.error(e)


def insert_data_to_DB():
    try:
        with create_connection() as conn:
            if conn is not None:
                # insert data
                # TEACHERS
                tchr_data = get_query_result(conn, "SELECT id FROM teachers LIMIT 1")
                if not tchr_data:
                    for i in range(1, TEACHERS_COUNT + 1):
                        sql_expression = """INSERT INTO teachers(name) VALUES(%s);"""
                        insert_data(conn, sql_expression, (fake.name(),))

                # GROUPS
                grp_data = get_query_result(conn, "SELECT id FROM groups LIMIT 1")
                if not grp_data:
                    for group in GROUPS:
                        sql_expression = """INSERT INTO groups(name) VALUES(%s);"""
                        insert_data(conn, sql_expression, (group,))

                # SUBJECTS
                subj_data = get_query_result(conn, "SELECT id FROM subjects LIMIT 1")
                if not subj_data:
                    for subject in SUBJECTS:
                        tchr_tuple = get_query_result(conn, "SELECT id FROM teachers")

                        sql_expression = (
                            """INSERT INTO subjects(name,teacher_id) VALUES(%s,%s);"""
                        )
                        insert_data(conn, sql_expression, (subject, choice(tchr_tuple)))

                # STUDENTS
                std_data = get_query_result(conn, "SELECT id FROM students LIMIT 1")
                if not std_data:
                    for i in range(STUDENTS_COUNT):
                        groupe_tuple = get_query_result(conn, "SELECT id FROM groups")

                        sql_expression = (
                            """INSERT INTO students(name,group_id) VALUES(%s,%s);"""
                        )
                        insert_data(
                            conn, sql_expression, (fake.name(), choice(groupe_tuple))
                        )

                # POINTS
                std_data = get_query_result(conn, "SELECT id FROM points LIMIT 1")
                if not std_data:
                    students_id_tuple = get_query_result(
                        conn, "SELECT id FROM students"
                    )
                    subj_id_tuple = get_query_result(conn, "SELECT id FROM subjects")

                    for student_id in students_id_tuple:
                        for _ in range(1, randint(MIN_POINTS_COUNT,MAX_POINTS_COUNT)):
                            sql_expression = """INSERT INTO points(student_id,subject_id, point, exam_date) 
                                                VALUES(%s,%s,%s,%s);"""
                            insert_data(
                                conn,
                                sql_expression,
                                (student_id, choice(subj_id_tuple), randint(1, 100), generate_random_workday()),
                            )

            else:
                print("Error! cannot create the database connection.")
    except RuntimeError as e:
        logging.error(e)


if __name__ == "__main__":
    create_all_tables()
    insert_data_to_DB()
