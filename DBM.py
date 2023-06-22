from typing import Any

import psycopg2

from config import config
from utils import formatting_vakansy


class DBManager:
    params = config()

    def __init__(self, dbname: str, params: dict):
        self.dbname = dbname
        self.params = params

    def create_database(self):
        """Создание базы данных и таблиц."""

        conn = psycopg2.connect(dbname='postgres', **self.params)
        conn.autocommit = True
        cur = conn.cursor()

        cur.execute(f"DROP DATABASE IF EXISTS {self.dbname}")
        cur.execute(f"CREATE DATABASE {self.dbname}")

        conn.close()

        conn = psycopg2.connect(dbname=self.dbname, **self.params)
        with conn.cursor() as cur:
            cur.execute("""CREATE TABL IF NOT EXISTS employers
                            (
                                individual_employer-number PRIMARY KEY,
                                employer_id int,
                                employer_name varchar(200) UNIQUE NOT NULL
                            )
                        """)

        with conn.cursor() as cur:
            cur.execute("""CREATE TABL IF NOT EXISTS  vacancies
                            (
                                vacancy_id int PRIMARY KEY,
                                vacancy_name varchar(200) UNIQUE NOT NULL,
                                individual_employer-number int REFERENCES employers(employer_id) NOT NULL,
                                description TEXT,
                                city varchar(100),
                                url text,
                                solary int
                            )
                        """)

        conn.commit()
        conn.close()


    def get_companies_and_vacancies_count(self):
        """Получает список всех компаний и количество вакансий у каждой компании"""

        conn = psycopg2.connect(dbname=self.dbname, **self.params)
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABL IF NOT EXISTS  companies vacancies count
                            (
                                employer_name varchar(200) UNIQUE NOT NULL,
                                count_vacancies int 
                            )
                """)

        conn.commit()
        conn.close()


def get_all_vacancies(self):
    """Получает список всех вакансий с указанием названия компании,
    названия вакансии и зарплаты и ссылки на вакансию."""

    conn = psycopg2.connect(dbname=self.dbname, **self.params)
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABL IF NOT EXISTS list_of_all_vacancies
                (
                    employer_name varchar(200) UNIQUE NOT NULL,
                    vacancy_name varchar(200) UNIQUE NOT NULL,
                    solary int,
                    url text
                )
            INSERT INTO employers (employer_name, vacancy_name, solary, url)
            VALUES (%s, %s, %s, %s)
            """)
        cur.execute("SELECT * FROM list_of_all_vacancies")
    conn.close()


def get_avg_salary(self):
    """Получает среднюю зарплату по вакансиям."""
    conn = psycopg2.connect(dbname=self.dbname, **self.params)
    with conn.cursor() as cur:
        cur.execute(
            """
            ALTER TABLE list_of_all_vacancies ADD COLUMN avg_salary FLOAT;

            UPDATE list of all vacancies
            SET avg_salary = (
                SELECT AVG(salary)
                FROM vacancies
                WHERE vacancies.vacancy_id = list of all vacancies.vacancy_id
            """)

        cur.execute("SELECT * FROM list_of_all_vacancies")
    conn.close()


def get_vacancies_with_higher_salary(self):
    """Получает список всех вакансий, у которых зарплата выше средней по всем вакансиям."""
    conn = psycopg2.connect(dbname=self.dbname, **self.params)
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABL IF NOT EXISTS above_average AS SELECT * FROM vacancies 
            WHERE salary > (SELECT AVG(salary) FROM vacancies);
            """)

        cur.execute("SELECT * FROM above_average")
    conn.close()


def get_vacancies_with_keyword(self):
    """Получает список всех вакансий, в названии которых содержатся переданные в метод слова, например python."""
    conn = psycopg2.connect(dbname=self.dbname, **self.params)
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE selection_of_vacancies 
                (
                    id int PRIMARY KEY,
                    description TEXT,
                    salary INT
                );

            INSERT INTO selection_of_vacancies (id, description, salary)
            SELECT * FROM vacancies
            WHERE description LIKE '%python%'
            """)

        cur.execute("SELECT * FROM selection_of_vacancies")
    conn.close()

def save_data_to_database(data: str, database_name: str, params: dict):
    """Запись в таблицы."""

    conn = psycopg2.connect(dbname=database_name, **params)

    with conn.cursor() as cur:
        for i in formatting_vakansy(data):
            cur.execute(
                """
                INSERT INTO employers (employer_id, employer_name)
                VALUES (%s, %s)
                RETURNING individual_employer-number
                """, i)
            cur.execute("SELECT * FROM employers")
    conn.close()

    with conn.cursor() as cur:
        for i in formatting_vakansy(data):
            cur.execute(
                """
                INSERT INTO vacancies (vacancy_id, vacancy_name, individual_employer-number, description, city, url, 
                solary)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING vacancy_id
                """, i)
            cur.execute("SELECT * FROM vacancies")
    conn.close()


