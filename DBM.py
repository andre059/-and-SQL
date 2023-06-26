import psycopg2

from utils import formatting_vakansy


class DBManager:

    def __init__(self, database_name: str, params: dict):
        self.database_name = database_name
        self.params = params

    def create_database(self):
        """Создание базы данных и таблиц."""

        conn = psycopg2.connect(dbname='postgres', **self.params)
        conn.autocommit = True
        cur = conn.cursor()

        cur.execute(f"DROP DATABASE IF EXISTS {self.database_name}")
        cur.execute(f"CREATE DATABASE {self.database_name}")

        conn.close()

        conn = psycopg2.connect(dbname=self.database_name, **self.params)
        with conn.cursor() as cur:
            cur.execute("""CREATE TABLE IF NOT EXISTS employers
                            (
                                employer_id int PRIMARY KEY,
                                employer_name varchar(200) NOT NULL
                            )
                        """)

        with conn.cursor() as cur:
            cur.execute("""CREATE TABLE IF NOT EXISTS vacancies
                            (
                                vacancy_id int UNIQUE,
                                vacancy_name varchar(200) UNIQUE NOT NULL,
                                employer_id int,
                                employer_name varchar(200) NOT NULL,
                                description TEXT,
                                city varchar(50),
                                publication_date date,
                                url text,
                                solary int,
                                CONSTRAINT fk_vacancies_employer_id FOREIGN KEY(employer_id) REFERENCES employers(employer_id)
                            )
                        """)

        conn.commit()
        conn.close()

    def save_data_to_database(self: str, database_name: str, params: dict):
        """Запись в таблицы."""

        conn = psycopg2.connect(dbname=database_name, **params)

        with conn.cursor() as cur:
            for i in formatting_vakansy(self):
                cur.execute(
                        """
                        INSERT INTO employers (employer_id, employer_name)
                        VALUES (%s, %s)
                        RETURNING individual_employer-number
                        """, i)

        conn.close()

        with conn.cursor() as cur:
            for i in formatting_vakansy(self):
                cur.executemany(
                            """
                            INSERT INTO vacancies (vacancy_id, vacancy_name, employer_id, employer_name, description, 
                            city, publication_date, url, solary)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            RETURNING vacancy_id
                            """, i)

        conn.close()

    def get_companies_and_vacancies_count(self):
        """Получает список всех компаний и количество вакансий у каждой компании"""

        conn = psycopg2.connect(dbname=self.database_name, **self.params)
        with conn.cursor() as cur:
            cur.execute("""SELECT DISTINCT(employer_name), COUNT(*) FROM vacancies
                                    GROUP BY employer_name;""")

            rows = cur.fetchall()
            for row in rows:
                print(row)

        conn.close()

    def get_all_vacancies(self):
        """Получает список всех вакансий с указанием названия компании,
        названия вакансии и зарплаты и ссылки на вакансию."""

        conn = psycopg2.connect(dbname=self.database_name, **self.params)
        with conn.cursor() as cur:
            cur.execute("""SELECT employer_name, vacancy_name, salary, url FROM vacancies;""")

            rows = cur.fetchall()
            for row in rows:
                print(row)

        conn.close()

    def get_avg_salary(self):
        """Получает среднюю зарплату по вакансиям."""
        conn = psycopg2.connect(dbname=self.database_name, **self.params)
        with conn.cursor() as cur:
            cur.execute("""SELECT AVG(salary) AS avg_salary FROM vacancies;""")

            rows = cur.fetchall()
            for row in rows:
                print(row)

        conn.close()

    def get_vacancies_with_higher_salary(self):
        """Получает список всех вакансий, у которых зарплата выше средней по всем вакансиям."""
        conn = psycopg2.connect(dbname=self.database_name, **self.params)
        with conn.cursor() as cur:
            cur.execute("""SELECT * FROM vacancies WHERE salary > (SELECT AVG(salary) FROM vacancies);""")

            rows = cur.fetchall()
            for row in rows:
                print(row)

        conn.close()

    def get_vacancies_with_keyword(self):
        """Получает список всех вакансий, в названии которых содержатся переданные в метод слова, например python."""
        conn = psycopg2.connect(dbname=self.database_name, **self.params)
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM vacancies WHERE description LIKE '%python%';")

            rows = cur.fetchall()
            for row in rows:
                print(row)

        conn.close()
