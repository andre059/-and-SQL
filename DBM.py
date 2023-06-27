import psycopg2

from utils import formatting_vakansy


class DBManager:

    def __init__(self, database_name: str, params: dict, date):
        self.database_name = database_name
        self.params = params
        self.date = date

    def create_database(self):
        """Создание базы данных и таблиц."""

        conn = psycopg2.connect(dbname='postgres', **self.params)
        conn.autocommit = True
        cur = conn.cursor()

        cur.execute(f"DROP DATABASE IF EXISTS {self.database_name}")
        cur.execute(f"CREATE DATABASE {self.database_name}")

        conn.close()

        with psycopg2.connect(dbname=self.database_name, **self.params) as conn:
            with conn.cursor() as cur:
                cur.execute("""CREATE TABLE IF NOT EXISTS employers
                                (
                                    key_id SERIAL PRIMARY KEY,
                                    employer_id int UNIQUE,
                                    employer_name varchar(200) NOT NULL
                                )
                            """)
        conn.commit()

        with conn.cursor() as cur:
            cur.execute("""CREATE TABLE IF NOT EXISTS vacancies
                            (
                                key_id  int,
                                employer_id int,
                                employer_name varchar(200) NOT NULL,
                                vacancy_id int UNIQUE,
                                vacancy_name varchar(200) UNIQUE NOT NULL,
                                url text,
                                description TEXT,
                                city varchar(50),
                                publication_date date,
                                solary_from text,
                                solary_to int,
                                solary_currency text,
                                CONSTRAINT fk_vacancies_key_id FOREIGN KEY(key_id) REFERENCES employers(key_id)
                            )
                        """)

        conn.commit()
        conn.close()

    def save_data_to_database(self):
        """Запись в таблицы."""

        with psycopg2.connect(dbname=self.database_name, **self.params) as conn:
            with conn.cursor() as cur:
                for i in self.date:
                    # print(i)
                    cur.executemany(
                                """
                                INSERT INTO employers (employer_id, employer_name)
                                VALUES (%s, %s)
                                RETURNING employer_id
                                """, (i[0], i[1]))

            with conn.cursor() as cur:
                for i in self.date:
                    cur.executemany(
                                """
                                INSERT INTO vacancies (key_id, employer_id, employer_name, vacancy_id, vacancy_name,  
                                url, description, city, publication_date, solary_from, solary_to, solary_currency)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                RETURNING vacancy_id
                                """, i)
        conn.commit()
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
            cur.execute("""SELECT employer_name, vacancy_name, solary_to, url FROM vacancies;""")

            rows = cur.fetchall()
            for row in rows:
                print(row)

        conn.close()

    def get_avg_salary(self):
        """Получает среднюю зарплату по вакансиям."""
        conn = psycopg2.connect(dbname=self.database_name, **self.params)
        with conn.cursor() as cur:
            cur.execute("""SELECT AVG(solary_to) AS avg_solary_to FROM vacancies;""")

            rows = cur.fetchall()
            for row in rows:
                print(row)

        conn.close()

    def get_vacancies_with_higher_salary(self):
        """Получает список всех вакансий, у которых зарплата выше средней по всем вакансиям."""
        conn = psycopg2.connect(dbname=self.database_name, **self.params)
        with conn.cursor() as cur:
            cur.execute("""SELECT * FROM vacancies WHERE solary_tov > (SELECT AVG(solary_to) FROM vacancies);""")

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
