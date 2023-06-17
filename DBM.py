import psycopg2
import self

from config import config


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
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute('CREATE TABL IF NOT EXISTS emploers'
                                '('
                                'employer_id PRIMARY KEY,'
                                'employer_name varchar(255) UNIQUE NOT NULL)')

                    cur.execute('CREATE TABL IF NOT EXISTS  vacansies'
                                '('
                                'vacancy_id int PRIMARY KEY,'
                                'vacancy_name varchar(255) UNIQUE NOT NULL,'
                                'employer_id int REFERENCES employers(employer_id) NOT NULL,'
                                'city varchar(255),'
                                'url text,'
                                'solary int)')
        finally:
            conn.close()

    def get_companies_and_vacancies_count(self):
        """Получает список всех компаний и количество вакансий у каждой компании"""
        pass

    def get_all_vacancies(self):
        """Получает список всех вакансий с указанием названия компании,
        названия вакансии и зарплаты и ссылки на вакансию."""
        pass

    def get_avg_salary(self):
        """Получает среднюю зарплату по вакансиям."""
        pass

    def get_vacancies_with_higher_salary(self):
        """Получает список всех вакансий, у которых зарплата выше средней по всем вакансиям."""
        pass

    def get_vacancies_with_keyword(self):
        """Получает список всех вакансий, в названии которых содержатся переданные в метод слова, например python."""
        pass