import psycopg2


class DBManager:

    def __init__(self, database_name: str, params: dict, date, word):
        self.database_name = database_name
        self.params = params
        self.date = date
        self.word = word

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
                cur.execute(
                            """
                                CREATE TABLE IF NOT EXISTS employers
                                    (
                                    employer_id int PRIMARY KEY,
                                    employer_name varchar(200) NOT NULL
                                    )
                            """
                            )
        conn.commit()

        with conn.cursor() as cur:
            cur.execute(
                        """
                            CREATE TABLE IF NOT EXISTS vacancies
                                (
                                key_id SERIAL,
                                employer_id int,  
                                vacancy_name varchar(200) NOT NULL,
                                url text,
                                description TEXT,
                                city varchar(50),
                                publication_date date,
                                solary_from text,
                                solary_to int,
                                solary_currency text,
                                CONSTRAINT fk_vacancies_employer_id FOREIGN KEY(employer_id) 
                                REFERENCES employers(employer_id)
                                )
                        """
                        )

        conn.commit()
        conn.close()

    def save_data_to_database(self):
        """Запись в таблицы."""

        with psycopg2.connect(dbname=self.database_name, **self.params) as conn:
            with conn.cursor() as cur:
                for i in self.date:
                    cur.execute(
                                """
                                    INSERT INTO employers (employer_id, employer_name)
                                    VALUES (%s, %s)
                                    ON CONFLICT DO NOTHING
                                    RETURNING employer_id
                                """,
                                (i[0], i[1])
                                )

            with conn.cursor() as cur:
                for i in self.date:
                    cur.execute(
                                """
                                INSERT INTO vacancies (employer_id, vacancy_name, url, description, city, 
                                publication_date, solary_from, solary_to, solary_currency)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                                RETURNING vacancy_name
                                """,
                                (i[0], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9])
                                )
        conn.commit()
        conn.close()

    def get_companies_and_vacancies_count(self):
        """Получает список всех компаний и количество вакансий у каждой компании"""

        print(f"\n{1} -- компания, количество вакансий\n")

        conn = psycopg2.connect(dbname=self.database_name, **self.params)
        with conn.cursor() as cur:
            cur.execute(
                        """
                            SELECT e.employer_name, COUNT(v.employer_id) AS vacancy_count
                            FROM employers e
                            JOIN vacancies v ON v.employer_id = e.employer_id
                            GROUP BY e.employer_name;
                        """
                        )

            rows = cur.fetchall()
            for row in rows:
                print(row)
            print()

        conn.close()

    def get_all_vacancies(self):
        """Получает список всех вакансий с указанием названия компании,
        названия вакансии и зарплаты и ссылки на вакансию."""

        print(f"{2} -- компания, название вакансии, зарплата min и max, ссылки на вакансию\n")

        conn = psycopg2.connect(dbname=self.database_name, **self.params)
        with conn.cursor() as cur:
            cur.execute(
                        """
                            SELECT employer_name, vacancy_name, solary_from, solary_to, url 
                            FROM vacancies v
                            JOIN employers e ON e.employer_name = e.employer_name;
                        """
                        )
            rows = cur.fetchall()
            for row in rows:
                print(row)
            print()

        conn.close()

    def get_avg_salary(self):
        """Получает среднюю зарплату по вакансиям."""

        print(f"{3} -- средняя зарплата по вакансиям\n")

        conn = psycopg2.connect(dbname=self.database_name, **self.params)
        with conn.cursor() as cur:
            cur.execute(
                        """
                        SELECT AVG(solary_to) AS avg_solary_to FROM vacancies;
                        """
                        )

            rows = cur.fetchall()
            for row in rows:
                if row[0] is not None:
                    print(" %.2f" % row)
                else:
                    print(row)
            print()

        conn.close()

    def get_vacancies_with_higher_salary(self):
        """Получает список всех вакансий, у которых зарплата выше средней по всем вакансиям."""

        print(f"{4} -- вакансий, у которых зарплата выше средней\n")

        conn = psycopg2.connect(dbname=self.database_name, **self.params)
        with conn.cursor() as cur:
            cur.execute(
                        """
                            SELECT vacancy_name FROM vacancies WHERE solary_to > (SELECT AVG(solary_to) 
                            FROM vacancies);
                        """
                        )

            rows = cur.fetchall()
            for row in rows:
                print(row)
            print()

        conn.close()

    def get_vacancies_with_keyword(self):
        """Получает список всех вакансий, в названии которых содержатся переданные в метод слова, например python."""

        print(f"{5} -- вакансий, в названии которых содержатся переданные в метод слово\n")

        conn = psycopg2.connect(dbname=self.database_name, **self.params)
        with conn.cursor() as cur:
            cur.execute(
                        f"SELECT * FROM vacancies WHERE description LIKE '%{self.word}%';"
                        )

            rows = cur.fetchall()
            for row in rows:
                print(row)

        conn.close()
