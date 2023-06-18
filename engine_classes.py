import json

import requests

from abc import ABC, abstractmethod
from connector import Connector

from jobs_classes import HHVacancy, SJVacancy


class Engine(ABC):
    @abstractmethod
    def get_request(self):
        pass

    @staticmethod
    def get_connector(file_name):
        """ Возвращает экземпляр класса Connector """
        return Connector(file_name)


class HH(Engine):
    """Класс с методами для HeadHunter"""
    URL = 'https://api.hh.ru/vacancies/'

    def __init__(self, search_key):
        super().__init__()
        self.params = {
            'text': f'{search_key}',
            'per_page': 100,
            'area': 113,
            'page': 0
        }

    def get_request(self):
        """Запрос вакансий API HeadHunter"""

        response = requests.get(self.URL, params=self.params)
        data = response.content.decode()
        response.close()
        js_hh = json.loads(data)

        with open("data_file_общий.json", "w", encoding="UTF-8") as f:
            json.dump(js_hh, f)
        # print(js_hh)
        return js_hh

    def get_info(self, data):
        """Структурирует получаемые из API данные по ключам"""
        info = {
            'from': 'HeadHunter',
            'name': data.get('name'),
            'url': data.get('alternate_url'),
            'description': data.get('snippet').get('responsibility'),
            'employer': {'id': data.get('employer').get('id'), 'name': data.get('employer').get('name')},
            'salary': data.get('salary', {}).get('from', 0),
            'date_published': data.get('published_at'),

        }

        return info

    def get_vacancies(self):
        """Записывает информацию о вакансии в список при наличии сведений о ЗП в рублях"""

        vacancies = []

        while len(vacancies) <= 50:
            data = self.get_request()
            items = data.get('items')
            if not items:  # Если нет вакансий на странице, выход из цикла
                break

            for vacancy in items:
                if vacancy.get('salary') is not None and vacancy.get('salary').get('currency') == 'RUR':
                    vacancies.append(self.get_info(vacancy))

            self.params['page'] += 1  # Увеличиваем значение параметра 'page' после обработки всех вакансий на
            # текущей странице
            with open("data_file_HH.json", "w", encoding="UTF-8") as f:
                json.dump(vacancies, f)

        return vacancies

    @property
    def vacancies(self):
        """Цикл создает список вакансий"""
        data_vacancies = self.get_vacancies()
        list_of_vacancies = []

        for data in data_vacancies:
            list_of_vacancies.append(HHVacancy(**data))
        return list_of_vacancies


class SuperJob(Engine):
    """Класс с методами для SuperJob"""
    URL = 'https://api.superjob.ru/2.0/vacancies/'

    def __init__(self, secret_key):
        super().__init__()
        self.HEADERS = None
        self.params = {'keywords': f'{secret_key}', 'count': 100, 'page': 0}

    def get_request(self):
        """Запрос вакансий API SuperJob"""
        self.HEADERS = {
            'Host': 'api.superjob.ru',
            'X-Api-App-Id':
                'sikret_kei',
            'Authorization': 'Bearer r.000000010000001.example.access_token',
            'Content-Type': 'application/x-www-form-urlencoded'
        }

        response = requests.get(self.URL, headers=self.HEADERS, params=self.params)
        data = response.content.decode()
        response.close()
        js_sj = json.loads(data)
        return js_sj

    def get_info_vacancy(self, data):
        """Структурирует получаемые из API данные, по ключам"""
        info = {
            'from': 'SuperJob',
            'name': data.get('profession'),
            'url': data.get('link'),
            'description': data.get('description'),
            'salary': data.get('payment_to'),
            'date_published': data.get('published_at')

        }
        return info

    def get_vacancies(self):
        """Записывает информацию о вакансии в список при наличии сведений о ЗП в рублях"""
        vacancies = []
        while len(vacancies) <= 50:
            data = self.get_request()
            objects = data.get('objects')
            if not objects:  # Если нет вакансий на странице, выход из цикла
                break
            for vacancy in objects:
                if vacancy.get('payment_to') != 0 and vacancy.get('currency') == 'rub':
                    vacancies.append(self.get_info_vacancy(vacancy))

            self.params['page'] += 1
            # Увеличиваем значение параметра 'page' после обработки всех вакансий на текущей странице
            with open("data_file_SJ.json", "w", encoding="UTF-8") as f:
                json.dump(vacancies, f)

        # print(vacancies)
        return vacancies

    @property
    def vacancies(self):
        """Цикл создает список вакансий"""
        vacancies_data = self.get_vacancies()
        sj_vacancies = []
        for data in vacancies_data:
            sj_vacancies.append(SJVacancy(**data))
        return sj_vacancies



search_keyword = 'Python'
rt = HH(search_keyword)
rt.get_request()
# rt.get_info()
rt.get_vacancies()
