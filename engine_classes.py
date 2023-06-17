import json
import requests


class HH:
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

        with open("data_file.json", "w", encoding="UTF-8") as f:
            json.dump(js_hh, f)

        print(js_hh)
        return js_hh

    def get_info(self, data):
        """Структурирует получаемые из API данные по ключам"""
        info = {
            'from': 'HeadHunter',
            'name': data.get('name'),
            'url': data.get('alternate_url'),
            'description': data.get('snippet').get('responsibility'),
            'salary': data.get('salary', {}).get('from', 0),
            'date_published': data.get('published_at'),

        }

        # print(info)
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
            list_of_vacancies.append(**data)
        return list_of_vacancies


search_keyword = 'Python'
rt = HH(search_keyword)
rt.get_request()
# rt.get_info()
# rt.get_vacancies()
