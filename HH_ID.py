import requests
import json


class HH:
    """Класс с методами для HeadHunter"""

    def __init__(self, employer_id: str):
        """Получение вакансий с НН.ру"""
        self.employer_id = employer_id
        self.data = requests.get(f'https://api.hh.ru/vacancies/', params={'employer_id': employer_id}).json()
        self.vacancy_list = []

        self.vacancy_list.append(self.data)

    def get_request(self):
        """Возвращает вакансий с НН.ру"""
        with open("data_file_общий.json", "w", encoding="cp1251") as f:
            json.dump(self.vacancy_list, f)
        # print(self.vacancy_list)
        return self.vacancy_list


# search_keyword = "5375272"
# rt = HH("1073798")
# rt.get_request()
# rt.get_info()
# rt.get_vacancies()
