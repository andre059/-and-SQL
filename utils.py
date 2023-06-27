import json
from datetime import datetime
from HH_ID import HH


def key_by_order(filename, order):
    with open(filename, 'r', encoding='UTF-8') as f:
        data = json.load(f)

    # Извлекаем ключ по порядку
    key = list(data.keys())[order - 1]

    return key


def formatting_vakansy(key: str):
    hh = HH(key)
    vacansy_list = hh.get_request()
    vacansy_hh = []
    for i in vacansy_list[0]["items"]:
        employer_id = i['employer']['id']
        employer_name = i['employer']['name']
        vacancy_name = i['name']
        url = i['apply_alternate_url']
        description = i['snippet']['requirement'], i['snippet']['responsibility']
        city = i['area']['name']
        publication_date = i['published_at']
        solary_from = i['salary']['from'] if i['salary'] else None
        solary_to = i['salary']['to'] if i['salary'] else None
        solary_currency = i['salary']['currency'] if i['salary'] else None

        date_obj = datetime.strptime(publication_date, '%Y-%m-%dT%H:%M:%S%z')
        formatted_date = date_obj.strftime('%d.%m.%Y %H:%M:%S')

        data_dict = employer_id, employer_name, vacancy_name, url, description, city, formatted_date, solary_from, \
            solary_to, solary_currency
        vacansy_hh.append(data_dict)

    # print(vacansy_hh)
    return vacansy_hh


# formatting_vakansy("1073798")
