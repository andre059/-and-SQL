from engine_classes import HH, SuperJob
from jobs_classes import HHVacancy, SJVacancy


def check_search(hh: HH, sj: SuperJob) -> bool:
    """Проверка на существование вакансии"""
    return hh.get_request()['items'] != [] or sj.get_request()['objects'] != []


def get_only_str_vacancies(data):
    """Складывает в список все вакансии экземпляров класссов HHVacancy и SJVacancy"""
    str_vacancies_list = []
    hh = HHVacancy
    sj = SJVacancy
    for item in data:
        if item['from'] == 'HeadHunter':
            str_vacancies_list.append(hh(item))
        else:
            str_vacancies_list.append(sj(item))
    return str_vacancies_list


def get_top_vacancies_by_salary(vacancies, top_count=5):
    """Сортирует топ вакансий по зарплате"""
    return sorted(vacancies, key=lambda i: i.max_salary, reverse=True)[:top_count]


def get_top_vacancies_by_date(vacancies, top_count=5):
    """Сортирует топ вакансий по датам"""
    return sorted(vacancies, key=lambda i: i.Vacancy.date_published, reverse=True)[:top_count]