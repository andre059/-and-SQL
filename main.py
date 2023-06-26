from DBM import DBManager
from config import config
from utils import key_by_order, formatting_vakansy


def main():
    params = config()

    print(f"\n\
          1 - APPTRIX\n\
          2 - ТД ГраСС\n\
          3 - АО НТЦ Атлас\n\
          4 - СБЕР\n\
          5 - OPTIMAL CITY Technologies\n\
          6 - Интеллектуальные системы\n\
          7 - ProAnalytics\n\
          8 - Технические Системы\n\
          9 - Янтарь-Айти\n\
          10 - Нордавинд")

    filename = 'employer_id.json'
    order = int(input('Укажите № компании, данные которой вы хотели посмотреть'))

    key = key_by_order(filename, order)
    date = formatting_vakansy(key)

    rt = DBManager('postgres2', params)
    rt.create_database()
    rt.save_data_to_database(date)
    rt.get_companies_and_vacancies_count()
    rt.get_all_vacancies()
    rt.get_avg_salary()
    rt.get_vacancies_with_higher_salary()
    rt.get_vacancies_with_keyword()


if __name__ == '__main__':
    main()
