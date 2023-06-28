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
          10 - Нордавинд\n")

    filename = 'employer_id.json'
    order = int(input('Укажите № компании, данные которой вы хотели посмотреть'))
    word = input("Укажите слово, которое должно встречаться в вакансиях")

    key = key_by_order(filename, order)
    date = formatting_vakansy(key)

    dbm = DBManager('postgres2', params, date, word)
    dbm.create_database()
    dbm.save_data_to_database()
    dbm.get_companies_and_vacancies_count()
    dbm.get_all_vacancies()
    dbm.get_avg_salary()
    dbm.get_vacancies_with_higher_salary()
    dbm.get_vacancies_with_keyword()


if __name__ == '__main__':
    main()
