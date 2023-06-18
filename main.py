import os
from engine_classes import Engine, HH, SuperJob
from utils import check_search, get_top_vacancies_by_salary


def main():

    global all_vacancies
    path = os.path.join('data_file.json')
    connector = Engine.get_connector(path)  # создаем экземпляр класса Connector функцией get_connector из класса Engine

    search_keyword = input('Введите ключевое слово поиска')

    hh = HH(search_keyword)
    sj = SuperJob(search_keyword)
    if check_search(hh, sj):
        h = hh.vacancies
        s = sj.vacancies
        all_vacancies = h + s
        connector.insert(path, all_vacancies)

    top_count = input('Введите колличество выводимых на экран вакансий')
    if not top_count.isdigit() or int(top_count) <= 0:
        print('Необходимо ввести целое число ')

    else:
        top_count = int(top_count)

    top_1 = get_top_vacancies_by_salary(all_vacancies, top_count)
    print(top_1)


if __name__ == '__main__':
    main()
