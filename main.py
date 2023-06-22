from utils import get_key_by_order


def main():
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

    get_key_by_order(filename, order)


if __name__ == '__main__':
    main()
