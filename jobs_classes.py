import datetime


class Vacancy:

    def __init__(self, name='', url='', description='', employer='', salary='',
                 date_published=datetime.datetime.now(), **kwargs):
        self.name = name
        self.url = url
        self.description = description
        # self.id = id_
        self.employer = employer
        self.salary = salary
        self.date_published = date_published

    def __lt__(self, other):
        return self.date_published < other.date_published

    def __str__(self):
        return f'Работодатель - {self.employer} \n;'

    def to_dict(self):
        return {
            'url': self.url,
            'name': self.name,
            'description': self.description,
            # 'id': self.id,
            'employer': self.employer,
            'salary': self.salary,
            'date_puplished': self.date_published
        }


class HHVacancy(Vacancy):
    """ HeadHunter Vacancy """

    def __repr__(self):
        return f"HH: {self.employer} \n;"

    def __str__(self):
        return f'HH:  {self.employer} \n;'

    @property
    def max_salary(self):
        """
        Вернуть количество вакансий от текущего сервиса.
        Получать количество необходимо динамически из файла.
        """
        value = self.salary
        return 0 if value is None else value

    def __gt__(self, other):
        return self.date_published > other.date_published

    @property
    def datetime(self):
        value = self.date_published
        return value


class SJVacancy(Vacancy):
    """ SuperJob Vacancy """

    def __repr__(self):
        return f"SJ: {self.name}, зарплата: {self.salary} руб/мес \n;"

    def __str__(self):
        return f'SJ: {self.name}, зарплата: {self.salary} руб/мес'

    @property
    def max_salary(self):
        value = self.salary
        return 0 if value is None else value

    def __gt__(self, other):
        return self.date_published > other.date_published

    @property
    def datetime(self):
        value = self.date_published
        return value
