import requests
import psycopg2

"""ТЗ не понравилось и решил, чтобы пользователь сам вводил компании которые ему интересны переменная companies, """
"""Если принципиально, могу переделать чутка"""

companies = list(map(str,input('Введите компании через пробел, которые Вам интересны ').split()))


URL_COMPANY = 'https://api.hh.ru/employers/'

conn = psycopg2.connect(host='localhost', database='coursework', user='postgres', password='12345')
def load_company(url):
    url_company = {}
    list_company = []
    count = 0
    for i in companies:
        count+=1
        response = requests.get(f'{url}', params={'per_page': 1, 'text': i, 'only_with_vacancies': True}).json()
        list_company.append(response)
        for item in response.get('items'):
            url_company[count] = item.get('vacancies_url')
    return list_company, url_company

"""Словарь url_company хранить ссылки на вакансии и ключ-номер, чтобы в дальнейшем использовать ключ использовать как company_id, а через ссылку выводить вакансии"""

list_company, url_company = load_company(URL_COMPANY)

"""Словарь list_company в дальнейшем будет использоваться для заполнения таблицы company"""

try:
    with conn:
        with conn.cursor() as cur:
            """Заполняем таблицу company данными"""
            count_1 = 0
            for i in list_company:
                count_1+=1
                cur.execute('INSERT INTO companies VALUES (%s,%s) ON CONFLICT (company_id) DO NOTHING',(count_1, i['items'][0]['name']))
                conn.commit()

            """заполняем таблицу job данными"""
            count = 0
            for i, url in url_company.items():
                data = requests.get(f'{url}', params={'per_page': 100, 'only_with_salary':True}).json()
                for item in data.get('items', []):
                    count += 1
                    cur.execute('INSERT INTO job VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (job_id) DO NOTHING',(count, i, item.get('name'),
                                                                                                              item['salary']['from'] if item.get('salary') else None,
                                                                                                              item['salary']['to'] if item.get('salary') else None,
                                                                                                              item.get('alternate_url')))
                    conn.commit()
finally:
    conn.close()



class DBManager:
    def __init__(self, conn):
        self.conn = psycopg2.connect(conn)
        self.a = 'Диспетчер чатов, удаленно'

    def get_all_vacancies(self):
        """медот для вывода всех строк из обоих таблиц"""
        with self.conn.cursor() as cur:
            cur.execute('SELECT* FROM companies INNER JOIN job USING(company_id)')
            data = cur.fetchall()
            conn.close()
        return data
    def get_avg_salary(self):
        """медот для нахождения средней зп"""
        with self.conn.cursor() as cur:
            cur.execute('SELECT AVG(solary_do) as solary_avg_do FROM job')
            data_1 = cur.fetchall()
            conn.close()
        return data_1
    def get_vacancies_with_higher_salary(self):
        """вывод всех вакансий где зп больше средней"""
        with self.conn.cursor() as cur:
            cur.execute('SELECT * FROM job WHERE solary_do > (SELECT AVG(solary_do) FROM job)')
            data_2 = cur.fetchall()
            conn.close()
        return data_2

    def get_vacancies_with_keyword(self):
        '''вывод вакансий по названию'''
        """в качестве аргумента задана обычная переменная c вакансией 'Диспетчер чатов, удаленно'. но можно сделать и инпут"""
        with self.conn.cursor() as cur:
            cur.execute(f"SELECT * FROM job WHERE job_title = '{self.a}'")
            data_3 = cur.fetchall()
            conn.close()
        return data_3

    def get_companies_and_vacancies_count(self):
        with self.conn.cursor() as cur:
            cur.execute(f'SELECT companies.name_company, COUNT(job.job_id) FROM companies LEFT JOIN job ON companies.company_id = job.company_id GROUP BY companies.name_company')
            data_4 = cur.fetchall()
            conn.close()
        return data_4

db_manager = DBManager("dbname='coursework' user='postgres' password='12345' host='localhost'")
print(db_manager.get_all_vacancies())
print(db_manager.get_avg_salary())
print(db_manager.get_vacancies_with_higher_salary())
print(db_manager.get_vacancies_with_keyword())
print(db_manager.get_companies_and_vacancies_count())