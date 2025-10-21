import requests
import psycopg2
from datetime import datetime
import time

def clear_table_completely(conn_params):
    """Очистка таблицы"""
    try:
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()
        
        # Полная очистка таблицы
        cursor.execute("TRUNCATE TABLE hh_vacancies RESTART IDENTITY;")
        conn.commit()
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f" Ошибка очистки таблицы: {e}")

def parse_hh_vacancies_api():
    """Парсинг вакансий"""
    
    # Данные для подключения к БД
    conn_params = {
        "host": "postgres",
        "port": 5432,
        "database": "airflow", 
        "user": "airflow",
        "password": "airflow"
    }
    
    all_vacancies_data = []
    
    try:
        
        # ОЧИСТКА ТАБЛИЦЫ ПЕРЕД НАЧАЛОМ
        clear_table_completely(conn_params)
        
        # Ключевые слова для фильтрации - Data Analysts + Data Engineers
        target_keywords = [
            'аналитик данных',
            'data analyst', 
            'дата-аналитик',
            'data analysis',
            'аналитик data',
            'analyst data',
            'инженер данных',
            'data engineer',
            'дата-инженер',
            'data engineering',
            'инженер data'
        ]
        
        # Счетчики для статистики
        total_vacancies_found = 0
        data_analyst_count = 0
        data_engineer_count = 0
        with_salary_count = 0
        without_salary_count = 0
        page = 0
        
        # Проходим по всем страницам
        while True:
            # Параметры запроса к API
            params = {
                'text': '"аналитик данных" OR "data analyst" OR "инженер данных" OR "data engineer"',
                'area': 1,                  # Москва
                'per_page': 100,            # Максимум на странице
                'page': page,
                'order_by': 'publication_time'  # Сначала новые
            }
            
            headers = {
                'User-Agent': 'HH-Parser-Analytics/1.0'
            }
            
            # Делаем запрос к API HH.ru
            response = requests.get('https://api.hh.ru/vacancies', 
                                  params=params, 
                                  headers=headers,
                                  timeout=15)
            
            print(f" Статус ответа: {response.status_code}")
            
            if response.status_code != 200:
                print(f" Ошибка API: {response.text}")
                break
            
            data = response.json()
            
            # Первая страница - получаем общее количество
            if page == 0:
                total_vacancies_found = data.get('found', 0)
                print(f"Всего найдено вакансий: {total_vacancies_found}")
                print(f"Страниц для обработки: {data.get('pages', 0)}")
            
            current_page_items = data.get('items', [])
            print(f"Загружено вакансий на странице {page + 1}: {len(current_page_items)}")
            
            # Если на странице нет вакансий - заканчиваем
            if not current_page_items:
                print("Достигнут конец списка вакансий")
                break
            
            # Обрабатываем вакансии на текущей странице
            page_vacancies_count = 0
            for i, item in enumerate(current_page_items):
                try:
                    vacancy_name = item.get('name', '').lower()
                    vacancy_url = item.get('alternate_url', '')
                    
                    # Строгая проверка: должна содержать хотя бы одно целевое ключевое слово
                    is_target_vacancy = any(keyword in vacancy_name for keyword in target_keywords)
                    
                    if not is_target_vacancy:
                        continue
                    
                    # Определяем тип вакансии
                    is_data_analyst = any(keyword in vacancy_name for keyword in [
                        'аналитик данных', 'data analyst', 'дата-аналитик', 'data analysis'
                    ])
                    is_data_engineer = any(keyword in vacancy_name for keyword in [
                        'инженер данных', 'data engineer', 'дата-инженер', 'data engineering'
                    ])
                    
                    # Компания
                    employer = item.get('employer', {})
                    company_name = employer.get('name', 'Компания не указана')
                    
                    # Зарплата
                    salary_info = item.get('salary')
                    has_salary = salary_info is not None
                    
                    if salary_info:
                        salary_from = salary_info.get('from')
                        salary_to = salary_info.get('to')
                        currency = salary_info.get('currency', 'RUR')
                        
                        # Конвертируем валюту
                        currency_map = {'RUR': 'руб.', 'USD': 'USD', 'EUR': 'EUR'}
                        currency = currency_map.get(currency, currency)
                        with_salary_count += 1
                        
                    else:
                        salary_from = None
                        salary_to = None
                        currency = None
                        without_salary_count += 1
                    
                    # Обновляем счетчики
                    if is_data_analyst:
                        data_analyst_count += 1
                    if is_data_engineer:
                        data_engineer_count += 1
                    
                    # Дата публикации из API
                    published_at_str = item.get('published_at')
                    if published_at_str:
                        published_at = datetime.strptime(published_at_str, '%Y-%m-%dT%H:%M:%S%z')
                    else:
                        published_at = datetime.now()
                    
                    all_vacancies_data.append({
                        'vacancy_name': item.get('name', ''),
                        'company_name': company_name,
                        'salary_from': salary_from,
                        'salary_to': salary_to,
                        'currency': currency,
                        'published_at': published_at,
                        'url': vacancy_url,
                        'vacancy_type': 'Data Analyst' if is_data_analyst else 'Data Engineer',
                        'has_salary': has_salary
                    })
                    
                    page_vacancies_count += 1
                    
                except Exception as e:
                    print(f" Ошибка обработки вакансии {i+1} на странице {page + 1}: {e}")
                    continue
            
            print(f" Обработано вакансий на странице {page + 1}: {page_vacancies_count}")
            
            # Проверяем, есть ли следующая страница
            pages = data.get('pages', 0)
            if page >= pages - 1 or page >= 19:  # Ограничение API - максимум 20 страниц (2000 вакансий)
                print(" Достигнуто максимальное количество страниц")
                break
                
            page += 1
            
            # Пауза между запросами чтобы не нагружать API
            time.sleep(0.5)
        
        # Сохраняем в БД
        if all_vacancies_data:
            save_to_database(all_vacancies_data, conn_params)
            
            # Выводим детальную статистику
            print(f"\n ИТОГОВАЯ СТАТИСТИКА:")
            print(f"Всего найдено HH.ru: {total_vacancies_found} вакансий")
            print(f"Обработано релевантных: {len(all_vacancies_data)} вакансий")
            print(f"Data Analysts: {data_analyst_count} вакансий")
            print(f"Data Engineers: {data_engineer_count} вакансий")
            print(f"С зарплатой: {with_salary_count} вакансий")
            print(f"Без зарплаты: {without_salary_count} вакансий")
            
            # Процент вакансий с зарплатой
            if all_vacancies_data:
                salary_percentage = (with_salary_count / len(all_vacancies_data)) * 100
                print(f"Процент вакансий с зарплатой: {salary_percentage:.1f}%")
                
            # Охват данных
            if total_vacancies_found > 0:
                coverage_percentage = (len(all_vacancies_data) / total_vacancies_found) * 100
                print(f"Охват данных: {coverage_percentage:.1f}%")
        else:
            print("Не найдено релевантных вакансий")
            
    except Exception as e:
        print(f"Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()

def save_to_database(vacancies_data, conn_params):
    """Сохраняет данные в PostgreSQL с обработкой ошибок"""
    try:
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()
        
        insert_query = """
        INSERT INTO hh_vacancies (vacancy_name, company_name, salary_from, salary_to, currency, published_at, url, vacancy_type, has_salary)
        VALUES (%(vacancy_name)s, %(company_name)s, %(salary_from)s, %(salary_to)s, %(currency)s, %(published_at)s, %(url)s, %(vacancy_type)s, %(has_salary)s);
        """
        
        added_count = 0
        for vacancy in vacancies_data:
            try:
                cursor.execute(insert_query, vacancy)
                added_count += 1
            except Exception as e:
                print(f"Ошибка при вставке вакансии {vacancy['vacancy_name']}: {e}")
                continue
        
        conn.commit()
        
        # Получим общее количество записей в таблице
        cursor.execute("SELECT COUNT(*) FROM hh_vacancies;")
        total_in_db = cursor.fetchone()[0]
        
        # Получим первый ID
        cursor.execute("SELECT MIN(id) FROM hh_vacancies;")
        first_id = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        print(f"\nУСПЕХ! В БД добавлено: {added_count} вакансий")
        print(f"Всего в базе: {total_in_db} вакансий")
        print(f"ID начинаются с: {first_id}")
        
    except Exception as e:
        print(f"Критическая ошибка сохранения в БД: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    parse_hh_vacancies_api()