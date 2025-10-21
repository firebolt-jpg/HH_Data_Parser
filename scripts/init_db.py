# scripts/init_db.py
import psycopg2
from psycopg2 import sql
import time

# Данные для подключения (из docker-compose.yml)
conn_params = {
    "host": "postgres",  # Имя из docker-compose.yml
    "port": 5432,
    "database": "airflow",
    "user": "airflow",
    "password": "airflow"
}

create_table_query = """
CREATE TABLE IF NOT EXISTS hh_vacancies (
    id SERIAL PRIMARY KEY,
    vacancy_name VARCHAR(500),
    company_name VARCHAR(255),
    salary_from INTEGER,
    salary_to INTEGER,
    currency VARCHAR(10),
    published_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    url VARCHAR(500),
    CONSTRAINT unique_vacancy_url UNIQUE (url)  -- Чтобы избежать дубликатов
);
"""

def create_table_with_retry(max_retries=3, delay=5):
    for attempt in range(max_retries):
        try:
            # Подключаемся к БД
            conn = psycopg2.connect(**conn_params)
            cursor = conn.cursor()
            
            # Выполняем запрос на создание таблицы
            cursor.execute(create_table_query)
            conn.commit()
            print("Таблица 'hh_vacancies' успешно создана или уже существует.")
            return True
            
        except Exception as e:
            print(f"Попытка {attempt + 1} из {max_retries} не удалась: {e}")
            if attempt < max_retries - 1:
                print(f"Повторная попытка через {delay} секунд...")
                time.sleep(delay)
        finally:
            if 'conn' in locals():
                cursor.close()
                conn.close()
                print("Соединение с PostgreSQL закрыто.")
    
    return False

if __name__ == "__main__":
    success = create_table_with_retry()
    if not success:
        print("Не удалось создать таблицу после нескольких попыток")