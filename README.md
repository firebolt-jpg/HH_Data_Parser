# HH_Data_Parser

ETL пайплайн для парсинга вакансий аналитиков данных с HH.ru с использованием Apache Airflow.

## Стек
- Apache Airflow 2.11.0
- PostgreSQL 18
- Python 3.12
- Docker & Docker Compose

## Быстрый запуск
```bash
git clone https://github.com/firebolt-jpg/HH_Data_Parser
cd hh_parser

# Запуск проекта
docker compose up airflow-init
docker compose up -d

# Веб-интерфейс: http://localhost:8080 (airflow/airflow)
# База данных: localhost:5433 (airflow/airflow)
