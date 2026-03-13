from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
import random

CONN_ID = 'my_dwh_connection'

default_args = {
    'owner': 'release_manager',
    'start_date': datetime(2023, 1, 1),
    'retries': 0
}

# Функция для генерации огромного SQL скрипта (Python)
def generate_big_data_sql():
    # Генерируем 5000 юзеров и 50 000 заказов
    users_sql = []
    for i in range(1, 5001):
        segment = random.choice(['vip', 'standard', 'new', 'churned'])
        users_sql.append(f"('user_{i}', '{segment}')")
    
    orders_sql = []
    for i in range(1, 50001):
        u_id = random.randint(1, 5000)
        amount = round(random.uniform(10.0, 50000.0), 2)
        # Имитируем разные даты для партиционирования
        date_offset = random.randint(0, 30) 
        orders_sql.append(f"({u_id}, {amount}, CURRENT_DATE - INTERVAL '{date_offset} days')")
    
    # Склеиваем в один большой INSERT (batch insert)
    return f"""
        INSERT INTO users (username, segment) VALUES {','.join(users_sql)};
        INSERT INTO orders (user_id, amount, order_date) VALUES {','.join(orders_sql)};
    """

with DAG(
    dag_id='03_big_data_load',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['mediascope', 'load_test']
) as dag:

    # 1. Подготовка структуры (DDL)
    init_schema = PostgresOperator(
        task_id='init_schema',
        postgres_conn_id=CONN_ID,
        sql="""
            DROP VIEW IF EXISTS dm_daily_revenue;
            DROP TABLE IF EXISTS orders;
            DROP TABLE IF EXISTS users;
            DROP TABLE IF EXISTS dq_log;

            CREATE TABLE users (
                user_id SERIAL PRIMARY KEY,
                username VARCHAR(50),
                segment VARCHAR(20)
            );
            
            CREATE TABLE orders (
                order_id SERIAL PRIMARY KEY,
                user_id INT,
                amount DECIMAL(10, 2),
                order_date DATE
            );

            -- Таблица для логов качества данных (Data Quality)
            CREATE TABLE dq_log (
                check_name VARCHAR(100),
                check_status VARCHAR(20), -- 'PASS', 'FAIL'
                check_timestamp TIMESTAMP DEFAULT NOW()
            );
        """
    )

    # 2. Генерация и заливка данных (Используем Python для генерации SQL)
    # В реальной жизни тут был бы COPY command, но для учебного стенда так нагляднее
    from airflow.operators.python import PythonOperator
    
    def load_data_func(**kwargs):
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        sql = generate_big_data_sql()
        pg_hook = PostgresHook(postgres_conn_id=CONN_ID)
        pg_hook.run(sql)

    load_heavy_data = PythonOperator(
        task_id='load_50k_rows',
        python_callable=load_data_func
    )

    # 3. Data Quality Check (Проверка качества)
    # Проверяем, нет ли заказов с отрицательной суммой или сирот (без юзера)
    dq_check = PostgresOperator(
        task_id='run_data_quality_checks',
        postgres_conn_id=CONN_ID,
        sql="""
            -- Проверка 1: Отрицательные суммы
            INSERT INTO dq_log (check_name, check_status)
            SELECT 'negative_amount_check', 
                CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
            FROM orders WHERE amount < 0;

            -- Проверка 2: Орфаны (заказы несуществующих юзеров)
            INSERT INTO dq_log (check_name, check_status)
            SELECT 'orphan_users_check',
                CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
            FROM orders o 
            LEFT JOIN users u ON o.user_id = u.user_id
            WHERE u.user_id IS NULL;
        """
    )

    # 4. Построение витрины (Тяжелый запрос с агрегацией)
    build_mart = PostgresOperator(
        task_id='build_revenue_mart',
        postgres_conn_id=CONN_ID,
        sql="""
            CREATE OR REPLACE VIEW dm_daily_revenue AS
            SELECT 
                u.segment,
                o.order_date,
                COUNT(o.order_id) as orders_count,
                SUM(o.amount) as total_revenue,
                AVG(o.amount) as avg_check
            FROM orders o
            JOIN users u ON o.user_id = u.user_id
            GROUP BY u.segment, o.order_date
            ORDER BY o.order_date DESC, total_revenue DESC;
        """
    )

    init_schema >> load_heavy_data >> dq_check >> build_mart