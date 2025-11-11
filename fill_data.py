"""
Скрипт для создания таблиц и заполнения их тестовыми данными.
Заполняет каждую таблицу 10 миллионами записей с рандомными текстовыми значениями.
"""

import psycopg2
import random
import string
import time
from psycopg2.extras import execute_batch
import os
from dotenv import load_dotenv
import logging
from datetime import datetime
import argparse

load_dotenv()

# Параметры подключения
DB_CONFIG = {
    'host': 'localhost',
    'port': os.getenv('POSTGRES_PORT', '5432'),
    'database': os.getenv('POSTGRES_DB', 'testdb'),
    'user': os.getenv('POSTGRES_USER', 'postgres'),
    'password': os.getenv('POSTGRES_PASSWORD', 'postgres')
}

# Количество записей для каждой таблицы
RECORDS_COUNT = 2_000_000

# Размер батча для вставки
BATCH_SIZE = 10000


def generate_random_string(min_length, max_length):
    """Генерирует случайную строку заданной длины."""
    length = random.randint(min_length, max_length)
    return ''.join(random.choices(string.ascii_letters + string.digits + string.punctuation + ' ', k=length))


def create_tables(conn, logger):
    """Создает все тестовые таблицы."""
    cur = conn.cursor()
    
    logger.info("Создание таблиц...")
    
    # Таблица 1: varchar, индекс на value, значения 1-100 символов
    cur.execute("""
        CREATE TABLE IF NOT EXISTS table1 (
            id SERIAL PRIMARY KEY,
            value VARCHAR
        );
        CREATE INDEX IF NOT EXISTS idx_table1_value ON table1(value);
    """)
    
    # Таблица 2: varchar, без индекса, значения 1-100 символов
    cur.execute("""
        CREATE TABLE IF NOT EXISTS table2 (
            id SERIAL PRIMARY KEY,
            value VARCHAR
        );
    """)
    
    # Таблица 3: varchar(1000), без индекса, значения 500-1000 символов
    cur.execute("""
        CREATE TABLE IF NOT EXISTS table3 (
            id SERIAL PRIMARY KEY,
            value VARCHAR(1000)
        );
    """)
    
    # Таблица 4: varchar(10000), без индекса, значения 9000-10000 символов
    cur.execute("""
        CREATE TABLE IF NOT EXISTS table4 (
            id SERIAL PRIMARY KEY,
            value VARCHAR(10000)
        );
    """)
    
    # Таблица 5: varchar(1000) с индексом, значения 500-1000 символов
    cur.execute("""
        CREATE TABLE IF NOT EXISTS table5 (
            id SERIAL PRIMARY KEY,
            value VARCHAR(1000)
        );
        CREATE INDEX IF NOT EXISTS idx_table5_value ON table5(value);
    """)
    
    # Таблица 6: text без индекса, значения 9000-10000 символов
    cur.execute("""
        CREATE TABLE IF NOT EXISTS table6 (
            id SERIAL PRIMARY KEY,
            value TEXT
        );
    """)
    
    # Таблица 7: text без индекса, значения 500-1000 символов
    cur.execute("""
        CREATE TABLE IF NOT EXISTS table7 (
            id SERIAL PRIMARY KEY,
            value TEXT
        );
    """)
    
    conn.commit()
    cur.close()
    logger.info("Таблицы созданы успешно.\n")


def get_table_count(conn, table_name):
    """Получает количество записей в таблице."""
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cur.fetchone()[0]
    cur.close()
    return count


def fill_table(conn, table_name, min_length, max_length, batch_size=BATCH_SIZE, logger=None, skip_if_exists=False):
    """Заполняет таблицу данными."""
    if logger is None:
        logger = logging.getLogger(__name__)
    
    cur = conn.cursor()
    
    # Проверяем, нужно ли заполнять таблицу
    if skip_if_exists:
        current_count = get_table_count(conn, table_name)
        if current_count >= RECORDS_COUNT:
            logger.info(f"Таблица {table_name} уже заполнена ({current_count:,} записей). Пропускаем.\n")
            cur.close()
            return 0
    
    logger.info(f"Заполнение таблицы {table_name}...")
    logger.info(f"  Диапазон длины строк: {min_length}-{max_length} символов")
    logger.info(f"  Количество записей: {RECORDS_COUNT:,}")
    logger.info(f"  Размер батча: {batch_size:,}")
    
    start_time = time.time()
    total_inserted = 0
    
    # Проверяем текущее количество записей
    current_count = get_table_count(conn, table_name)
    if current_count > 0:
        logger.info(f"  В таблице уже есть {current_count:,} записей. Продолжаем заполнение.")
        total_inserted = current_count
    
    # Очищаем таблицу только если она пуста или нужно перезаписать
    if current_count == 0:
        cur.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY")
        conn.commit()
    
    while total_inserted < RECORDS_COUNT:
        batch = []
        batch_count = min(batch_size, RECORDS_COUNT - total_inserted)
        
        for _ in range(batch_count):
            value = generate_random_string(min_length, max_length)
            batch.append((value,))
        
        execute_batch(
            cur,
            f"INSERT INTO {table_name} (value) VALUES (%s)",
            batch,
            page_size=batch_size
        )
        
        conn.commit()
        total_inserted += batch_count
        
        if total_inserted % 100000 == 0:
            elapsed = time.time() - start_time
            rate = total_inserted / elapsed if elapsed > 0 else 0
            logger.info(f"  Вставлено: {total_inserted:,} / {RECORDS_COUNT:,} "
                       f"({total_inserted * 100 / RECORDS_COUNT:.1f}%) "
                       f"| Скорость: {rate:.0f} записей/сек")
    
    elapsed = time.time() - start_time
    rate = total_inserted / elapsed if elapsed > 0 else 0
    
    logger.info(f"✓ Таблица {table_name} заполнена за {elapsed:.2f} секунд "
               f"({rate:.0f} записей/сек)\n")
    
    cur.close()
    return elapsed


def main():
    """Основная функция."""
    # Парсинг аргументов командной строки
    parser = argparse.ArgumentParser(description='Заполнение таблиц тестовыми данными')
    parser.add_argument('--start-from', type=int, default=1, 
                       help='Номер таблицы, с которой начать заполнение (1-7). По умолчанию: 1')
    parser.add_argument('--skip-existing', action='store_true',
                       help='Пропускать таблицы, которые уже заполнены')
    args = parser.parse_args()
    
    start_from = args.start_from
    if start_from < 1 or start_from > 7:
        print(f"Ошибка: номер таблицы должен быть от 1 до 7, получено: {start_from}")
        return
    
    # Настройка логирования в файл
    log_filename = f"fill_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
        ]
    )
    logger = logging.getLogger(__name__)
    
    logger.info("=" * 60)
    logger.info("Заполнение таблиц тестовыми данными")
    logger.info("=" * 60)
    logger.info(f"Количество записей на таблицу: {RECORDS_COUNT:,}")
    logger.info(f"Начало заполнения с таблицы: table{start_from}")
    logger.info(f"Пропуск существующих таблиц: {args.skip_existing}")
    logger.info(f"Логи записываются в файл: {log_filename}\n")
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        logger.info("Подключение к базе данных установлено.\n")
        
        # Создаем таблицы
        create_tables(conn, logger)
        
        # Заполняем таблицы
        results = {}
        
        # Список таблиц с их параметрами
        tables_config = [
            (1, 'table1', 1, 100),
            (2, 'table2', 1, 100),
            (3, 'table3', 500, 1000),
            (4, 'table4', 9000, 10000),
            (5, 'table5', 500, 1000),
            (6, 'table6', 9000, 10000),
            (7, 'table7', 500, 1000),
        ]
        
        for table_num, table_name, min_len, max_len in tables_config:
            if table_num < start_from:
                logger.info(f"Пропускаем таблицу {table_name} (до начальной таблицы table{start_from})")
                continue
            
            results[table_name] = fill_table(
                conn, table_name, min_len, max_len, 
                logger=logger, 
                skip_if_exists=args.skip_existing
            )
        
        conn.close()
        
        # Итоговая статистика
        logger.info("=" * 60)
        logger.info("Итоговая статистика заполнения:")
        logger.info("=" * 60)
        total_time = sum(results.values())
        for table, elapsed in results.items():
            logger.info(f"{table}: {elapsed:.2f} сек")
        logger.info(f"\nОбщее время: {total_time:.2f} сек ({total_time/60:.2f} минут)")
        logger.info("=" * 60)
        logger.info(f"Логи сохранены в файл: {log_filename}")
        
    except psycopg2.Error as e:
        logger.error(f"Ошибка базы данных: {e}")
    except Exception as e:
        logger.error(f"Ошибка: {e}")


if __name__ == "__main__":
    main()

