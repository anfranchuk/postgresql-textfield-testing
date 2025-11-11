"""
Скрипт для тестирования операций INSERT, SELECT, UPDATE, DELETE
и сбора статистики производительности.
"""

import psycopg2
import random
import string
import time
import statistics
from psycopg2.extras import execute_batch
import os
from dotenv import load_dotenv
import logging
from datetime import datetime
import sys

load_dotenv()

# Параметры подключения
DB_CONFIG = {
    'host': 'localhost',
    'port': os.getenv('POSTGRES_PORT', '5432'),
    'database': os.getenv('POSTGRES_DB', 'testdb'),
    'user': os.getenv('POSTGRES_USER', 'postgres'),
    'password': os.getenv('POSTGRES_PASSWORD', 'postgres')
}

# Количество операций для тестирования
OPERATIONS_COUNT = 1000

# Размер батча для операций
BATCH_SIZE = 100

# Глобальная переменная для logger
logger = None


def log_print(message, level='info'):
    """Выводит сообщение в консоль и в лог-файл."""
    print(message)
    if logger:
        if level == 'info':
            logger.info(message)
        elif level == 'error':
            logger.error(message)
        elif level == 'warning':
            logger.warning(message)


def generate_random_string(min_length, max_length):
    """Генерирует случайную строку заданной длины."""
    length = random.randint(min_length, max_length)
    return ''.join(random.choices(string.ascii_letters + string.digits + string.punctuation + ' ', k=length))


def get_table_config(table_num):
    """Возвращает конфигурацию таблицы."""
    configs = {
        1: {'min_len': 1, 'max_len': 100, 'type': 'varchar', 'indexed': True},
        2: {'min_len': 1, 'max_len': 100, 'type': 'varchar', 'indexed': False},
        3: {'min_len': 500, 'max_len': 1000, 'type': 'varchar(1000)', 'indexed': False},
        4: {'min_len': 9000, 'max_len': 10000, 'type': 'varchar(10000)', 'indexed': False},
        5: {'min_len': 500, 'max_len': 1000, 'type': 'varchar(1000)', 'indexed': True},
        6: {'min_len': 9000, 'max_len': 10000, 'type': 'text', 'indexed': False},
        7: {'min_len': 500, 'max_len': 1000, 'type': 'text', 'indexed': False},
    }
    return configs[table_num]


def get_max_id(conn, table_name):
    """Получает максимальный ID из таблицы."""
    cur = conn.cursor()
    cur.execute(f"SELECT MAX(id) FROM {table_name}")
    result = cur.fetchone()[0]
    cur.close()
    return result if result else 0


def format_size(bytes_size):
    """Форматирует размер в байтах в читаемый формат."""
    for unit in ['Б', 'КБ', 'МБ', 'ГБ', 'ТБ']:
        if bytes_size < 1024.0:
            return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.2f} ПБ"


def get_table_stats(conn, table_name):
    """Получает статистику таблицы: размер таблицы, индексов и средний размер записи."""
    cur = conn.cursor()
    
    # Получаем размеры таблицы
    cur.execute(f"""
        SELECT 
            pg_total_relation_size('{table_name}') as total_size,
            pg_relation_size('{table_name}') as table_size,
            pg_indexes_size('{table_name}') as indexes_size,
            (SELECT COUNT(*) FROM {table_name}) as record_count
    """)
    
    row = cur.fetchone()
    total_size = row[0] if row[0] else 0
    table_size = row[1] if row[1] else 0
    indexes_size = row[2] if row[2] else 0
    record_count = row[3] if row[3] else 0
    
    # Вычисляем средний размер записи
    avg_record_size = table_size / record_count if record_count > 0 else 0
    
    cur.close()
    
    return {
        'total_size': total_size,
        'table_size': table_size,
        'indexes_size': indexes_size,
        'record_count': record_count,
        'avg_record_size': avg_record_size
    }


def test_insert(conn, table_name, min_length, max_length, count=OPERATIONS_COUNT):
    """Тестирует операцию INSERT."""
    cur = conn.cursor()
    times = []
    
    for _ in range(count):
        value = generate_random_string(min_length, max_length)
        start = time.perf_counter()
        cur.execute(f"INSERT INTO {table_name} (value) VALUES (%s)", (value,))
        conn.commit()
        elapsed = time.perf_counter() - start
        times.append(elapsed)
    
    cur.close()
    
    base_stats = {
        'total_time': sum(times),
        'avg_time': statistics.mean(times),
        'min_time': min(times),
        'max_time': max(times),
        'median_time': statistics.median(times),
        'ops_per_sec': count / sum(times) if sum(times) > 0 else 0
    }
    
    # Добавляем расширенную статистику
    base_stats.update(calculate_extended_stats(times))
    
    return base_stats


def test_select_by_id(conn, table_name, max_id, count=OPERATIONS_COUNT):
    """Тестирует операцию SELECT по ID (PK)."""
    cur = conn.cursor()
    times = []
    
    for _ in range(count):
        random_id = random.randint(1, max_id)
        start = time.perf_counter()
        cur.execute(f"SELECT * FROM {table_name} WHERE id = %s", (random_id,))
        cur.fetchone()
        elapsed = time.perf_counter() - start
        times.append(elapsed)
    
    cur.close()
    
    base_stats = {
        'total_time': sum(times),
        'avg_time': statistics.mean(times),
        'min_time': min(times),
        'max_time': max(times),
        'median_time': statistics.median(times),
        'ops_per_sec': count / sum(times) if sum(times) > 0 else 0
    }
    
    # Добавляем расширенную статистику
    base_stats.update(calculate_extended_stats(times))
    
    return base_stats


def calculate_extended_stats(times):
    """Вычисляет расширенную статистику из списка времен выполнения."""
    if not times or len(times) == 0:
        return {}
    
    sorted_times = sorted(times)
    n = len(sorted_times)
    
    # Перцентили
    p95_idx = int(n * 0.95)
    p99_idx = int(n * 0.99)
    p95 = sorted_times[p95_idx] if p95_idx < n else sorted_times[-1]
    p99 = sorted_times[p99_idx] if p99_idx < n else sorted_times[-1]
    
    # Стандартное отклонение
    stddev = statistics.stdev(times) if len(times) > 1 else 0
    
    # Коэффициент вариации (CV) - показывает стабильность
    mean_time = statistics.mean(times)
    cv = (stddev / mean_time * 100) if mean_time > 0 else 0
    
    return {
        'p95_time': p95,
        'p99_time': p99,
        'stddev': stddev,
        'cv_percent': cv
    }


def test_update_by_id(conn, table_name, min_length, max_length, max_id, count=OPERATIONS_COUNT):
    """Тестирует операцию UPDATE по ID."""
    cur = conn.cursor()
    times = []
    
    for _ in range(count):
        random_id = random.randint(1, max_id)
        new_value = generate_random_string(min_length, max_length)
        start = time.perf_counter()
        cur.execute(f"UPDATE {table_name} SET value = %s WHERE id = %s", (new_value, random_id))
        conn.commit()
        elapsed = time.perf_counter() - start
        times.append(elapsed)
    
    cur.close()
    
    base_stats = {
        'total_time': sum(times),
        'avg_time': statistics.mean(times),
        'min_time': min(times),
        'max_time': max(times),
        'median_time': statistics.median(times),
        'ops_per_sec': count / sum(times) if sum(times) > 0 else 0
    }
    
    # Добавляем расширенную статистику
    base_stats.update(calculate_extended_stats(times))
    
    return base_stats


def test_delete_by_id(conn, table_name, max_id, count=OPERATIONS_COUNT):
    """Тестирует операцию DELETE по ID."""
    cur = conn.cursor()
    times = []
    
    # Получаем список ID для удаления
    cur.execute(f"SELECT id FROM {table_name} ORDER BY RANDOM() LIMIT %s", (count,))
    ids_to_delete = [row[0] for row in cur.fetchall()]
    
    for id_to_delete in ids_to_delete:
        start = time.perf_counter()
        cur.execute(f"DELETE FROM {table_name} WHERE id = %s", (id_to_delete,))
        conn.commit()
        elapsed = time.perf_counter() - start
        times.append(elapsed)
    
    cur.close()
    
    base_stats = {
        'total_time': sum(times),
        'avg_time': statistics.mean(times),
        'min_time': min(times),
        'max_time': max(times),
        'median_time': statistics.median(times),
        'ops_per_sec': count / sum(times) if sum(times) > 0 else 0
    }
    
    # Добавляем расширенную статистику
    base_stats.update(calculate_extended_stats(times))
    
    return base_stats


def test_table(conn, table_num):
    """Тестирует все операции для одной таблицы."""
    table_name = f'table{table_num}'
    config = get_table_config(table_num)
    
    log_print(f"\n{'='*80}")
    log_print(f"Тестирование таблицы {table_name}")
    log_print(f"  Тип: {config['type']}, Индекс: {'Да' if config['indexed'] else 'Нет'}")
    log_print(f"  Длина строк: {config['min_len']}-{config['max_len']} символов")
    log_print(f"{'='*80}")
    
    max_id = get_max_id(conn, table_name)
    
    # Получаем статистику таблицы
    stats = get_table_stats(conn, table_name)
    log_print(f"Текущее количество записей: {stats['record_count']:,}")
    log_print(f"  Размер таблицы: {format_size(stats['table_size'])}")
    if stats['indexes_size'] > 0:
        log_print(f"  Размер индексов: {format_size(stats['indexes_size'])}")
    log_print(f"  Общий размер: {format_size(stats['total_size'])}")
    if stats['record_count'] > 0:
        log_print(f"  Средний размер записи: {format_size(stats['avg_record_size'])}")
    
    if max_id == 0:
        log_print(f"⚠ Таблица {table_name} пуста. Пропускаем тестирование.", 'warning')
        return None
    
    results = {
        'table': table_name,
        'config': config,
        'record_count': max_id,
        'stats': stats
    }
    
    # Тест INSERT
    log_print("\n[INSERT] Тестирование вставки...")
    results['insert'] = test_insert(conn, table_name, config['min_len'], config['max_len'])
    log_print(f"  Среднее время: {results['insert']['avg_time']*1000:.3f} мс")
    log_print(f"  P95: {results['insert']['p95_time']*1000:.3f} мс, P99: {results['insert']['p99_time']*1000:.3f} мс")
    log_print(f"  Стандартное отклонение: {results['insert']['stddev']*1000:.3f} мс")
    log_print(f"  Коэффициент вариации: {results['insert']['cv_percent']:.2f}%")
    log_print(f"  Операций/сек: {results['insert']['ops_per_sec']:.2f}")
    
    # Обновляем max_id после INSERT
    max_id = get_max_id(conn, table_name)
    
    # Тест SELECT по ID
    log_print("\n[SELECT BY ID] Тестирование выборки по ID...")
    results['select_by_id'] = test_select_by_id(conn, table_name, max_id)
    log_print(f"  Среднее время: {results['select_by_id']['avg_time']*1000:.3f} мс")
    log_print(f"  P95: {results['select_by_id']['p95_time']*1000:.3f} мс, P99: {results['select_by_id']['p99_time']*1000:.3f} мс")
    log_print(f"  Стандартное отклонение: {results['select_by_id']['stddev']*1000:.3f} мс")
    log_print(f"  Коэффициент вариации: {results['select_by_id']['cv_percent']:.2f}%")
    log_print(f"  Операций/сек: {results['select_by_id']['ops_per_sec']:.2f}")
    
    # Тест UPDATE по ID
    log_print("\n[UPDATE BY ID] Тестирование обновления по ID...")
    results['update_by_id'] = test_update_by_id(
        conn, table_name, config['min_len'], config['max_len'], max_id
    )
    log_print(f"  Среднее время: {results['update_by_id']['avg_time']*1000:.3f} мс")
    log_print(f"  P95: {results['update_by_id']['p95_time']*1000:.3f} мс, P99: {results['update_by_id']['p99_time']*1000:.3f} мс")
    log_print(f"  Стандартное отклонение: {results['update_by_id']['stddev']*1000:.3f} мс")
    log_print(f"  Коэффициент вариации: {results['update_by_id']['cv_percent']:.2f}%")
    log_print(f"  Операций/сек: {results['update_by_id']['ops_per_sec']:.2f}")
    
    # Тест DELETE по ID
    log_print("\n[DELETE BY ID] Тестирование удаления по ID...")
    results['delete_by_id'] = test_delete_by_id(conn, table_name, max_id)
    log_print(f"  Среднее время: {results['delete_by_id']['avg_time']*1000:.3f} мс")
    log_print(f"  P95: {results['delete_by_id']['p95_time']*1000:.3f} мс, P99: {results['delete_by_id']['p99_time']*1000:.3f} мс")
    log_print(f"  Стандартное отклонение: {results['delete_by_id']['stddev']*1000:.3f} мс")
    log_print(f"  Коэффициент вариации: {results['delete_by_id']['cv_percent']:.2f}%")
    log_print(f"  Операций/сек: {results['delete_by_id']['ops_per_sec']:.2f}")
    
    return results


def print_summary(all_results):
    """Выводит итоговую сводку результатов."""
    log_print("\n" + "="*80)
    log_print("ИТОГОВАЯ СВОДКА РЕЗУЛЬТАТОВ")
    log_print("="*80)
    
    # Статистика размеров таблиц
    log_print("\n" + "-"*80)
    log_print("СТАТИСТИКА РАЗМЕРОВ ТАБЛИЦ")
    log_print("-"*80)
    log_print(f"{'Таблица':<12} {'Записей':<15} {'Размер таблицы':<20} {'Размер индексов':<20} {'Общий размер':<20} {'Ср. размер записи':<20}")
    log_print("-"*80)
    
    for result in all_results:
        if result is None or 'stats' not in result:
            continue
        
        stats = result['stats']
        table = result['table']
        log_print(f"{table:<12} {stats['record_count']:>13,}  "
              f"{format_size(stats['table_size']):<20} "
              f"{format_size(stats['indexes_size']):<20} "
              f"{format_size(stats['total_size']):<20} "
              f"{format_size(stats['avg_record_size']):<20}")
    
    log_print("-"*80)
    
    # Создаем таблицу для сравнения операций
    log_print("\n" + "-"*80)
    log_print("ПРОИЗВОДИТЕЛЬНОСТЬ ОПЕРАЦИЙ")
    log_print("-"*80)
    log_print(f"{'Таблица':<12} {'Операция':<20} {'Среднее (мс)':<15} {'Оп/сек':<15}")
    log_print("-"*80)
    
    for result in all_results:
        if result is None:
            continue
        
        table = result['table']
        config = result['config']
        indexed = "idx" if config['indexed'] else "no-idx"
        
        for op_name, op_result in [
            ('INSERT', result.get('insert')),
            ('SELECT BY ID', result.get('select_by_id')),
            ('UPDATE BY ID', result.get('update_by_id')),
            ('DELETE BY ID', result.get('delete_by_id')),
        ]:
            if op_result:
                log_print(f"{table:<12} {op_name:<20} {op_result['avg_time']*1000:>12.3f}  {op_result['ops_per_sec']:>12.2f}")
    
    log_print("-"*80)
    
    # Детальная статистика
    log_print("\n" + "="*80)
    log_print("ДЕТАЛЬНАЯ СТАТИСТИКА")
    log_print("="*80)
    
    for result in all_results:
        if result is None:
            continue
        
        log_print(f"\n{result['table']} ({result['config']['type']}, "
              f"{'с индексом' if result['config']['indexed'] else 'без индекса'}):")
        
        # Выводим статистику размеров
        if 'stats' in result:
            stats = result['stats']
            log_print(f"  Статистика размеров:")
            log_print(f"    Количество записей: {stats['record_count']:,}")
            log_print(f"    Размер таблицы: {format_size(stats['table_size'])}")
            if stats['indexes_size'] > 0:
                log_print(f"    Размер индексов: {format_size(stats['indexes_size'])}")
            log_print(f"    Общий размер: {format_size(stats['total_size'])}")
            log_print(f"    Средний размер записи: {format_size(stats['avg_record_size'])}")
        
        log_print(f"  Производительность операций:")
        for op_name, op_result in [
            ('INSERT', result.get('insert')),
            ('SELECT BY ID', result.get('select_by_id')),
            ('UPDATE BY ID', result.get('update_by_id')),
            ('DELETE BY ID', result.get('delete_by_id')),
        ]:
            if op_result:
                log_print(f"  {op_name}:")
                log_print(f"    Среднее: {op_result['avg_time']*1000:.3f} мс")
                log_print(f"    Медиана: {op_result['median_time']*1000:.3f} мс")
                log_print(f"    P95: {op_result['p95_time']*1000:.3f} мс")
                log_print(f"    P99: {op_result['p99_time']*1000:.3f} мс")
                log_print(f"    Мин: {op_result['min_time']*1000:.3f} мс")
                log_print(f"    Макс: {op_result['max_time']*1000:.3f} мс")
                log_print(f"    Стандартное отклонение: {op_result['stddev']*1000:.3f} мс")
                log_print(f"    Коэффициент вариации: {op_result['cv_percent']:.2f}%")
                log_print(f"    Операций/сек: {op_result['ops_per_sec']:.2f}")


def main():
    """Основная функция."""
    global logger
    
    # Настройка логирования в файл
    log_filename = f"test_operations_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
        ]
    )
    logger = logging.getLogger(__name__)
    
    log_print("="*80)
    log_print("ТЕСТИРОВАНИЕ ПРОИЗВОДИТЕЛЬНОСТИ ОПЕРАЦИЙ PostgreSQL")
    log_print("="*80)
    log_print(f"Количество операций для каждого теста: {OPERATIONS_COUNT:,}")
    log_print(f"Логи записываются в файл: {log_filename}\n")
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        log_print("Подключение к базе данных установлено.")
        
        all_results = []
        
        # Тестируем все таблицы (1-7)
        table_numbers = [1, 2, 3, 4, 5, 6, 7]
        for table_num in table_numbers:
            result = test_table(conn, table_num)
            all_results.append(result)
        
        conn.close()
        
        # Выводим итоговую сводку
        print_summary(all_results)
        
        log_print("\n" + "="*80)
        log_print("Тестирование завершено!")
        log_print("="*80)
        log_print(f"Логи сохранены в файл: {log_filename}")
        
    except psycopg2.Error as e:
        log_print(f"Ошибка базы данных: {e}", 'error')
    except Exception as e:
        log_print(f"Ошибка: {e}", 'error')
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()

