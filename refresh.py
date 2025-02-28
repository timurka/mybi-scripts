import psycopg2
import os
import yaml
import argparse
from config import DB_CONFIG
from datetime import datetime

def log_message(message):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] {message}")

def run_benchmarks(cursor):
    """Запускает бенчмарки для проверки корректности данных в материализованных представлениях"""
    log_message("Запуск бенчмарков для проверки данных...")
    
    # Путь к файлу с бенчмарками
    benchmark_file = '/Users/benchmarks.yaml'
    
    try:
        # Загрузка бенчмарков из YAML-файла
        with open(benchmark_file, 'r') as file:
            benchmarks = yaml.safe_load(file)
        
        if not benchmarks:
            log_message("Файл бенчмарков пуст или имеет неверный формат")
            return
        
        total_tests = 0
        passed_tests = 0
        failed_tests = 0
        
        # Выполнение каждого бенчмарка
        for benchmark in benchmarks:
            table_name = benchmark.get('table')
            tests = benchmark.get('tests', [])
            
            log_message(f"\n📊 Проверка таблицы: {table_name}")
            log_message("-" * 80)
            
            for test in tests:
                total_tests += 1
                query = test.get('query')
                expected = test.get('expected')
                column = test.get('column', None)
                description = test.get('description', 'Нет описания')
                
                log_message(f"Тест: {description}")
                log_message(f"Запрос: {query}")
                
                try:
                    cursor.execute(query)
                    result = cursor.fetchall()
                    
                    # Если указан конкретный столбец для проверки
                    if column is not None and result:
                        # Получаем индекс столбца
                        column_names = [desc[0] for desc in cursor.description]
                        if column in column_names:
                            column_index = column_names.index(column)
                            actual_value = result[0][column_index] if result else None
                            log_message(f"Проверяемый столбец: {column}")
                        else:
                            log_message(f"❌ Ошибка: Столбец '{column}' не найден в результате запроса")
                            failed_tests += 1
                            log_message("-" * 40)
                            continue
                    # Если нужно проверить количество строк
                    elif expected.get('count') is not None:
                        actual_value = len(result)
                        log_message(f"Проверка количества строк")
                    # Если нужно проверить первое значение первой строки
                    else:
                        actual_value = result[0][0] if result else None
                        log_message(f"Проверка первого значения")
                    
                    # Определяем ожидаемое значение
                    if 'value' in expected:
                        expected_value = expected['value']
                    elif 'count' in expected:
                        expected_value = expected['count']
                    else:
                        log_message(f"❌ Ошибка: Неверный формат ожидаемого результата в бенчмарке")
                        failed_tests += 1
                        log_message("-" * 40)
                        continue
                    
                    # Сравниваем результаты
                    log_message(f"Ожидаемое значение: {expected_value}")
                    log_message(f"Полученное значение: {actual_value}")
                    
                    if actual_value == expected_value:
                        log_message(f"✅ ТЕСТ ПРОЙДЕН")
                        passed_tests += 1
                    else:
                        log_message(f"❌ ТЕСТ НЕ ПРОЙДЕН - значения не совпадают")
                        failed_tests += 1
                
                except Exception as e:
                    log_message(f"❌ Ошибка при выполнении бенчмарка: {e}")
                    failed_tests += 1
                
                log_message("-" * 40)
        
        # Итоговый результат
        log_message("\n📋 ИТОГИ БЕНЧМАРКОВ")
        log_message("=" * 50)
        
        success_rate = (passed_tests / total_tests) * 100 if total_tests > 0 else 0
        log_message(f"Всего тестов: {total_tests}")
        log_message(f"✅ Успешно: {passed_tests}")
        log_message(f"❌ Неудачно: {failed_tests}")
        log_message(f"Процент успешных: {success_rate:.1f}%")
        
        # Вывод рекомендаций при наличии ошибок
        if failed_tests > 0:
            log_message("\n⚠️ РЕКОМЕНДАЦИИ:")
            log_message("1. Проверьте формат ожидаемых результатов в файле benchmarks.yaml")
            log_message("2. Убедитесь, что материализованные представления содержат ожидаемые данные")
            log_message("3. Проверьте правильность SQL-запросов в бенчмарках")
        
    except Exception as e:
        log_message(f"❌ Ошибка при запуске бенчмарков: {e}")

def create_indexes(cursor, conn, index_file_path, max_index_lines):
    """Создает индексы из файла одной транзакцией"""
    log_message("Начало создания индексов...")
    
    try:
        # Чтение индексов из файла
        log_message(f"Чтение индексов из файла: {index_file_path}")
        index_commands = []
        
        with open(index_file_path, 'r') as file:
            for i, line in enumerate(file):
                if i >= max_index_lines:
                    break
                # Пропускаем пустые строки и комментарии
                line = line.strip()
                if line and not line.startswith('--') and not line.startswith('/*'):
                    index_commands.append(line)
        
        log_message(f"Прочитано {len(index_commands)} команд создания индексов")
        
        # Объединяем все команды в одну транзакцию
        combined_command = "\n".join(index_commands)
        
        try:
            log_message("Выполнение всех команд создания индексов одной транзакцией...")
            cursor.execute(combined_command)
            conn.commit()
            log_message("✅ Все индексы успешно созданы")
        except psycopg2.Error as e:
            conn.rollback()
            log_message(f"❌ Ошибка при создании индексов: {e}")
            
            # Если произошла ошибка, пробуем выполнить команды по одной для выявления проблемной
            log_message("Попытка выполнения команд по одной для выявления проблемы...")
            for i, index_command in enumerate(index_commands):
                try:
                    cursor.execute(index_command)
                    conn.commit()
                except psycopg2.Error as e:
                    if "already exists" in str(e):
                        log_message(f"Индекс уже существует (команда {i+1}): {index_command[:60]}...")
                    else:
                        log_message(f"❌ Ошибка в команде {i+1}: {index_command[:60]}...")
                        log_message(f"   Ошибка: {e}")
                    
    except Exception as e:
        log_message(f"❌ Ошибка при чтении или выполнении файла индексов: {e}")

    log_message("Создание индексов завершено")

def drop_materialized_views(cursor, conn, drop_commands):
    """Удаляет материализованные представления одной транзакцией"""
    log_message("Начало удаления существующих материализованных представлений...")
    
    try:
        # Объединяем все команды DROP в одну транзакцию
        combined_drop_command = "\n".join(drop_commands)
        
        try:
            log_message("Выполнение всех команд удаления представлений одной транзакцией...")
            cursor.execute(combined_drop_command)
            conn.commit()
            log_message("✅ Все материализованные представления успешно удалены")
        except psycopg2.Error as e:
            conn.rollback()
            log_message(f"❌ Ошибка при удалении представлений: {e}")
            
            # Если произошла ошибка, пробуем выполнить команды по одной для выявления проблемной
            log_message("Попытка выполнения команд по одной для выявления проблемы...")
            for i, drop_command in enumerate(drop_commands):
                try:
                    log_message(f"Выполнение: {drop_command}")
                    cursor.execute(drop_command)
                    conn.commit()
                    log_message("Команда успешно выполнена")
                except psycopg2.Error as e:
                    log_message(f"Ошибка при удалении представления (команда {i+1}): {e}")
    
    except Exception as e:
        log_message(f"❌ Ошибка при удалении материализованных представлений: {e}")
    
    log_message("Удаление материализованных представлений завершено")

def refresh_materialized_view(only_benchmarks=False):
    """
    Обновляет материализованные представления и запускает бенчмарки
    
    Args:
        only_benchmarks (bool): Если True, запускает только бенчмарки без обновления представлений
    """
    # Параметры подключения к базе данных
    db_params = {
        'dbname': os.getenv('DB_NAME', 'dbname'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'), 
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': os.getenv('DB_PORT', '5432')
    }
    
    # Команды для удаления материализованных представлений в правильном порядке
    drop_commands = [
        "drop materialized view if exists table1;",
        "drop materialized view if exists table2;",
    ]

    # Список SQL файлов для выполнения в нужном порядке
    base_path = '/Users/'
    sql_files = [
        f'{base_path}table1.sql',
        f'{base_path}table2.sql',
    ]
    
    # Путь к файлу с индексами и количество строк для чтения
    index_file_path = f'{base_path}index.sql'
    max_index_lines = 41  # Количество строк для чтения из файла индексов

    conn = None
    try:
        # Подключение к базе данных
        log_message(f"Попытка подключения к PostgreSQL на {db_params['host']}:{db_params['port']}")
        log_message(f"База данных: {db_params['dbname']}, пользователь: {db_params['user']}")
        
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        log_message("Успешное подключение к PostgreSQL")

        if not only_benchmarks:
            # Удаление существующих материализованных представлений одной транзакцией
            drop_materialized_views(cursor, conn, drop_commands)

            # Выполнение каждого SQL файла по очереди
            for sql_file_path in sql_files:
                log_message(f"Обработка файла: {sql_file_path}")
                
                # Чтение SQL файла
                with open(sql_file_path, 'r') as file:
                    sql_command = file.read()
                
                log_message(f"SQL файл успешно прочитан, размер: {len(sql_command)} байт")

                # Выполнение SQL команды
                log_message("Начало выполнения SQL команды...")
                cursor.execute(sql_command)
                conn.commit()
                log_message(f"SQL команда из файла {sql_file_path} успешно выполнена и зафиксирована")

            # Создание индексов
            create_indexes(cursor, conn, index_file_path, max_index_lines)
        else:
            log_message("Режим только бенчмарков: пропуск обновления материализованных представлений")
        
        # Запуск бенчмарков для проверки данных
        run_benchmarks(cursor)

    except (Exception, psycopg2.Error) as error:
        log_message(f"❌ Ошибка при работе с PostgreSQL: {error}")

    finally:
        # Закрытие соединения
        if conn:
            cursor.close()
            conn.close()
            log_message("Соединение с PostgreSQL закрыто")

if __name__ == "__main__":
    # Настройка аргументов командной строки
    parser = argparse.ArgumentParser(description='Обновление материализованных представлений и запуск бенчмарков')
    parser.add_argument('--only-benchmarks', action='store_true', 
                        help='Запустить только бенчмарки без обновления представлений')
    args = parser.parse_args()
    
    if args.only_benchmarks:
        log_message("Запуск только бенчмарков")
        refresh_materialized_view(only_benchmarks=True)
    else:
        log_message("Запуск полного обновления материализованных представлений")
        refresh_materialized_view()
    
    log_message("Завершение работы скрипта") 
