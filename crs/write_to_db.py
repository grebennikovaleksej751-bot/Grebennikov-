import pandas as pd
import sqlite3
from sqlalchemy import create_engine

#Получаем настройки для подключения к базе
def get_connection_settings(db_file="creds.db"):
    try:
        # Открываем базу с настройками и берем данные из access
        conn = sqlite3.connect(db_file)
        
        settings_data = pd.read_sql_query("SELECT url, port, user, pass FROM access LIMIT 1", conn)
        
        conn.close()
        
        return settings_data.iloc[0].to_dict()
        
    except Exception as e:
        print(f"Ошибка при чтении {db_file}: {e}")
        return None

# Читаем данные из CSV файла 
def load_csv_data(filename="dataset.csv"):
    try:
        print("Чтение файла с разделителем ';'")
        data = pd.read_csv(filename, sep=';', encoding='utf-8-sig')
        print("Успешно прочитано с разделителем ';'")
        return data
        
    except Exception as e:
        print(f"Ошибка при чтении файла: {e}")
        return None

# Загрузка данных в PostgreSQL
def upload_to_database(dataframe, credentials, db_name, table_name):
    try:
        # Создаем строку подключения
        conn_string = f"postgresql://{credentials['user']}:{credentials['pass']}@{credentials['url']}:{credentials['port']}/{db_name}"
        
        engine = create_engine(conn_string)
        
        # Проверка
        test_connection = engine.connect()
        test_connection.close()
        print(f"Успешно подключились к базе {db_name}")
        
        # Смена названий колонок
        clean_columns = []
        for column in dataframe.columns:
            clean_column = column.strip().lower().replace(' ', '_').replace('-', '_').replace('.', '_').replace('(', '').replace(')', '')
            clean_columns.append(clean_column)
        
        dataframe.columns = clean_columns
        
        # Загружаем данные в таблицу
        dataframe.to_sql(
            name=table_name,
            con=engine,
            if_exists='replace',
            index=False
        )
        
        print(f"Данные записаны в таблицу {table_name}")
        return engine
        
    except Exception as e:
        print(f"Ошибка при загрузке в базу {db_name}: {e}")
        return None

# Смотрим, что именно записалось в таблицу
def show_table_data(engine, table_name, rows_to_show=5):
    try:
        # Берем несколько строк из таблицы
        result = pd.read_sql(f"SELECT * FROM {table_name} LIMIT {rows_to_show}", engine)
        
        print(f"\nПервые {rows_to_show} строк из таблицы {table_name}:")
        print(result)
        
        # Показываем информацию о таблице
        table_info = pd.read_sql(f"""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = '{table_name}'
        """, engine)
        
        print(f"\nСтруктура таблицы {table_name}:")
        print(table_info)
        
        return True
        
    except Exception as e:
        print(f"Ошибка при чтении таблицы: {e}")
        return False

# Главная функция программы
def main():
    print("=" * 50)
    print("ПРОГРАММА ДЛЯ ЗАГРУЗКИ ДАННЫХ В БАЗУ")
    print("=" * 50)
    
    # Получаем настройки подключения
    db_settings = get_connection_settings()
    
    if db_settings is None:
        print("Не удалось получить настройки подключения!")
        return
    
    # Загружаем данные из CSV файла
    csv_data = load_csv_data("dataset.csv")
    
    if csv_data is None:
        print("Не удалось загрузить данные из CSV файла!")
        return
    
    print(f"\nУспешно загружено {len(csv_data)} строк")
    print(f"Колонки: {list(csv_data.columns)}")
    
    # Показываем первые строки данных
    print("\nПервые 3 строки данных:")
    print(csv_data.head(3))
    
    print("\n" + "=" * 50)
    print("ЗАГРУЗКА ДАННЫХ В БАЗУ HOMEWORKS")
    print("=" * 50)
    
    # Загружаем данные в основную базу
    limited_data = csv_data.head(100)
    database_engine = upload_to_database(limited_data, db_settings, "homeworks", "grebennikov")
    
    if database_engine is not None:
        # Показываем что записалось
        data_check = show_table_data(database_engine, "grebennikov")
        
        # Самопроверка
        row_count = pd.read_sql("SELECT COUNT(*) FROM grebennikov", database_engine).iloc[0,0]
        print(f"\nЗагружено строк: {row_count}")

        
        if data_check:
            print("\n" + "=" * 50)
            print("ЗАГРУЗКА УСПЕШНО ЗАВЕРШЕНА!")
            print("=" * 50)
        else:
            print("Есть проблемы с записанными данными")
    else:
        print("Не удалось загрузить данные в базу")

# Запускаем программу
if __name__ == "__main__":
    main()
