import os
import pandas as pd
import sqlite3
from sqlalchemy import create_engine, Text, Integer, Float

def get_connection_settings(db_file="creds.db"):
    #Получаем настройки для подключения к базе
    try:
        with sqlite3.connect(db_file) as conn:
            settings_data = pd.read_sql_query("SELECT url, port, user, pass FROM access LIMIT 1", conn)
            return settings_data.iloc[0].to_dict()
    except Exception as e:
        print(f"Ошибка при чтении {db_file}: {e}")
        return None

def load_data():
    #Загружаем данные из CSV файла
    try:
        print("Загрузка данных из CSV...")
        df = pd.read_csv("dataset.csv", sep=';', encoding='utf-8-sig')
        print(f"Загружено {len(df)} строк")
        return df.head(100)  # Берем только 100 строк
    except Exception as e:
        print(f"Ошибка при загрузке CSV: {e}")
        return None

def convert_data_types(df):
    #Приведение типов данных
    print("Приведение типов данных...")
    
    df_clean = df.copy()
    
    # Числовые колонки
    if 'Number' in df_clean.columns:
        df_clean['Number'] = pd.to_numeric(df_clean['Number'], errors='coerce').fillna(0).astype('int64')
    
    if 'ID' in df_clean.columns:
        df_clean['ID'] = pd.to_numeric(df_clean['ID'], errors='coerce').fillna(0).astype('int64')
    
    if 'Number of atoms' in df_clean.columns:
        df_clean['Number of atoms'] = pd.to_numeric(df_clean['Number of atoms'], errors='coerce').fillna(0).astype('int64')
    
    if 'G3' in df_clean.columns:
        df_clean['G3'] = pd.to_numeric(df_clean['G3'].astype(str).str.replace(',', '.'), errors='coerce').astype('float64')
    
    # Текстовые колонки
    text_columns = ['Chemical compound', 'Name of compound', 'Authors', 
                   'Literature', 'Symmetry', 'Type', 'Topology']
    
    for col in text_columns:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].fillna('').astype(str)
    
    print("Типы данных после преобразования:")
    for col, dtype in df_clean.dtypes.items():
        print(f"  {col}: {dtype}")
    
    return df_clean

def upload_to_database(dataframe, credentials, table_name):
   #Загружаем данные в PostgreSQL
    try:
        # Создаем подключение
        conn_string = f"postgresql://{credentials['user']}:{credentials['pass']}@{credentials['url']}:{credentials['port']}/homeworks"
        engine = create_engine(conn_string)
        
        # Очищаем названия колонок
        clean_columns = []
        for column in dataframe.columns:
            clean_column = (column.strip()
                          .lower()
                          .replace(' ', '_')
                          .replace('-', '_')
                          .replace('.', '_')
                          .replace('(', '')
                          .replace(')', ''))
            clean_columns.append(clean_column)
        
        dataframe.columns = clean_columns
        
        # Определяем SQL типы
        sql_dtypes = {}
        for col in dataframe.columns:
            if dataframe[col].dtype in ['int64', 'Int64']:
                sql_dtypes[col] = Integer
            elif dataframe[col].dtype == 'float64':
                sql_dtypes[col] = Float
            else:
                sql_dtypes[col] = Text
        
        # Загружаем данные
        rows_count = dataframe.shape[0]
        print(f"Загружаем {rows_count} строк в базу...")
        
        dataframe.to_sql(
            name=table_name, 
            con=engine, 
            if_exists='replace', 
            index=False,
            dtype=sql_dtypes
        )
        
        print(f"Данные записаны в таблицу {table_name}")
        return engine, rows_count
        
    except Exception as e:
        print(f"Ошибка при загрузке в базу: {e}")
        return None, 0

def self_check(engine, table_name, expected_rows):
    #самопроверка
    print("\n" + "="*50)
    print("САМОПРОВЕРКА")
    print("="*50)
    
    try:
        # Проверяем количество строк
        result = pd.read_sql(f"SELECT COUNT(*) as row_count FROM {table_name}", engine)
        actual_rows = result['row_count'].iloc[0]
        
        print(f"Ожидаемое количество строк: {expected_rows}")
        print(f"Фактическое количество строк: {actual_rows}")
        
        if actual_rows == expected_rows:
            print("Количество строк совпадает!")
        else:
            print("Ошибка: количество строк не совпадает!")
            return False
        
        # Показываем структуру таблицы
        table_info = pd.read_sql(f"""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = '{table_name}'
        """, engine)
        
        print(f"\nСтруктура таблицы:")
        print(table_info)
        
        # Показываем первые 3 строки
        sample_data = pd.read_sql(f"SELECT * FROM {table_name} LIMIT 3", engine)
        print(f"\nПервые 3 строки таблицы:")
        print(sample_data)
        
        return True
        
    except Exception as e:
        print(f" Ошибка при самопроверке: {e}")
        return False

def main():
    print("=" * 50)
    print("ЗАГРУЗКА ДАННЫХ В БАЗУ")
    print("=" * 50)
    
    # Получаем настройки подключения
    db_settings = get_connection_settings()
    if not db_settings:
        print("Не удалось получить настройки подключения!")
        return
    
    # Загружаем данные
    data = load_data()
    if data is None or data.empty:
        print("Не удалось загрузить данные!")
        return
    
    # Приводим типы
    data = convert_data_types(data)
    
    # Загружаем в базу
    engine, expected_rows = upload_to_database(data, db_settings, "grebennikov")
    
    if engine:
        # Выполняем самопроверку
        if self_check(engine, "grebennikov", expected_rows):
            print("\n" + "=" * 50)
            print("ЗАГРУЗКА УСПЕШНО ЗАВЕРШЕНА!")
            print("=" * 50)
        else:
            print("\n" + "=" * 50)
            print("ОШИБКА ПРИ ЗАГРУЗКЕ!")
            print("=" * 50)
    else:
        print("Не удалось загрузить данные в базу")

if __name__ == "__main__":
    main()

# Запускаем программу
if __name__ == "__main__":
    main()
