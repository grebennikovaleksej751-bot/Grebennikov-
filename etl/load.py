import pandas as pd
import sqlite3
from sqlalchemy import create_engine
import urllib.parse
import os
from pathlib import Path

def get_connection_settings(db_file="creds.db"):
    """Получение настроек подключения к базе данных"""
    
    try:
        # Открываем базу с настройками и берем данные из access
        conn = sqlite3.connect(db_file)
        settings_data = pd.read_sql_query("SELECT url, port, user, pass FROM access LIMIT 1", conn)
        conn.close()
        
        return settings_data.iloc[0].to_dict()
        
    except Exception as e:
        print(f"Ошибка при чтении {db_file}: {e}")
        return None

def upload_to_database(dataframe, credentials, db_name, table_name):
    """Загрузка данных в PostgreSQL"""
    
    try:
        # Кодируем пароль для безопасного подключения
        encoded_password = urllib.parse.quote_plus(credentials['pass'])
        
        # Создаем строку подключения с явным указанием кодировки
        conn_string = f"postgresql://{credentials['user']}:{encoded_password}@{credentials['url']}:{credentials['port']}/{db_name}?client_encoding=utf8"
        
        # Создаем engine с настройками кодировки
        engine = create_engine(
            conn_string,
            connect_args={'options': '-c client_encoding=utf8'},
            echo=False
        )
        
        # Проверка подключения
        test_connection = engine.connect()
        test_connection.close()
        print(f"Успешно подключились к базе {db_name}")
        
        # Очищаем строковые данные от проблемных символов
        for col in dataframe.columns:
            if dataframe[col].dtype == 'object':
                dataframe[col] = dataframe[col].astype(str).str.encode('utf-8', 'ignore').str.decode('utf-8')
        
        # Загружаем данные в таблицу
        dataframe.to_sql(
            name=table_name,
            con=engine,
            if_exists='replace',
            index=False,
            method='multi',
            chunksize=1000
        )
        
        print(f"Данные записаны в таблицу {table_name}")
        return engine
        
    except Exception as e:
        print(f"Ошибка при загрузке в базу {db_name}: {e}")
        import traceback
        traceback.print_exc()
        return None

def save_to_parquet(dataframe, output_dir="data/processed", filename="grebennikov_processed.parquet"):
    """Сохранение данных в Parquet формат"""
    
    # Создаем директорию если не существует
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    output_path = os.path.join(output_dir, filename)
    dataframe.to_parquet(output_path, index=False)
    
    print(f"Данные сохранены в Parquet: {output_path}")
    return output_path

def validate_output_data(dataframe, expected_rows=None):
    """Валидация выходных данных"""
    
    print("Валидация выходных данных...")
    
    if dataframe.empty:
        raise ValueError("Выходные данные пустые")
    
    if expected_rows and len(dataframe) != expected_rows:
        print(f"Предупреждение: ожидалось {expected_rows} строк, получено {len(dataframe)}")
    
    # Проверяем, что числовые колонки имеют правильный тип
    if 'g3' in dataframe.columns:
        if not pd.api.types.is_float_dtype(dataframe['g3']):
            print("Предупреждение: колонка g3 не имеет тип float")
    
    print(f"Выходные данные валидны. Размер: {dataframe.shape}")
    return True

def show_table_data(engine, table_name, rows_to_show=5):
    """Просмотр данных в таблице"""
    
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
