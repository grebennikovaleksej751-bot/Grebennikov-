import os
import pandas as pd
from sqlalchemy import create_engine, inspect
from dotenv import load_dotenv

# Загрузка переменных окружения из .env файла
def load_pg_creds(env_file=".env"):
    try:
        load_dotenv(env_file)
        
        creds = {
            'url': os.getenv('POSTGRES_URL'),
            'port': os.getenv('POSTGRES_PORT'),
            'user': os.getenv('POSTGRES_USER'),
            'pass': os.getenv('POSTGRES_PASSWORD')
        }
        
        # Проверяем, что все  загружено
        for key, value in creds.items():
            if value is None:
                print(f"Предупреждение: переменная {key} не найдена в .env файле")
        
        print("Учетные данные загружены из .env файла")
        return creds
    except Exception as e:
        print(f"Ошибка при чтении .env файла: {e}")
        return None

# Загрузка и подготовка данных с приведением типов 
def prepare_dataset(file_path="dataset.csv", max_rows=100):
    try:
        
        df = pd.read_csv(file_path, sep=';', nrows=max_rows)
            
        df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]
        
        df = convert_data_types(df)
        
        print(f"Загружено {len(df)} строк с колонками: {list(df.columns)}")
        return df
    except Exception as e:
        print(f"Ошибка при загрузке dataset.csv: {e}")
        return None
    

def convert_data_types(df):
    """Приведение типов данных"""
    print("Приведение типов данных...")
    
    df_clean = df.copy()
    
    # Числовые колонки
    if 'number' in df_clean.columns:
        df_clean['number'] = pd.to_numeric(df_clean['number'], errors='coerce').fillna(0).astype('int64')
    
    if 'id' in df_clean.columns:
        df_clean['id'] = pd.to_numeric(df_clean['id'], errors='coerce').fillna(0).astype('int64')
    
    if 'number_of_atoms' in df_clean.columns:
        df_clean['number_of_atoms'] = pd.to_numeric(df_clean['number_of_atoms'], errors='coerce').fillna(0).astype('int64')
    
    if 'g3' in df_clean.columns:
        df_clean['g3'] = pd.to_numeric(df_clean['g3'].astype(str).str.replace(',', '.'), errors='coerce').astype('float64')
    
    # Текстовые колонки
    text_columns = ['chemical_compound', 'name_of_compound', 'authors', 
                   'literature', 'symmetry', 'type', 'topology']
    
    for col in text_columns:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].fillna('').astype(str)
    
    print("Типы данных после преобразования:")
    for col, dtype in df_clean.dtypes.items():
        print(f"  {col}: {dtype}")
    
    return df_clean

# Запись в PostgreSQL 
def upload_to_postgres(df, creds, table_name="grebennikov"):
    try:
        # Подключение к homeworks
        conn_str = f"postgresql+psycopg2://{creds['user']}:{creds['pass']}@{creds['url']}:{creds['port']}/homeworks"
        print(f"Подключаемся к: {creds['url']}:{creds['port']}/homeworks")
        
        engine = create_engine(conn_str, pool_recycle=3600, connect_args={'connect_timeout': 10})

        # Проверяем соединение
        with engine.connect() as test_conn:
            print("Соединение с homeworks установлено успешно!")

        # Запись данных
        with engine.begin() as conn:
            df.to_sql(
                name=table_name,
                con=conn,
                schema="public",
                if_exists="replace",
                index=False,
                method='multi'
            )

        # Проверка наличия таблицы
        inspector = inspect(engine)
        tables = inspector.get_table_names(schema="public")
        if table_name in tables:
            print(f"Таблица '{table_name}' успешно создана в схеме public!")
            
            # информация о структуре таблицы
            columns_info = inspector.get_columns(table_name, schema="public")
            print(f"\nСтруктура таблицы {table_name}:")
            for col in columns_info:
                print(f"  {col['name']}: {col['type']}")
                
            return engine
        else:
            print(f"Таблица '{table_name}' не найдена. Текущие таблицы: {tables}")
            return None

    except Exception as e:
        print(f"Ошибка при загрузке в PostgreSQL: {e}")
        return None

# Проверка результата 
def verify_upload(engine, table_name="grebennikov"):
    
    try:
        # общее количество строк
        count_query = f"SELECT COUNT(*) as total_rows FROM public.{table_name}"
        count_result = pd.read_sql(count_query, con=engine)
        total_rows = count_result.iloc[0]['total_rows']
        
        print(f"\nВсего загружено строк: {total_rows}")
        
        # Показываем первые 5 строк
        query = f"SELECT * FROM public.{table_name} LIMIT 5"
        sample = pd.read_sql(query, con=engine)
        print("\nПервые 5 строк загруженных данных:")
        print(sample.to_string(index=False))
        
        return True
    except Exception as e:
        print(f"Ошибка при проверке данных: {e}")
        return False

# Основная программа
if __name__ == "__main__":
    print("ЗАГРУЗКА ДАННЫХ В POSTGRESQL (первые 100 строк)")
    print("=" * 50)
    
    # Чтение учётных данных из .env
    print("Чтение учётных данных из .env файла")
    credentials = load_pg_creds()
    if credentials is None:
        print("Не удалось загрузить учетные данные. Проверьте файл .env")
        exit(1)
    
    # Подготовка датасета (только первые 100 строк)
    print("Подготовка датасета (первые 100 строк)")
    data = prepare_dataset("dataset.csv", max_rows=100)
    if data is None:
        print("Не удалось загрузить данные из dataset.csv")
        exit(1)
    
    print(f"Размер датасета: {data.shape[0]} строк, {data.shape[1]} колонок")
    
    # Загрузка в PostgreSQL
    print("\nЗагрузка в PostgreSQL (homeworks)")
    engine = upload_to_postgres(data, credentials, table_name="grebennikov")
    if engine is None:
        print("Не удалось загрузить данные в базу")
        exit(1)
    
    # Проверка результата
    print("\nПроверка результата")
    success = verify_upload(engine, "grebennikov")
    
    if success:
        print("\n" + "="*50)
        print("ЗАГРУЗКА УСПЕШНО ЗАВЕРШЕНА!")
        print(f"Первые 100 строк загружены в таблицу 'grebennikov' базы данных 'homeworks'")
    else:
        print("\nЗАГРУЗКА ЗАВЕРШЕНА С ПРЕДУПРЕЖДЕНИЯМИ")
