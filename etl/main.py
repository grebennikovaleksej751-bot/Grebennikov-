import argparse
import sys
import os
import pandas as pd

# Добавляем путь к пакету
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl.extract import download_dataset, load_raw_data, validate_raw_data
from etl.transform import convert_data_types, clean_column_names, limit_data
from etl.load import (get_connection_settings, upload_to_database, 
                     save_to_parquet, validate_output_data, show_table_data)
from etl.validate import validate_dataframe_structure, validate_numeric_columns, check_data_quality

def run_etl_pipeline(max_rows=100, output_format='both', table_name='grebennikov'):
    """
    Запуск полного ETL пайплайна
    
    Args:
        max_rows: Максимальное количество строк для загрузки
        output_format: Формат вывода ('db', 'parquet', 'both')
        table_name: Имя таблицы в базе данных
    """
    
    print("=" * 50)
    print("ЗАПУСК ETL ПАЙПЛАЙНА")
    print("=" * 50)
    
    try:
        # Extract
        print("\n1. EXTRACT - Загрузка данных")
        print("-" * 30)
        
        csv_path = download_dataset()
        raw_data = load_raw_data(csv_path)
        validate_raw_data(raw_data)
        
        # Transform
        print("\n2. TRANSFORM - Преобразование данных")
        print("-" * 30)
        
        transformed_data = convert_data_types(raw_data)
        transformed_data = clean_column_names(transformed_data)
        final_data = limit_data(transformed_data, max_rows=max_rows)
        
        print(f"После трансформации: {final_data.shape}")
        print(f"Типы данных:")
        for col, dtype in final_data.dtypes.items():
            print(f"  {col}: {dtype}")
        
        # Валидация 
        validate_dataframe_structure(final_data)
        validate_numeric_columns(final_data)
        check_data_quality(final_data)
        
        # load
        print("\n3. LOAD - Выгрузка данных")
        print("-" * 30)
        
        # Загрузка в Parquet
        if output_format in ['parquet', 'both']:
            parquet_path = save_to_parquet(final_data)
            print(f"Сохранено в Parquet: {parquet_path}")
        
        # Загрузка в базу данных
        if output_format in ['db', 'both']:
            db_settings = get_connection_settings()
            
            if db_settings is None:
                print("Не удалось получить настройки подключения к БД")
            else:
                engine = upload_to_database(final_data, db_settings, "homeworks", table_name)
                
                if engine is not None:
                    show_table_data(engine, table_name)
                    
                    # Проверка количества строк
                    row_count = pd.read_sql(f"SELECT COUNT(*) FROM {table_name}", engine).iloc[0,0]
                    print(f"\nЗагружено строк в БД: {row_count}")
        
        # Финальная валидация
        print("\n4. ВАЛИДАЦИЯ РЕЗУЛЬТАТОВ")
        print("-" * 30)
        validate_output_data(final_data, expected_rows=max_rows)
        
        print("\n" + "=" * 50)
        print("ETL ПАЙПЛАЙН УСПЕШНО ЗАВЕРШЕН!")
        print("=" * 50)
        
    except Exception as e:
        print(f"\nОШИБКА В ETL ПАЙПЛАЙНЕ: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description='ETL пайплайн для обработки химических данных')
    
    parser.add_argument('--max-rows', type=int, default=100,
                       help='Максимальное количество строк для обработки (обязательно)')
    parser.add_argument('--output', choices=['db', 'parquet', 'both'], default='both',
                       help='Формат вывода данных')
    parser.add_argument('--table-name', default='grebennikov',
                       help='Имя таблицы в базе данных')
    
    args = parser.parse_args()
    
    # Проверяем обязательный аргумент
    if args.max_rows <= 0:
        parser.error("--max-rows должен быть положительным числом")
    
    run_etl_pipeline(
        max_rows=args.max_rows,
        output_format=args.output,
        table_name=args.table_name
    )

if __name__ == "__main__":
    main()
