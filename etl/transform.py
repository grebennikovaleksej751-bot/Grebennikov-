import pandas as pd

def convert_data_types(dataframe):
    """
    Приведение типов данных для датасета химических соединений
    """
    df = dataframe.copy()
    
    print("Начало приведения типов данных...")
    
    # Сохраняем исходные типы для отладки
    original_dtypes = df.dtypes
    print("Исходные типы данных:")
    for col, dtype in original_dtypes.items():
        print(f"  {col}: {dtype}")
    
    # Обработка числовых колонок
    numeric_columns = ['Number', 'ID', 'G3', 'Number of atoms']
    
    for col in numeric_columns:
        if col in df.columns:
            print(f"Обработка колонки {col}...")
            # Заменяем запятые на точки и преобразуем в числовой формат
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str).str.replace(',', '.')
                df[col] = pd.to_numeric(df[col], errors='coerce')

    # Обработка колонки G3 (вероятно, дробные числа)
    if 'G3' in df.columns:
        print("Обработка колонки G3...")
        # Убеждаемся, что это float
        df['G3'] = pd.to_numeric(df['G3'], errors='coerce').astype('float64')
    
    # Обработка текстовых колонок
    text_columns = ['Chemical compound', 'Name of compound', 'Authors', 
                   'Literature', 'Symmetry', 'Type', 'Topology']
    
    for col in text_columns:
        if col in df.columns:
            # Преобразуем в строку и заменяем NaN на пустые строки
            df[col] = df[col].astype(str).replace('nan', '').replace('None', '')
    
    # Проверяем результаты преобразования
    print("\nТипы данных после преобразования:")
    for col, dtype in df.dtypes.items():
        print(f"  {col}: {dtype}")
    
    return df

def clean_column_names(dataframe):
    """Очистка названий колонок для базы данных"""
    
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
    return dataframe

def limit_data(dataframe, max_rows=100):
    """Ограничение количества строк (для задания - max 100 строк)"""
    return dataframe.head(max_rows)
