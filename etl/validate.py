import pandas as pd
from typing import List


def validate_raw_data(df: pd.DataFrame) -> bool:
    """
    Validate raw data after extraction
    
    Args:
        df: Raw DataFrame
        
    Returns:
        Validation success status
    """
    print("Validating raw data...")
    
    validation_passed = True
    
    
    if df.empty:
        print("✗ Validation failed: DataFrame is empty")
        return False
    
    print(f"✓ Data size: {df.shape[0]} rows, {df.shape[1]} columns")
    
    required_columns = ['G3']  
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        print(f"✗ Missing required columns: {missing_columns}")
        validation_passed = False
    else:
        print("✓ All required columns present")
    
    empty_columns = df.columns[df.isnull().all()].tolist()
    if empty_columns:
        print(f" Warning: Completely empty columns: {empty_columns}")
    
    
    critical_columns = ['G3']
    for col in critical_columns:
        if col in df.columns:
            # Check for NaN values
            nan_count = df[col].isna().sum()
            if nan_count > 0:
                print(f"⚠ Warning: Column '{col}' has {nan_count} NaN values")
            
            # Check data type (basic check)
            print(f"✓ Column '{col}': {df[col].dtype}")
    
    # проверка повторяющихся строк
    duplicate_count = df.duplicated().sum()
    if duplicate_count > 0:
        print(f"⚠ Warning: Found {duplicate_count} duplicate rows")
    
    return validation_passed


def validate_loaded_data(df: pd.DataFrame) -> bool:
    """
    Validate data after transformation and before loading
    
    Args:
        df: Transformed DataFrame
        
    Returns:
        Validation success status
    """
    print("Validating loaded data...")
    
    validation_passed = True
    
    # правильность типов данных
    if 'g3' in df.columns:
        if not pd.api.types.is_float_dtype(df['g3']):
            print("✗ Validation failed: Column 'g3' should be float")
            validation_passed = False
        else:
            print("✓ Column 'g3' is float type")
    
    
    numerical_columns = df.select_dtypes(include=['number']).columns
    
    for col in numerical_columns:
        if col in df.columns:
            # Check for infinite values
            inf_count = (df[col] == float('inf')).sum() + (df[col] == float('-inf')).sum()
            if inf_count > 0:
                print(f"⚠ Warning: Column '{col}' has {inf_count} infinite values")
            
            
            if df[col].notna().any():
                print(f"✓ Column '{col}': min={df[col].min():.2f}, max={df[col].max():.2f}, mean={df[col].mean():.2f}")
    
    # проверка пробелов
    text_columns = df.select_dtypes(include=['object']).columns
    for col in text_columns:
        if col in df.columns:
            # Check for strings with only whitespace
            whitespace_only = df[col].str.strip().eq('').sum()
            if whitespace_only > 0:
                print(f"⚠ Warning: Column '{col}' has {whitespace_only} empty/whitespace-only values")
    
    # Проверка на пустые строки
    completely_null_rows = df.isnull().all(axis=1).sum()
    if completely_null_rows > 0:
        print(f"⚠ Warning: Found {completely_null_rows} completely null rows")
    
    print("✓ Loaded data validation completed")
    return validation_passed


def validate_database_data(
    engine: object, 
    table_name: str = "grebennikov"
) -> bool:
    """
    Validate data in the database
    
    Args:
        engine: SQLAlchemy engine
        table_name: Table name to validate
        
    Returns:
        Validation success status
    """
    try:
        
        count_query = f"SELECT COUNT(*) as row_count FROM public.{table_name}"
        count_result = pd.read_sql(count_query, con=engine)
        row_count = count_result.iloc[0]['row_count']
        
        if row_count == 0:
            print("✗ Database validation failed: Table is empty")
            return False
        
        print(f"✓ Database validation: {row_count} rows in table '{table_name}'")
        return True
        
    except Exception as e:
        print(f"✗ Database validation error: {e}")
        return False
