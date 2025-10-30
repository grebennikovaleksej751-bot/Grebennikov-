import gdown
import pandas as pd
import os
from pathlib import Path

def download_dataset(file_id="1nlMjNIXTlpgZxeLT62wG5IO3hWqsBfH1", output_dir="data/raw"):
    """Загрузка датасета с Google Drive"""
    
    # Создаем директорию если не существует
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    output_path = os.path.join(output_dir, "dataset.csv")
    url = f"https://drive.google.com/uc?id={file_id}"
    
    print(f"Загрузка датасета в {output_path}...")
    gdown.download(url, output_path, quiet=False)
    
    return output_path

def load_raw_data(filepath, encoding='utf-8-sig'):
    """Загрузка сырых данных из CSV файла"""
    
    print(f"Чтение файла {filepath} с разделителем ';'")
    
    # Пробуем разные кодировки
    encodings_to_try = [encoding, 'cp1251', 'latin1', 'utf-8']
    
    for enc in encodings_to_try:
        try:
            data = pd.read_csv(filepath, sep=';', encoding=enc)
            print(f"Успешно прочитано с кодировкой '{enc}'")
            return data
        except UnicodeDecodeError:
            continue
        except Exception as e:
            print(f"Ошибка с кодировкой {enc}: {e}")
            continue
    
    raise Exception("Не удалось прочитать файл ни с одной из кодировок")

def validate_raw_data(dataframe):
    """Валидация сырых данных"""
    
    print("Валидация сырых данных...")
    
    # Проверяем наличие обязательных колонок
    required_columns = ['G3']
    missing_columns = [col for col in required_columns if col not in dataframe.columns]
    
    if missing_columns:
        raise ValueError(f"Отсутствуют обязательные колонки: {missing_columns}")
    
    # Проверяем, что данные не пустые
    if dataframe.empty:
        raise ValueError("Датасет пустой")
    
    print(f"Данные валидны. Размер: {dataframe.shape}")
    print(f"Колонки: {list(dataframe.columns)}")
    
    return True
