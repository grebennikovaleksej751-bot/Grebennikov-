import gdown
import pandas as pd

FILE_ID = "12MkiB6pcc9Tie_qV3v1xFJuJvQY83qDl"
URL = f"https://drive.google.com/uc?id={FILE_ID}"

OUTPUT = "dataset.csv"  # Тут качаем файл
gdown.download(URL, OUTPUT, quiet=False)

raw_data = pd.read_csv(OUTPUT,sep=';', encoding='utf-8-sig')

print(raw_data.head(10)) # Тут выводим первые 10 значений из датасета

print(raw_data.dtypes)  # показывает текущие типы данных, которые определились автоматически

raw_data["G3"] = raw_data["G3"].str.replace(',', '.').astype(float) # Заменяем запятые на точки в столбце G3 и преобразуем в float

print("\nПосле преобразования:")
print(raw_data.head(10))
print("\nТип данных G3:", raw_data["G3"].dtype) # Проверяем, что для G3 изменился тип данных

print(raw_data.dtypes) # Выводим еще раз типы данных для проверки
