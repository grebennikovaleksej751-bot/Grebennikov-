import gdown
import pandas as pd

FILE_ID = "12MkiB6pcc9Tie_qV3v1xFJuJvQY83qDl"
URL = f"https://drive.google.com/uc?id={FILE_ID}"

OUTPUT = "dataset.csv"  # Тут качаем файл
gdown.download(URL, OUTPUT, quiet=False)

raw_data = pd.read_csv(OUTPUT,sep=';', encoding='utf-8-sig')

print(raw_data.head(10)) # Тут выводим первые 10 значений из датасета
