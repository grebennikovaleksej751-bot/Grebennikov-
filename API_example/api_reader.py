import requests
import pandas as pd

def get_sunrise_sunset_data():
    """Получаем данные о восходе и закате через Open-Meteo API"""
    
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 59.93863,   # Санкт-Петербург
        "longitude": 30.31413,
        "daily": ["sunrise", "sunset"],  # Ключевое изменение - список
        "timezone": "auto",  # Автоматическое определение часового пояса
        "forecast_days": 16  # Максимум 16 дней для бесплатного API
    }
    
    response = requests.get(url, params=params)
    print(f"Status: {response.status_code}")
    
    if response.status_code == 200:
        data = response.json()
        print("Данные успешно получены!")
        
        # Создаем DataFrame
        df = pd.DataFrame({
            'date': data['daily']['time'],
            'sunrise': data['daily']['sunrise'],
            'sunset': data['daily']['sunset']
        })
        
        return df
    else:
        print(f"Ошибка: {response.status_code}")
        print(f"Ответ сервера: {response.text}")  # Покажет детали ошибки
        return None

# Запускаем
