
import os
from os.path import join
import pandas as pd 
from datetime import datetime, timedelta

# date interval
date_start = datetime.today()
date_end = date_start + timedelta(days=7)

# date formating
date_start = date_start.strftime('%Y-%m-%d')
date_end = date_end.strftime('%Y-%m-%d')

city = 'RiodeJaneiro'
key = 'G45UFSJZLMPXARA77KV84HL2T'

URL = join('https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/',
            f'{city}/{date_start}/{date_end}?unitGroup=metric&include=days&key={key}&contentType=csv')

data = pd.read_csv(URL)
print(data.head())

file_path = f'/home/fernando/Documents/datapipeline/semana={date_start}/'
os.mkdir(file_path)

data.to_csv(file_path + 'row_data.csv')
data[['datetime', 'tempmin', 'temp', 'tempmax']].to_csv(file_path + 'temperatures.csv')
data[['datetime', 'description', 'icon']].to_csv(file_path + 'conditions.csv')

