# Если библиотека не установлена, нужно сделать pip install s3fs

import s3fs
import pandas as pd

config = {
    'access_key': 'YOUR_ACCESS_KEY',
    'secret_key': 'YOUR_SECRET_KEY',
    'bucket': 'YOUR_BUCKET_NAME',
    'endpoint_url': 'https://storage.yandexcloud.net' 
}

# Подключение к S3
fs = s3fs.S3FileSystem(
    key=config['access_key'],
    secret=config['secret_key'],
    client_kwargs={
        "endpoint_url": config['endpoint_url'],
        "region_name": "ru-central1"
    }
)

# Посмотреть список файлов в бакете
files = fs.ls(config['bucket'])
print(files)

# Загрузить первую таблицу с S3 в Pandas
# Или вручную задать название файла из списка files
file_path = f"s3://{files[0]}"                     
df = pd.read_csv(
    file_path,
    storage_options={
        "key": config['access_key'],
        "secret": config['secret_key'],
        "client_kwargs": {
            "endpoint_url": config['endpoint_url'],
            "region_name": config['region']
        }
    }
)