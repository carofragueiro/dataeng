import requests
import json
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
import os

# PONER ACÁ USUARIO Y PASSWORD PARA ENTRAR
os.environ['API_USER'] = ''
os.environ['API_PASSWORD'] = ''


USER = os.getenv('API_USER')
PASSWORD = os.environ.get('API_PASSWORD')


# creo la url
url = URL.create(
drivername='redshift+redshift_connector',
host='data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com',
port=5439, 
database='data-engineer-database', 
username=USER,
password=PASSWORD 
)

# creo la conexión
engine = create_engine(url)

# consulto la API, datos de trading de IBM
url = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol=IBM&apikey=ZVHFO9EF0AUJ5CBF'
r = requests.get(url)
data = r.json()



# creo la tabla 
from sqlalchemy import MetaData
meta = MetaData()

RedshiftDBTable = sqlalchemy.Table(
'IBM_data',
meta,
sqlalchemy.Column('date', sqlalchemy.VARCHAR(80)),
sqlalchemy.Column('open', sqlalchemy.VARCHAR(80)),
sqlalchemy.Column('high', sqlalchemy.VARCHAR(80)),
sqlalchemy.Column('low', sqlalchemy.VARCHAR(80)),
sqlalchemy.Column('close', sqlalchemy.VARCHAR(80))
)

# si ya existe la borro y luego la creo
if sqlalchemy.inspect(engine).has_table('ibm_data'):
    RedshiftDBTable.drop(bind=engine)

RedshiftDBTable.create(bind=engine)


# preparo el insert

from sqlalchemy import orm as sa_orm

Session = sa_orm.sessionmaker()
Session.configure(bind=engine)
session = Session()

for dia in data['Time Series (Daily)']:
    insert_data_row = RedshiftDBTable.insert().values(
            date=dia,
            open=data['Time Series (Daily)'][dia]["1. open"],
            high=data['Time Series (Daily)'][dia]["2. high"],
            low=data['Time Series (Daily)'][dia]["3. low"],
            close=data['Time Series (Daily)'][dia]["4. close"],
         )
        # ejecuto el insert
    session.execute(insert_data_row)
    session.commit()
