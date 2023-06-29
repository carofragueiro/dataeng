# dateeng

Consulto la API de Alpha Advantage, con los valores de distintas acciones:

TIME_SERIES_DAILY_ADJUSTED 
This API returns raw (as-traded) daily open/high/low/close/volume values, daily adjusted close values, and historical split/dividend events of the global equity specified, covering 20+ years of historical data.
https://www.alphavantage.co/documentation/

En este caso consulté los de IBM, para eso se lo paso como parámetro a la API en la URL del request. 

Luego creo la tabla vacía en redshift, y a continuación la voy cargando con las filas de la respuesta de la API. 

Cambios en relación a entrega 1: 
- Agregué el symbol como columna de la tabla (IBM)
- Inserté la tabla con SQL query en vez desde SQLAlchemy directamente, para poder usar distkey y sortkey.
- Subí el script al repo
