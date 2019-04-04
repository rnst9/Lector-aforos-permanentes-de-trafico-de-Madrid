import json
import urllib
from pandas.io.json import json_normalize
import pandas as pd
from datetime import datetime
import plotly as py
import cufflinks as cf
import plotly.tools
import plotly.plotly as py
# =============================================================================
# CATÁLOGO DE DATOS POR MES Y AÑO
# =============================================================================
tittle='Aforos%20de%20tr%C3%A1fico%20en%20la%20ciudad%20de%20Madrid%20permanentes'
fmt='json'
url='https://datos.madrid.es/egob/catalogo/title/{0}.{1}'.format(tittle, fmt)

response=json.load(urllib.request.urlopen(url))
p=json_normalize(response['result']['items'][0]['distribution'])
# p = catalogo dataframe descargado, almacenada para seguimiento
k=p
k = k[k['format.value'].str.contains('text/csv')]
k.iat[-1, 4]= 'estaciones'
k['year'], k['month'] = k['title'].str.split('. ', 1).str
k=k.drop(['title'], axis=1)
k.iat[-1, 6]= 'estaciones'
k['month']=k['month'].str.lower()
# k = catalogo limpio separando meses y años. variable catalogo almacenada para seguimiento.
catalogo=k
del(fmt, p, response, tittle, url)
# =============================================================================
# SELECCIÓN DE ARCHIVOS
# =============================================================================
# funcion de lectura de datos de entrada
seleccion=[]
def seleccion_datos(a,b):
    lista=[a,b]
    seleccion.append(lista)

# =============================================================================
# seleccion de mes y año a descargar
# =============================================================================
seleccion_datos('2018','enero')
seleccion_datos('2018','febrero')

# =============================================================================
# ALMACENANDO URL DE DESCARGA
# =============================================================================
url_array=[]
for i in range(len(seleccion)):  
    m=k['accessURL'][(k.year == seleccion[i][0]) & (k.month == seleccion[i][1])].to_list()
    url_array.append(m)
del(k)
# =============================================================================
# DESCARGA DE ARCHIVOS
# =============================================================================
df_array=[]
for j in range(len(url_array)):
    data = pd.read_csv(url_array[j][0], sep=';', encoding = "ISO-8859-1")
    df_array.append(data)

df = pd.concat(df_array, axis=0, ignore_index=True)
# df1 variable de seguimiento
datos_descargados=df
df=df.dropna()
del(data, df_array, i,j,m,seleccion,url_array)
# =============================================================================
# ORDEN, LIMPIEZA Y PREPARACIÓN DEL DF PARA IMPRIMIR GRAFICAS
#https://datos.madrid.es/FWProjects/egob/Catalogo/Transporte/ficheros/Estructura_DS_AforosPermanentes.pdf
# =============================================================================

# Dataframe con numero de coches por hora, estacion y dia
# =============================================================================
df_1 = df.loc[df['FSEN'] == '2-'].append(df.loc[df['FSEN'] == '1-'])
df_2 = df.loc[df['FSEN'] == '2='].append(df.loc[df['FSEN'] == '1='])
# df_1 Datos tomados de 1:00 a 12:00 (-)
# df_2 Datos tomados de 13:00 a 24:00 (=)

df_1['FSEN']= df_1['FSEN'].str.replace(r'\D', '')
df_2['FSEN']= df_2['FSEN'].str.replace(r'\D', '')
# la columna FSEN queda codificada como 1 y 2

df_3 = pd.merge(df_1, df_2,  
                how='left', 
                left_on=["FSEN", 'FEST','FDIA'], 
                right_on = ["FSEN", 'FEST','FDIA'])

newcolumns = ['FDIA', 'FEST', 'FSEN', '00:00', '01:00', '02:00', '03:00',
              '04:00', '05:00', '06:00', '07:00', '08:00', '09:00', '10:00',
              '11:00', '12:00', '13:00', '14:00', '15:00', '16:00', '17:00',
              '18:00', '19:00', '20:00', '21:00', '22:00', '23:00']

df_3.columns = newcolumns
#df_3 contiene datos por dia, estacion, sentido y horas

df_4= df_3.groupby(['FDIA', 'FEST']).sum().reset_index()
#df_4 contiene datos por dia, estacion y hora. trafico1 variable almacenada para seguimiento
trafico1=df_4
trafico=trafico1
del(df, df_1, df_2, df_3, df_4, newcolumns)
# Dataframe con el nombre y ubicación de las estaciones 
# =============================================================================
estaciones = pd.read_csv('https://datos.madrid.es/egob/catalogo/300233-5-aforo-trafico-permanentes.csv', sep=';', encoding = "ISO-8859-1")

estaciones['estacion']='ES'
estaciones['estacion'][0:9]='ES0'
new = estaciones["Nº"].copy().apply(str)
estaciones['FEST']= estaciones['estacion'].str.cat(new, sep ="")
estaciones.drop( "estacion", axis = 1, inplace = True)
#Este dataframe contiene ['Nº', 'ESTACION', 'LATITUD', 'LONGITUD', 'FEST'], pero solo se usará la columna ESTACION

nombre_st=estaciones[['ESTACION', 'FEST']]
trafico=pd.merge(trafico, nombre_st, left_on='FEST', right_on='FEST', how='left')
trafico = trafico[['FDIA', 'FEST','ESTACION', '00:00', '01:00', '02:00', '03:00', '04:00', '05:00',
       '06:00', '07:00', '08:00', '09:00', '10:00', '11:00', '12:00', '13:00',
       '14:00', '15:00', '16:00', '17:00', '18:00', '19:00', '20:00', '21:00',
       '22:00', '23:00']]
del(new, nombre_st)
# =============================================================================
trafico['ESTACION']=trafico['FEST'].str.cat(trafico["ESTACION"], sep =" ")
trafico.drop(['FEST'], axis =1, inplace=True)
trafico1=trafico
# trafico1 es el dataframe final. variable trafico1 almacenada para seguimiento
# =============================================================================


#ABRACADABRA
trafico = trafico.set_index(['FDIA'])
trafico = trafico.set_index(['ESTACION'], append=True)
trafico = trafico.unstack('FDIA')
trafico=trafico.T
trafico=trafico.reset_index()
trafico.rename(columns={'level_0':'FHORA'}, inplace=True)

trafico['FDIA']=trafico['FDIA'].str.cat(trafico["FHORA"], sep =" ")
trafico.drop(['FHORA'], axis =1, inplace=True)
trafico['FDIA']  = trafico['FDIA'] .apply(lambda x: datetime.strptime(x, '%d/%m/%y %H:%M'))
trafico.rename(columns={'FDIA':'FECHA'}, inplace=True)
trafico.sort_index(inplace=True)
trafico2=trafico
#TRAFICO2 VARIABLE DE SEGUIMIENTO

#Este dataframe se puede agrupar por dias, horas con la suma o la media.
#la idea es lanzar querys a todo el dataframe según se quiera.
trafico.reset_index(inplace=True)
trafico=trafico.resample('D', on='FECHA').mean()
trafico.drop(['index'], axis =1, inplace=True)
del(catalogo, datos_descargados, estaciones, trafico1, trafico2)
# =============================================================================
# Enviando a plotly
# =============================================================================

plotly.tools.set_credentials_file(username='XXX', api_key='XXX')
py.sign_in('XXX', 'XXX')

cf.set_config_file(offline=False, world_readable=True, theme='ggplot')
trafico.iplot(kind='scatter', filename='enero-febreo media de trafico por dia')




















