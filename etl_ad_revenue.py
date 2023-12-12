from utils.gcp_utils import BQ
from utils.project import PROJECT,SECRET
from google.cloud import bigquery
from datetime import datetime, timedelta
import requests,sys,time,io,csv,pandas_gbq, os
import pandas as pd

global ARGS_UNITY,tdy_dt

tdy_dt = datetime.strptime(sys.argv[1], '%Y%m%d')
tdy_str = datetime.strftime(tdy_dt, '%Y-%m-%d')
ytd_str = datetime.strftime(tdy_dt+timedelta(days=-1), '%Y-%m-%d')


ARGS_UNITY ={
    'organizationId' : SECRET['unity_org']
    ,'apiKeyValue' : SECRET['unity']
    ,'groupBy':'country,placement,platform,game'
    ,'fields': 'start_count,view_count,revenue_sum'
    ,'scale' :'hour'
    ,'start':f'{tdy_str}T00:00:00Z'
    ,'end':f'{tdy_str}T23:59:00Z'
    ,'gameIds1':'5351290'
    #{''5351291','}
}

def get_adsRevDf():
    url ='https://monetization.api.unity.com/stats/v1/operate/organizations/{organizationId}?groupBy={groupBy}&fields={fields}&scale={scale}&start={start}&end={end}&apikey={apiKeyValue}&gameIds={gameIds1},{gameIds2}'.format(**ARGS_UNITY)
    print(url)
    r = requests.get(url)
    print(r.status_code)
    buff = io.StringIO(r.text)

    df= pd.read_csv(buff)
    # print(df)
    return df

ARGS ={
    'tdy' : tdy_str
    ,'ytd' : ytd_str
    ,'project': PROJECT[sys.argv[2]]
    # ,'dataset_src' : 'dev_02'
    ,'dataset_dst' : 'ads_bi'
    # ,'table_id_src': 'analytics_server_register'
    ,'table_id_dst': 'etl_ad_revenue'
    ,'config' : sys.argv[2]
}    

schema = [
        bigquery.SchemaField("imp_date", "DATETIME")
        ,bigquery.SchemaField("timestamp", "STRING")
        ,bigquery.SchemaField("country", "STRING")
        ,bigquery.SchemaField("placement", "STRING")
        ,bigquery.SchemaField("platform", "STRING")
        ,bigquery.SchemaField("source_game_id", "NUMERIC")
        ,bigquery.SchemaField("source_name", "STRING")
        ,bigquery.SchemaField("start_count", "NUMERIC")
        ,bigquery.SchemaField("view_count", "NUMERIC")
        ,bigquery.SchemaField("revenue_sum", "FLOAT")       
]
def main():
    bq = BQ(ARGS['dataset_dst'],ARGS['config'])
   
    if bq.tableIfNotExist(ARGS['table_id_dst']) == True:
       print ('table_Notexists')
       bq.tableCreate(schema,ARGS['table_id_dst'],'imp_date')

    sql = '''
        select 
            exists( 
            select * 
            from {project}.{dataset_dst}.{table_id_dst}
            where imp_date = '{tdy}')
        '''.format(**ARGS)
    
    if bq.dataIfExist(sql) == True:
       print ('overwriting existing data')

       sql = '''
            delete
            from {project}.{dataset_dst}.{table_id_dst}
            where imp_date = '{tdy}'
        '''.format(**ARGS)
       
       bq.execute(sql)
    
    df = get_adsRevDf()

    df['imp_date'] = tdy_dt
    df = df[['imp_date','timestamp','country','placement','platform','source_game_id'
              ,'source_name','start_count','view_count','revenue_sum']]
    # print(df.dtypes)
    pandas_gbq.to_gbq(df, '{dataset_dst}.{table_id_dst}'.format(**ARGS)
                  ,project_id ='{project}'.format(**ARGS)
                  , if_exists = 'append')

if __name__ == '__main__':
   main()