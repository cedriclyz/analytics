from utils.gcp_utils import BQ
from utils.project import PROJECT
from google.cloud import bigquery
from datetime import datetime, timedelta
import requests,sys,time,io,csv,pandas_gbq, os
import pandas as pd

tdy_dt = datetime.strptime(sys.argv[1], '%Y%m%d')
tdy_str = datetime.strftime(tdy_dt, '%Y-%m-%d')
ytd_str = datetime.strftime(tdy_dt+timedelta(days=-1), '%Y-%m-%d')
ARGS ={
    'tdy' : tdy_str
    ,'ytd' : ytd_str
    ,'project': PROJECT[sys.argv[2]]
    ,'dataset_src' : 'dev_02'
    ,'dataset_dst' : 'ads_bi'
    ,'table_id_src': 'analytics_server_register'
    ,'table_id_dst': 'dwd_user_reg_ctry_d'
    ,'config' : sys.argv[2]
}    

schema = [
        bigquery.SchemaField("dtstatdate", "DATETIME")
        ,bigquery.SchemaField("reg_date", "DATETIME")
        ,bigquery.SchemaField("game_name", "STRING")
        ,bigquery.SchemaField("os", "STRING")
        ,bigquery.SchemaField("uid", "STRING")
        ,bigquery.SchemaField("ip", "STRING")
        ,bigquery.SchemaField("countryCode", "STRING")
        ,bigquery.SchemaField("region", "STRING")
        ,bigquery.SchemaField("city", "STRING")
        ,bigquery.SchemaField("timezone", "STRING")       
]

def get_ctry(job):
    output = io.StringIO() 
    writer = csv.writer(output)
    field = ["dtstatdate","reg_date","game_name", "os", "uid","ip","countryCode","region","city","timeZone"]
    writer.writerow(field)
    cnt = 0
    for row in job:
        input = []
        reg_date,platform,uid,ip =row[0],row[1],row[2],row[3]
        input.extend([tdy_dt,reg_date,'glompa',platform,uid,ip])

        if ip == None:input.extend(['none','none','none','none'])
        else:
            r = requests.get(f'https://freeipapi.com/api/json/{ip}')
            r.json()
            input.extend([r.json()['countryCode'],r.json()['regionName'],r.json()['cityName'],r.json()['timeZone']])
            time.sleep(2)
        writer.writerow(input)
        cnt+=1
        print(f'get_ctry_success_{cnt}')
    # print(output.getvalue())

    output.seek(0)
    return pd.read_csv(output,parse_dates=['dtstatdate','reg_date']).fillna('none')

def main():
    bq = BQ(ARGS['dataset_dst'],ARGS['config'])
   
    if bq.tableIfNotExist(ARGS['table_id_dst']) == True:
       print ('table_Notexists')
       bq.tableCreate(schema,ARGS['table_id_dst'],'dtstatdate')

    sql = '''
        select 
            exists( 
            select * 
            from {project}.{dataset_dst}.{table_id_dst}
            where dtstatdate = '{tdy}')
        '''.format(**ARGS)
    
    if bq.dataIfExist(sql) == True:
       print ('overwriting existing data')

       sql = '''
            delete
            from {project}.{dataset_dst}.{table_id_dst}
            where dtstatdate = '{tdy}'
        '''.format(**ARGS)
       
       bq.execute(sql)
    
    sql = '''
            select
                datetime(time) reg_date
                ,json_extract_scalar(data,'$.platform') os
                ,json_extract_scalar(data,'$.uid') uid
                ,json_extract_scalar(data,'$.ip') ip
            from 
                monster-analytics-388208.dev02.analytics_server_register
            where 
                date(time) = '{tdy}'
    '''.format(**ARGS)
    job = bq.execute(sql)

    df = get_ctry(job)
    
    pandas_gbq.to_gbq(df, '{dataset_dst}.{table_id_dst}'.format(**ARGS)
                  ,project_id ='{project}'.format(**ARGS)
                  , if_exists = 'append')

if __name__ == '__main__':
   main()