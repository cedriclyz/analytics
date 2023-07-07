from utils.gcp_utils import BQ
from utils.project import PROJECT
from google.cloud import bigquery
from datetime import datetime, timedelta
import sys 

tdy_dt = datetime.strptime(sys.argv[1], '%Y%m%d')
tdy_str = datetime.strftime(tdy_dt, '%Y-%m-%d')
ytd_str = datetime.strftime(tdy_dt+timedelta(days=-1), '%Y-%m-%d')
ARGS ={
    'tdy' : tdy_str
    ,'ytd' : ytd_str
    ,'project': PROJECT[sys.argv[2]]
    ,'dataset' : 'prod'
    ,'dataset_dst' : 'ads_bi'
    ,'table_id_src': 'analytics_server_register'
    ,'table_id_dst': 'dwd_full_user_reg_d'
    ,'config' : sys.argv[2]
}    

schema = [
        bigquery.SchemaField("dtstatdate", "DATE")
        ,bigquery.SchemaField("reg_date", "DATE")
        ,bigquery.SchemaField("game_name", "STRING")
        ,bigquery.SchemaField("os", "STRING")
        ,bigquery.SchemaField("uid", "STRING")
]

if __name__ == '__main__':
    bq = BQ(ARGS['dataset_dst'],ARGS['config'])
   
    if bq.tableIfNotExist(ARGS['table_id_dst']) == True:
       print ('table_Notexists')
       bq.tableCreate(schema,ARGS['table_id_dst'])

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
    INSERT INTO {project}.{dataset_dst}.{table_id_dst} (
        dtstatdate
        ,reg_date
        ,game_name
        ,os
        ,uid
    ) 
    select *
    from
    (
        select
            date('{tdy}') dtstatdate 
            ,coalesce(t_ytd.reg_date,t_tdy.reg_date) reg_date
            ,coalesce(t_ytd.game_name,t_tdy.game_name) game_name
            ,coalesce(t_ytd.os,t_tdy.os) os
            ,coalesce(t_ytd.uid,t_tdy.uid) uid
        from
        (
            SELECT reg_date,game_name,os,uid
            FROM 
                `{project}.{dataset_dst}.{table_id_dst}`
            where
                dtstatdate = '{ytd}'
        )t_ytd
        full join 
        (
            SELECT 
                date(datetime(time,'Asia/Singapore')) reg_date
                ,'alice' game_name
                ,case
                when json_extract_scalar(data, '$.platform') ='android' then 'gp'
                when json_extract_scalar(data, '$.platform') ='ios' then 'ios'
                end os
                ,json_extract_scalar(data, '$.uid') uid
            FROM 
                `{project}.{dataset}.{table_id_src}`
            where 
                date(datetime(time,'Asia/Singapore')) = '{tdy}'
        )t_tdy
        on t_ytd.uid =t_tdy.uid and t_ytd.os =t_tdy.os
    )
    '''.format(**ARGS)
       
    bq.execute(sql)
