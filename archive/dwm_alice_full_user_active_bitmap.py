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
    ,'dataset_src' : 'prod'
    ,'dataset_dst' : 'ads_bi'
    ,'table_id_reg_src': 'dwd_full_user_reg_d'
    ,'table_id_login_src': 'analytics_server_login'
    ,'table_id_dst': 'dwm_full_user_active_bitmap'
    ,'config' : sys.argv[2]
}    

schema = [
        bigquery.SchemaField("dtstatdate", "DATE")
        ,bigquery.SchemaField("reg_date", "DATE")
        ,bigquery.SchemaField("game_name", "STRING")
        ,bigquery.SchemaField("os", "STRING")
        ,bigquery.SchemaField("uid", "STRING")
        ,bigquery.SchemaField("bitmap", "STRING")
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
        ,bitmap
    ) 
    select *
    from
    ( 
        select
            date('{tdy}') dtstatdate
            ,reg_date
            ,game_name
            ,os
            ,uid
            ,concat(bitmap,if(tt.uid_login is null ,0,1)) test
        from
        (
            select * 
            from 
                `{dataset_dst}.{table_id_dst}`
            where 
                dtstatdate = '{ytd}'
        )t
        left join
        (
            select 
                json_extract_scalar(data, '$.uid')uid_login 
            from 
                {dataset_src}.{table_id_login_src}
            where 
                date(datetime(time,'Asia/Singapore'))= '{tdy}'
            group by 
                json_extract_scalar(data, '$.uid')
        )tt
        on t.uid = tt.uid_login

        union all

        select *, '1'as bitmap
        from 
            `{dataset_dst}.{table_id_reg_src}`
        where 
            dtstatdate = '{tdy}' and reg_date = '{tdy}'
    )
    '''.format(**ARGS)
       
    bq.execute(sql)
