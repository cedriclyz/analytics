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
    ,'table_id_src': 'analytics_server_shop_iap'
    ,'table_id_reg_src': 'dwd_full_user_reg_d'
    ,'dataset_dst' : 'ads_bi'
    ,'table_id_dst': 'dwm_full_user_pay_bitmap'
    ,'config' : sys.argv[2]
}    

schema = [
        bigquery.SchemaField("dtstatdate", "DATE")
        ,bigquery.SchemaField("game_name", "STRING")
        ,bigquery.SchemaField("os", "STRING")
        ,bigquery.SchemaField("uid", "STRING")
        ,bigquery.SchemaField('is_pay','STRING')
        ,bigquery.SchemaField("start_date", "DATE")
        ,bigquery.SchemaField('frequency','NUMERIC')
        ,bigquery.SchemaField('pay_total','NUMERIC') 
        ,bigquery.SchemaField('bitmap','STRING')     
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
            ,game_name
            ,os
            ,uid
            ,is_pay
            ,start_date
            ,frequency
            ,pay_total
            ,bitmap
        ) 
        select 
            date('{tdy}') dtstatdate 
            ,game_name
            ,t_ytd.os
            ,t_ytd.uid
            ,coalesce(t_ytd.is_pay,t_tdy.is_pay) is_pay
            ,coalesce(t_ytd.start_date,t_tdy.start_date) start_date
            ,t_ytd.frequency + if(t_tdy.frequency is null,0,t_tdy.frequency) frequency
            ,t_ytd.pay_total + if(t_tdy.pay_total is null,0,t_tdy.pay_total) pay_total
            ,concat(bitmap, if(t_tdy.uid is null,0,1)) bitmap
        from
        (
            SELECT 
                dtstatdate
                ,game_name
                ,os
                ,uid
                ,is_pay
                ,start_date
                ,frequency
                ,pay_total
                ,bitmap
            FROM 
                `{project}.{dataset_dst}.{table_id_dst}`
            where
                dtstatdate = '{ytd}'
        )t_ytd
        left join
        (
            SELECT
                date(datetime(time,'Asia/Singapore')) start_date
                ,json_extract_scalar(data, '$.uid') uid
                ,json_extract_scalar(data, '$.isPaid') is_pay
                ,count(*) frequency
                ,sum(cast(json_extract_scalar(data, '$.price')as numeric)) pay_total
            FROM 
                `{project}.{dataset}.{table_id_src}`
            where 
                date(datetime(time,'Asia/Singapore')) = '{tdy}'
            group by
                date(datetime(time,'Asia/Singapore')) 
                ,json_extract_scalar(data, '$.uid')
                ,json_extract_scalar(data, '$.isPaid')

        )t_tdy
        on t_ytd.uid =t_tdy.uid

        union all 

        select 
            date('{tdy}') dtstatdate 
            ,game_name
            ,tnew.os
            ,tnew.uid
            ,tnew_tdy.is_pay is_pay
            ,tnew_tdy.start_date start_date
            ,tnew_tdy.frequency frequency
            ,tnew_tdy.pay_total pay_total
            , '1' as bitmap
        from
        (   select t.*
            from
            (
                select game_name,os,uid 
                from
                    `{project}.{dataset_dst}.{table_id_reg_src}`
                where
                    dtstatdate = '{tdy}'
            )t
            left join
            (
                SELECT uid
                FROM 
                    `{project}.{dataset_dst}.{table_id_dst}`
                where
                    dtstatdate = '{ytd}'
            )tt
            on t.uid =tt.uid
            where tt.uid is null 
        )tnew
        inner join
        (
            SELECT
                date(datetime(time,'Asia/Singapore')) start_date
                ,json_extract_scalar(data, '$.uid') uid
                ,json_extract_scalar(data, '$.isPaid') is_pay
                ,count(*) frequency
                ,sum(cast(json_extract_scalar(data, '$.price')as numeric)) pay_total
            FROM 
                `{project}.{dataset}.{table_id_src}`
            where 
                date(datetime(time,'Asia/Singapore')) = '{tdy}'
            group by 
                date(datetime(time,'Asia/Singapore'))
                ,json_extract_scalar(data, '$.uid')
                ,json_extract_scalar(data, '$.isPaid')
        )tnew_tdy
        on tnew.uid =tnew_tdy.uid
    '''.format(**ARGS)
       
    bq.execute(sql)
