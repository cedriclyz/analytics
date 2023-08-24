from utils.gcp_utils import BQ
from utils.project import PROJECT
from google.cloud import bigquery
from datetime import datetime, timedelta
import sys 

tdy_dt = datetime.strptime(sys.argv[2], '%Y%m%d')
tdy_str = datetime.strftime(tdy_dt, '%Y-%m-%d')
ytd_str = datetime.strftime(tdy_dt+timedelta(days=-1), '%Y-%m-%d')
ARGS ={
    'tdy' : tdy_str
    ,'ytd' : ytd_str
    ,'project': PROJECT[sys.argv[1]]
    ,'table_id_src': 'dwm_full_user_active_bitmap'
    ,'dataset_dst' : 'ads_bi'
    ,'table_id_dst': 'dwm_full_user_churn_d'
    ,'config' : sys.argv[1]
}    

schema = [
        bigquery.SchemaField("dtstatdate", "DATE")
        ,bigquery.SchemaField("game_name", "STRING")
        ,bigquery.SchemaField("os", "STRING")
        ,bigquery.SchemaField("uid", "STRING")
        ,bigquery.SchemaField('status','STRING')
        ,bigquery.SchemaField("start_date", "DATE")
        ,bigquery.SchemaField('end_date','DATE')
        ,bigquery.SchemaField('period_flag','STRING')   
]

if __name__ == '__main__':
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
    INSERT INTO {project}.{dataset_dst}.{table_id_dst}(
        dtstatdate
        ,game_name
        ,os
        ,uid
        ,status
        ,start_date
        ,end_date
        ,period_flag
    )
    select 
        dtstatdate
        ,game_name
        ,os
        ,uid,
        status_update status
        ,case 
            when if(status is null,'',status) <> status_update then date('{tdy}')
            else start_date 
        end start_date_update
        , date('{tdy}') end_date
        ,'curr' period_flag
    from
    (
        select 
            date('{tdy}') dtstatdate 
            ,'alice' as game_name
            ,coalesce(t_ytd.os,t_tdy.os) os
            ,coalesce(t_ytd.uid,t_tdy.uid) uid
            ,t_ytd.status
            ,case 
                when instr(reverse(bitmap),'1') >=3 then 'churn'
                else 'active' 
            end status_update
            ,start_date
            ,end_date
            ,bitmap
        from
        (
            select 
                dtstatdate
                ,game_name
                ,os
                ,uid
                ,status
                ,start_date
                ,end_date
                ,period_flag
            from 
                {project}.{dataset_dst}.{table_id_dst}
            where
                dtstatdate = '{ytd}' and period_flag ='curr'
        )t_ytd
        full outer join
        (
            select *
            from 
                {project}.{dataset_dst}.{table_id_src}
            where 
                dtstatdate = '{tdy}'
        )t_tdy
        on t_ytd.uid =t_tdy.uid
    )

    union all

    select 
        dtstatdate
        ,game_name
        ,os
        ,uid
        ,status
        ,start_date
        ,end_date
        ,'prev' period_flag
    from
    (
        select
            date('{tdy}') dtstatdate 
            ,'alice' as game_name
            ,coalesce(t_ytd.os,t_tdy.os) os
            ,coalesce(t_ytd.uid,t_tdy.uid) uid
            ,t_ytd.status
            ,case when instr(reverse(bitmap),'1') >=3 then 'churn'
            else 'active' end status_update
            ,start_date
            ,end_date
            ,bitmap
        from
        (
            select 
                dtstatdate
                ,game_name
                ,os
                ,uid
                ,status
                ,start_date
                ,end_date
            from 
                magic-adventure-analytics.ads_bi.dwm_full_user_churn_d
            where
                dtstatdate = '{ytd}'
        )t_ytd
        full outer join
        (
            select *
            from 
                magic-adventure-analytics.ads_bi.dwm_full_user_active_bitmap
            where 
                dtstatdate = '{tdy}'
        )t_tdy
        on t_ytd.uid =t_tdy.uid
    )
    where status <> status_update
    '''.format(**ARGS)
       
    bq.execute(sql)
