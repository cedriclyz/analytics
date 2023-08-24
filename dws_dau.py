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
    ,'table_id_dst': 'dws_dau'
    ,'config' : sys.argv[1]
}    

schema = [
        bigquery.SchemaField("dtstatdate", "DATE")
        ,bigquery.SchemaField("game_name", "STRING")
        ,bigquery.SchemaField("installs", "NUMERIC")
        ,bigquery.SchemaField("spend", "NUMERIC")
        ,bigquery.SchemaField('wau','NUMERIC')
        ,bigquery.SchemaField("dau", "NUMERIC")
        ,bigquery.SchemaField('revenue','NUMERIC')
        ,bigquery.SchemaField('d1r','FLOAT64') 
        ,bigquery.SchemaField('d2r','FLOAT64')   
        ,bigquery.SchemaField('d3r','FLOAT64')   
        ,bigquery.SchemaField('d7r','FLOAT64')   
        ,bigquery.SchemaField('d14r','FLOAT64')
        ,bigquery.SchemaField('churn_rate','FLOAT64')  
        ,bigquery.SchemaField('acqusition_rate','FLOAT64')       
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
    declare tdy date default '{tdy}';
    declare ytd date default date_sub(tdy,INTERVAL 1 DAY);
    declare retention_last date default date_sub(tdy,INTERVAL 21 DAY);
    INSERT INTO {project}.{dataset_dst}.{table_id_dst}(
        dtstatdate
        ,installs
        ,spend
        ,wau
        ,dau
        ,revenue
        ,d1r
        ,d2r
        ,d3r
        ,d7r
        ,d14r
        ,churn_rate
        ,acqusition_rate
    )
        select 
        t.dtstatdate
        ,installs
        ,spend
        ,wau
        ,dau
        ,revenue
        ,round(d1r,2) d1r
        ,round(d2r,2) d2r
        ,round(d3r,2) d3r
        ,round(d7r,2) d7r
        ,round(d14r,2) d14r
        ,round(t5.churn_rate,2) churn_rate
        ,round(t5.acqusition_rate,2) acqusition_rate
    from
    (
    select 
        dtstatdate
        ,sum(if(instr(reverse(bitmap),'1') = 1,1,0)) dau
        ,sum(if(instr(reverse(bitmap),'1') <= 7,1,0)) wau
    from 
        {project}.{dataset_dst}.dwm_full_user_active_bitmap
    where 
        dtstatdate = tdy
    group by 
        dtstatdate
    )t
    left join
    (
        select 
            dtstatdate, sum(install) installs,sum(spend) spend
        from   
            {project}.{dataset_dst}.dwd_ads_cost_install
        where 
            game_name = 'magic_adventures'
        group by 
            dtstatdate
    )t2
    on t.dtstatdate = t2.dtstatdate
    left join
    (
        select dtstatdate, sum(purchase_amt_tdy) revenue
        from
        (
            select 
                *
                , if(pay_total -lag(pay_total) over(partition by uid order by dtstatdate asc) is null,pay_total/100,(pay_total -lag(pay_total) over(partition by uid order by dtstatdate asc))/100)purchase_amt_tdy
            from 
                {project}.{dataset_dst}.dwm_full_user_pay_bitmap
        )
        where dtstatdate = tdy
        group by dtstatdate
    )t3
    on t.dtstatdate = t3.dtstatdate
    left join
    (
        select 
            dtstatdate
            ,sum(if(substr(bitmap,2,1) = '1',1,0))/ sum(if(substr(bitmap,1,1) = '1',1,0)) d1r
            ,sum(if(substr(bitmap,3,1) = '1',1,0))/ sum(if(substr(bitmap,1,1) = '1',1,0)) d2r
            ,sum(if(substr(bitmap,4,1) = '1',1,0))/ sum(if(substr(bitmap,1,1) = '1',1,0)) d3r
            ,sum(if(substr(bitmap,8,1) = '1',1,0))/ sum(if(substr(bitmap,1,1) = '1',1,0)) d7r
            ,sum(if(substr(bitmap,15,1) = '1',1,0))/ sum(if(substr(bitmap,1,1) = '1',1,0)) d14r
        from 
            {project}.{dataset_dst}.dwm_full_user_active_bitmap
        where 
            dtstatdate =tdy
        and 
            reg_date between retention_last and tdy
        group by 
            dtstatdate
    )t4
    on t.dtstatdate = t4.dtstatdate
    left join 
    (
        select 
            dtstatdate
            ,sum(if(status = 'churn' and start_date=end_date,1,0)) / sum(if(status ='active',1,0)) churn_rate
            ,sum(if(status ='active' and start_date =end_date,1,0)) / sum(if(status ='active',1,0)) acqusition_rate
        from 
            magic-adventure-analytics.ads_bi.dwm_full_user_churn_d
        where 
            dtstatdate = tdy
        and 
            period_flag = 'curr'
        group by 
            dtstatdate
    )t5
    on t.dtstatdate = t5.dtstatdate
    '''.format(**ARGS)
       
    bq.execute(sql)
