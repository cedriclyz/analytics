from utils.gcp_utils import BQ
from utils.project import PROJECT
from google.cloud import bigquery
from datetime import datetime, timedelta
import sys 

tdy_dt = datetime.strptime(sys.argv[1], '%Y%m%d')
tdy_str = datetime.strftime(tdy_dt, '%Y-%m-%d')

ARGS ={
    'tdy' : tdy_str
    ,'project': PROJECT[sys.argv[2]]
    ,'dataset' : 'ads_bi'
    ,'table_id_src': 'etl_fb_ads'
    ,'table_id_dst': 'dwd_ads_cost_install'
    ,'config' : sys.argv[2]
}    

schema = [
        bigquery.SchemaField("dtstatdate", "DATE")
        ,bigquery.SchemaField("campaign_name", "STRING")
        ,bigquery.SchemaField("game_name", "STRING")
        ,bigquery.SchemaField("os", "STRING")
        ,bigquery.SchemaField("age", "STRING")
        ,bigquery.SchemaField("gender", "STRING")
        ,bigquery.SchemaField("impressions", "NUMERIC")
        ,bigquery.SchemaField("reach", "NUMERIC")
        ,bigquery.SchemaField("clicks", "NUMERIC")
        ,bigquery.SchemaField("install", "NUMERIC")
        ,bigquery.SchemaField("spend", "NUMERIC")
]

if __name__ == '__main__':
    bq = BQ(ARGS['dataset'],ARGS['config'])
   
    if bq.tableIfNotExist(ARGS['table_id_dst']) == True:
       print ('table_Notexists')
       bq.tableCreate(schema,ARGS['table_id_dst'])

    sql = '''
        select 
            exists( 
            select * 
            from {project}.{dataset}.{table_id_dst}
            where dtstatdate = '{tdy}')
        '''.format(**ARGS)
    
    if bq.dataIfExist(sql) == True:
       print ('overwriting existing data')

       sql = '''
            delete
            from {project}.{dataset}.{table_id_dst}
            where dtstatdate = '{tdy}'
        '''.format(**ARGS)
       
       bq.execute(sql)
    
    sql = '''
        INSERT INTO {project}.{dataset}.{table_id_dst} (
            dtstatdate
            ,campaign_name
            ,game_name
            ,gender
            ,age
            ,os
            ,impressions
            ,reach
            ,clicks
            ,install
            ,spend
        )
        SELECT 
            date(imp_date) dtstatdate
            ,campaign_name
            ,case
                when lower(regexp_replace(regexp_extract(campaign_name, r'-\w+-',1),'-','')) ='glompa' then 'gloompa' 
                else lower(regexp_replace(regexp_extract(campaign_name, r'-\w+-',1),'-',''))
            end game_name
            ,gender
            ,age
            ,os
            ,cast(if(impressions = '','0',impressions) as numeric) impressions
            ,cast(if(reach = '','0',reach) as numeric) reach
            ,cast(if(clicks = '','0',clicks) as numeric) clicks
            ,cast(if(install = '','0',install) as numeric) install
            ,cast(if(spend = '','0',spend) as numeric) spend
        FROM 
            {project}.{dataset}.{table_id_src} 
        where imp_date = '{tdy}'
        '''.format(**ARGS)
       
    bq.execute(sql)
