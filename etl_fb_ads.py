from datetime import datetime, timedelta
import re, sys
import pandas as pd
import pandas_gbq
from utils.fb_utils import FB
from utils.gcp_utils import BQ
from utils.project import PROJECT
from google.cloud import bigquery

tdy_dt = datetime.strptime(sys.argv[1], '%Y%m%d')
tdy_str = datetime.strftime(tdy_dt, '%Y-%m-%d')

ARGS ={
    'tdy' : tdy_str
    ,'project': PROJECT[sys.argv[2]]
    ,'dataset' : 'ads_bi'
    ,'table_id': 'etl_fb_ads'
    ,'config' : sys.argv[2]
}    

params_adsLast60_d = {
        'time_range': {'since':'2023-08-01'.format(**ARGS),
                   'until':'{tdy}'.format(**ARGS)},
        'level' : 'ad'
        ,'use_unified_attribution_setting' : True   
}

fields_adsLast60_d=['ad_name','attribution_setting']

params_tdy = {
    'time_range': {'since':'{tdy}'.format(**ARGS),
                   'until':'{tdy}'.format(**ARGS)},
    'level' : 'ad',
    'action_breakdowns':['action_type'],
    'filtering': [{'field':'action_type','operator':'CONTAIN','value':'mobile_app_install'},
                  #{'field':'spend','operator':'GREATER_THAN','value':0.00},
                  {'field':'ad.name','operator':'EQUAL','value':''}],
    'breakdowns':['gender','age'],
    'action_attribution_windows' : ['1d_click','skan_click','1d_view']
    ,'use_unified_attribution_setting' : True  
}
fields_tdy=['campaign_name','adset_name',
        'impressions','clicks','spend','reach','actions','attribution_setting']

schema = [
        bigquery.SchemaField("imp_date", "STRING")
        ,bigquery.SchemaField("campaign_name", "STRING")
        ,bigquery.SchemaField("adset_name", "STRING")
        ,bigquery.SchemaField("os", "STRING")
        ,bigquery.SchemaField("age", "STRING")
        ,bigquery.SchemaField("gender", "STRING")
        ,bigquery.SchemaField("date_start", "STRING")
        ,bigquery.SchemaField("date_stop", "STRING")
        ,bigquery.SchemaField("spend", "STRING")
        ,bigquery.SchemaField("impressions", "STRING")
        ,bigquery.SchemaField("reach", "STRING")
        ,bigquery.SchemaField("clicks", "STRING")
        ,bigquery.SchemaField("install", "STRING")
]

if __name__ == '__main__':      

    #get data from fb API
    get_fbInsight_last60d =FB(params_adsLast60_d,fields_adsLast60_d,tdy_str)
    ad_list = get_fbInsight_last60d.toList()

    #setting up empty df
    insert = {
            'imp_date':tdy_str,
            'campaign_name':'',
            'adset_name':'',
            'os':'',
            'age':'',
            'gender':'',
            'date_start':'',
            'date_stop':'',
            'spend': '',
            'impressions':'',
            'reach':'',
            'clicks':'',
            'install':'',
    }
    df = pd.DataFrame(columns = list(insert.keys()))   
    
    # calling and compiling by individual ads
    for ad, attribution_setting in ad_list:
        params_tdy['filtering'][1]['value'] = ad
        df = FB(params_tdy,fields_tdy,tdy_str).toDf(df,attribution_setting)
    
    df=df.drop_duplicates()
     
    bq = BQ(ARGS['dataset'],ARGS['config'])
   
    if bq.tableIfNotExist(ARGS['table_id']) == True:
       print ('table_Notexists')
       bq.tableCreate(schema,ARGS['table_id'])

    sql = '''
        select 
            exists( 
            select * 
            from {project}.{dataset}.{table_id}
            where imp_date = '{tdy}')
        '''.format(**ARGS)
    
    if bq.dataIfExist(sql) == True:
       print ('overwriting existing data')

       sql = '''
            delete
            from {project}.{dataset}.{table_id}
            where imp_date = '{tdy}'
        '''.format(**ARGS)
       
       bq.execute(sql)

    pandas_gbq.to_gbq(df, '{dataset}.{table_id}'.format(**ARGS)
              ,project_id ='{project}'.format(**ARGS)
              , if_exists = 'append')

  
