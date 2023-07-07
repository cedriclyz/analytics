import pandas as pd
import re
import pandas_gbq
from utils.gcp_utils import BQ

df = pd.read_csv(r"C:\Users\lyzce\Downloads\Magic-Adventures-AD-Ads-29-May-20233-Jun-2023.csv",
                 usecols= ['Ad name','Age', 'Gender'
                           ,'Reporting starts', 'Reporting ends'
                           ,'Amount spent (USD)','Impressions','Reach'
                           ,'Mobile app installs'],
                           dtype = 'string')

map ={
        'Ad name' : 'campaign_name'
        ,'Age': 'age'
        ,'Gender': 'gender'
        ,'Reporting starts': 'date_start'
        ,'Reporting ends': 'date_stop'
        ,'Amount spent (USD)': 'spend'
        ,'Impressions': 'impressions'
        ,'Reach': 'reach'
        ,'': 'clicks'
        ,'Mobile app installs': 'install'
}

df = df.rename( columns= map)
df['imp_date'] = df['date_start']
df['clicks'] = ''
df['adset_name'] = df['campaign_name']

df['os'] = df['campaign_name'].map(lambda x: re.search('- \w+', x).group(0).replace('- ','').lower())

df =df[['imp_date','campaign_name','adset_name','os','age',
       'gender','date_start','date_stop','spend','impressions',
       'reach','clicks','install']]

pandas_gbq.to_gbq(df, 'ads_bi.etl_fb_ads'
              ,project_id ='magic-adventure-analytics'
              , if_exists = 'append')
print(df.columns)
