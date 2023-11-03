from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
import pandas as pd
import re
from utils.fb_key import key

params_adsLast60_d = {
        # 'time_range': {'since':'{tdy}'.format(**ARGS),
        #                'until':'{tdy}'.format(**ARGS)},
        'level' : 'adset'   
}
fields_adsLast60_d=['adset_name']

class FB:
    
    def __init__(self,params,fields,run_dt):
        FacebookAdsApi.init(key['app-id'], key['appsecret'], key['access-token'])
        myact = AdAccount(key['adaccount-id'])
        self.i = myact.get_insights(params=params, fields=fields)
        self.dt = run_dt

    def toList(self):
        list =[]
        for i in range(0,len(self.i)):
            list.append((self.i[i]['campaign_name'],self.i[i]['attribution_setting']))
        return list

    def toDf(self,df,attribution_setting):
        for i in range(0,len(self.i)):
            print(self.i[i])
        # init dict to insert to dataframe
            insert = {
                    'imp_date':self.dt,
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
        # get os values
            os = re.search('^\w+', self.i[i]['campaign_name']).group(0).replace('-','').lower()
            insert['os'] = os

        #loop to get other values
            for k,v in insert.items():
                for k1,v1 in self.i[i].items():
                    if k==k1:
                        print(k1) 
                        insert[k] = v1

            for k1,v1 in self.i[i].items():
                if k1=='actions':
                    print(k1)
                    if attribution_setting == '1d_view_1d_click':
                        print('1d_view_1d_click_install_attribution')
                        try: 
                            insert['install'] = str(int(self.i[i]['actions'][0]['1d_click'])+ int(self.i[i]['actions'][0]['1d_view']))
                        except:
                            try:
                                insert['install'] = str(int(self.i[i]['actions'][0]['1d_click']))
                            except:
                                try:
                                    insert['install'] = str(int(self.i[i]['actions'][0]['1d_view']))
                                except Exception as e:
                                    print('not_captured')
                                    print(self.i[i])
                                    print(e)
                        # finally:
                        #     print('not_captured')
                        #     print(self.i[i])
                    elif attribution_setting == '1d_click':
                        print('1d_click_install_attribution')
                        try:
                            insert['install'] = str(int(self.i[i]['actions'][0]['1d_click']))
                        except:
                            print(f'***********{self.i[i]}')
                    elif attribution_setting == 'skan':
                        insert['install'] = str(int(self.i[i]['actions'][0]['skan_click']))
                    else:    
                        insert['install'] = None
                    
            print(insert)

            df_insert = pd.DataFrame.from_dict([insert],orient='columns')
            # print (f'insert_df:{df_insert}')
            df = pd.concat([df, df_insert], ignore_index=True)
        
        return df

if __name__ == '__main__':
    # FacebookAdsApi.init(key['app-id'], key['appsecret'], key['access-token'])
    # a = AdAccount(key['adaccount-id'])
    # print(a.get_insights(params = params_adsLast60_d,fields = fields_adsLast60_d))
    fb=FB(params_adsLast60_d,fields_adsLast60_d)
    print(fb.toList())