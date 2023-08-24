from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import os, sys

schema = [
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("post_abbr", "STRING"),
        bigquery.SchemaField("date", "DATE")]

class BQ:

    def __init__(self,dataset,file):
        path = os.path.join(os.getcwd(),'utils',file +'.json')
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path
        self.c = bigquery.Client()
        self.p = self.c.project
        self.dataset_ref = bigquery.DatasetReference(self.p, dataset)  

    def tableIfNotExist(self,table_id):
        table_ref = bigquery.TableReference(self.dataset_ref, table_id)
        try:
            self.c.get_table(table_ref)
            return False
        except NotFound:
            return True
        
    def tableCreate(self,schema,table_id,partition_field):
        table_ref = self.dataset_ref.table(table_id)
        table = bigquery.Table(table_ref, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            # name of column to use for partitioning
            field=partition_field) 
        table = self.c.create_table(table)
        print("Created table {}".format(table.table_id))
        return True
    
    def dataIfExist(self,sql):
        # print (f'{self.dataset_ref}.{self.table_id}')
        sql = sql
        job = self.c.query(sql)
        rows = job.result()
        results = list(rows)[0][0]
        if results == True: return True

    def execute(self,sql):
        sql = sql
        job = self.c.query(sql)
        rows = job.result()
        print(list(rows))
        return job

        
if __name__ == '__main__':

   bq = BQ(f'{sys.argv[1]}',f'{sys.argv[2]}','2023-06-17','ods_etl_campaign_costs')
   
   if bq.tableIfNotExist() == True:
       print ('table_Notexists')
       bq.tableCreate(schema)

   if bq.dataIfExist() == True:
       print ('overwriting existing data')
       bq.dataDelete()

   bq.tableInsert()
