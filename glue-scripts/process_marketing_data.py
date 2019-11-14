
# Python Shell Job (python 3.6)
import sys
from awsglue.utils import getResolvedOptions

import awswrangler


args = getResolvedOptions(sys.argv, ['s3_output_path', 'database_name', 'table_name'])
s3_output_path = args['s3_output_path']
database_name = args['database_name']
table_name = args['table_name']


#Create AWS Wrangler session
session = awswrangler.Session()


#Execute Athena SQL query and create a Pandas DF with the result

dataframe = session.pandas.read_sql_athena(
    sql="select * from "+table_name,
    database=database_name
)

#Save in parquet format on S3 and Glue Data Catalog table

session.pandas.to_parquet(
    dataframe=dataframe,
    database=database_name,
    path=s3_output_path
#    partition_cols=["col_name"],
)

