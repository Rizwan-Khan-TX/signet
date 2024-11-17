import cx_Oracle
import pandas as pd
import psycopg2
import boto3
from datetime import datetime

# oracle config
oracle_table="RETAIL.SALES"
oracle_pk="TRX_ID"
oracledb="TESTDB"

# redshift config
redshift_stg_table="store.daily_sales_stg"
redshift_table="store.daily_sales"
redshift_pk="trx_id"
redshift_db="testdb"

# S3 Bucket config
s3_bucket_name = 'oracle_to_redshift'
s3_key_prefix = 'cdc/sales/'
s3_client = boto3.client('s3')

def get_secret(secret_name):
    # Retrieve a secret from AWS Secrets Manager
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager")  
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except Exception as e:
        print(f"Error retrieving secret: {e}")
        return None
    
    # Parse the secret value
    if "SecretString" in get_secret_value_response:
        return json.loads(get_secret_value_response["SecretString"])
    else:
        return None

def last_redshift_date(redshift_url, redshift_properties):
    redshift_conn = psycopg2.connect(
    host=redshift_host,
    port=redshift_port,
    user=redshift_user,
    password=redshift_password,
    dbname=redshift_dbname
)
    # Write DataFrame to Redshift using JDBC
    get_last_date = "Select max(last_update) from "+redshift_table
    redshift_cursor = redshift_conn.cursor()
    # I am not sure about following syntax, I hope I can use query through df.read.jdbc as follows
    df=spark.read.jdbc(url=redshift_url, query=get_last_date, properties=redshift_properties).load()
    return df.iloc[0, 0]
    

def get_deleted_records_from_oracle(spark, oracle_url, oracle_properties, last_date):
    dsn = cx_Oracle.makedsn(oracle_url)
    connection = cx_Oracle.connect(oracle_properties["user"], oracle_properties["password"], dsn)
    oracle_query = "SELECT * FROM "+oracle_table+"_deleted  WHERE last_updated > :last_date "
    # or following query, really depends on how oracle CDC is configured
    #oracle_query = "SELECT * FROM "+oracle_table+"_deleted  WHERE DELETED=1 and last_updated > :last_date "
    oracle_cursor = connection.cursor()
    oracle_cursor.execute(oracle_query, last_date=last_date)
    
    # Fetch CDC data
    cdc_data = cursor.fetchall()
    
    # Optionally, fetch column names
    columns = [desc[0] for desc in cursor.description]
    
    # Process and transform the data if necessary
    cdc_data_dict = [dict(zip(columns, row)) for row in cdc_data]
    
    # Close the connection
    cursor.close()
    connection.close()
    
    # Convert to DataFrame for easier manipulation and return
    return pd.DataFrame(cdc_data_dict)
    
def read_from_oracle(spark, oracle_url, oracle_properties, last_date):
    dsn = cx_Oracle.makedsn(oracle_url)
    connection = cx_Oracle.connect(oracle_properties["user"], oracle_properties["password"], dsn)
    oracle_query = "SELECT * FROM "+oracle_table+"  WHERE last_updated > :last_date "
    oracle_cursor = connection.cursor()
    oracle_cursor.execute(oracle_query, last_date=last_date)
    
    # Fetch CDC data
    cdc_data = cursor.fetchall()
    
    # Optionally, fetch column names
    columns = [desc[0] for desc in cursor.description]
    
    # Process and transform the data if necessary
    cdc_data_dict = [dict(zip(columns, row)) for row in cdc_data]
    
    # Close the connection
    cursor.close()
    connection.close()
    
    # Convert to DataFrame for easier manipulation and return
    return pd.DataFrame(cdc_data_dict)
    
def write_to_redshift(df, deleted_df, redshift_url, redshift_properties):
    # still have to take care of deletes
    truncate_query = "TRUNCATE TABLE "+redshift_stg_table # Truncate stage table
    spark.read.jdbc(url=redshift_url, query=truncate_query, properties=redshift_properties).load() # Truncate stg table
    deleted_df.write.jdbc(url=redshift_url, table=redshift_stg_table, mode="overwrite", properties=redshift_properties) # load data into stage table
    delete_query="delete from "+redshift_table+" Where "+redshift_pk+" in (Select "+redshift_pk+" from "+redshift_stg_table+")" 
    spark.read.jdbc(url=redshift_url, query=delete_query, properties=redshift_properties).load() # delete records from dest table that do not exist in stg table
    
    # no reuse this stg table for new (inserted/updated) data
    truncate_query = "TRUNCATE TABLE "+redshift_stg_table # Truncate stage table
    spark.read.jdbc(url=redshift_url, query=truncate_query, properties=redshift_properties).load() # Truncate stg table
    df.write.jdbc(url=redshift_url, table=redshift_stg_table, mode="overwrite", properties=redshift_properties) # load data into stage table
        
    #upsert/merge new and updated data 
    merge_query="Merge into "+redshift_table+" using "+redshift_stg_table+" on ( "+redshift_table+"."+redshift_pk+"="+redshift_stg_table+"."+redshift_pk+")" 
    merge_query+=" WHEN MATCHED THEN update SET col1 = "+redshift_stg_table+".col1, col2 = "+redshift_stg_table+".col2, col3 = "+redshift_stg_table+".col3"
    merge_query+=" WHEN NOT MATCHED THEN INSERT("+redshift_pk+",col1,col2,col3) values ("+redshift_stg_table+".col1,"+redshift_stg_table+".col2, "+redshift_stg_table+".col3)"
    df.read.jdbc(url=redshift_url, query=merge_query, properties=redshift_properties).load() # merge records in dest table 

def validate_row_count(source_count, target_count):
    # Validate the row counts between source and target
    if source_count == target_count:
        print(f"Row count validation passed: Source = {source_count}, Target = {target_count}")
    else:
        print(f"Row count validation failed: Source = {source_count}, Target = {target_count}")
        raise ValueError("Row count mismatch between source and target!")


def main():
    # Initialize Spark session
    spark = SparkSession.builder.appName("OracleToRedshift").getOrCreate()

    # Retrieve credentials from AWS Secrets Manager
    oracle_secret = get_secret("oracle-conn-info")  
    redshift_secret = get_secret("redshift-conn-info")  

    if oracle_secret is None or redshift_secret is None:
        print("Error: Unable to retrieve credentials from Secrets Manager")
        return

    # Extract credentials
    oracle_url = f"jdbc:oracle:thin:@{oracle_secret['host']}:{oracle_secret['port']}:{oracle_secret['db']}"
    oracle_properties = {
        "user": oracle_secret["user"],
        "password": oracle_secret["password"]
    }

    redshift_url = f"jdbc:redshift://{redshift_secret['host']}:{redshift_secret['port']}/{redshift_secret['db']}"
    redshift_properties = {
        "user": redshift_secret["username"],
        "password": redshift_secret["password"]
    }
    
   
    # Step 1: Get last_date from Redshift 
    last_date=last_redshift_date(redshift_url, redshift_properties):
    
    # Step 2a: Read New data from Oracle
    oracle_df = read_from_oracle(spark, oracle_url, oracle_properties,last_date)
    
    # Step 2b: Read deleted data from Oracle
    oracle_deleted_df = get_deleted_records_from_oracle(spark, oracle_url, oracle_properties,last_date)
    
    # Step 3: Load to Redshift
    write_to_redshift(oracle_df, oracle_deleted_df, redshift_url, redshift_properties,loadaction)
    
    # Step 3: Validate row counts
    source_count = oracle_df.count()

    # Validate row count in Redshift (this assumes the data is already loaded)
    redshift_df = spark.read.jdbc(url=redshift_url, table=redshift_table, properties=redshift_properties)
    target_count = redshift_df.count()

    # Validate counts
    validate_row_count(source_count, target_count)

    # Stop the Spark session
    spark.stop()


if __name__ == "__main__":
    main()
