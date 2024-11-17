"""
# for overwritting the dest table
python load_data.py O

# for append 
python load_data.py A

# for Merge data 
python load_data.py M

"""

import boto3
import json
from pyspark.sql import SparkSession
import sys

oracle_table="RETAIL.SALES"
oracle_pk="TRX_ID"
redshift_stg_table="store.daily_sales_stg"
redshift_table="store.daily_sales"
redshift_pk="trx_id"

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


def read_from_oracle(spark, oracle_url, oracle_properties):
    # Read data from Oracle Database using JDBC
    return spark.read.jdbc(url=oracle_url, table=oracle_table, properties=oracle_properties)


def write_to_redshift(df, redshift_url, redshift_properties, loadaction):
    # Write DataFrame to Redshift using JDBC
    if loadaction='O':   # O   For overwrite destination
        df.write.jdbc(url=redshift_url, table=redshift_table, mode="overwrite", properties=redshift_properties)
    elif loadaction='A': # A   Append with destination
        df.write.jdbc(url=redshift_url, table=redshift_table, mode="append", properties=redshift_properties)
    elif loadaction='M': # M   Merge with destination
        truncate_query = "TRUNCATE TABLE "+redshift_stg_table # Truncate stage table
        spark.read.jdbc(url=redshift_url, query=truncate_query, properties=redshift_properties).load() # Truncate stg table
        df.write.jdbc(url=redshift_url, table=redshift_stg_table, mode="overwrite", properties=redshift_properties) # load data into stage table
        delete_query="delete from "+redshift_table+" Where "+redshift_pk+" Not in (Select "+redshift_pk+" from "+redshift_stg_table+")" 
        spark.read.jdbc(url=redshift_url, query=delete_query, properties=redshift_properties).load() # delete records from dest table that do not exist in stg table
        
        merge_query="Merge into "+redshift_table+" using "+redshift_stg_table+" on ( "+redshift_table+"."+redshift_pk+"="+redshift_stg_table+"."+redshift_pk+")" 
        #Not using backslash here because I am not sure if that will work with indentation (unfortunately dont have enough time to configure entire dev environment)
        merge_query+=" WHEN MATCHED THEN update SET col1 = "+redshift_stg_table+".col1, col2 = "+redshift_stg_table+".col2, col3 = "+redshift_stg_table+".col3"
        merge_query+=" WHEN NOT MATCHED THEN INSERT("+redshift_pk+",col1,col2,col3) values ("+redshift_stg_table+".col1,"+redshift_stg_table+".col2, "+redshift_stg_table+".col3)"
        spark.read.jdbc(url=redshift_url, query=merge_query, properties=redshift_properties).load() # merge records in dest table 
    else:
        print("no valid load action defined")


def validate_row_count(source_count, target_count):
    # Validate the row counts between source and target
    if source_count == target_count:
        print(f"Row count validation passed: Source = {source_count}, Target = {target_count}")
    else:
        print(f"Row count validation failed: Source = {source_count}, Target = {target_count}")
        raise ValueError("Row count mismatch between source and target!")


def main():
    loadaction=sys.argv[0] 
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

    # Step 1: Read data from Oracle
    oracle_df = read_from_oracle(spark, oracle_url, oracle_properties)
    
    # Step 2: Load to Redshift
    write_to_redshift(oracle_df, redshift_url, redshift_properties,loadaction)
    
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
