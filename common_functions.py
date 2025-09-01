import json, os, inspect, pendulum as pdl, re
# from sqlalchemy.orm import Session , sqlalchemy as db
from pandas import DataFrame, to_numeric
from google.api_core.exceptions import BadRequest
from google.cloud import secretmanager, bigquery, firestore
from google.auth import compute_engine
from google.oauth2 import service_account
# from google.cloud.sql.connector import Connector, IPTypes
# import pg8000

import os
from google.auth import default
from google.auth.credentials import Credentials
from google.oauth2 import service_account
from google.auth.compute_engine import Credentials as compute_engine_Credentials

def get_service_account_credentials() -> Credentials:
    # Nimm zuerst GCP_PROJECT oder GOOGLE_CLOUD_PROJECT
    project_id = os.environ.get("GCP_PROJECT") or os.environ.get("GOOGLE_CLOUD_PROJECT")

    if project_id in ["riseupbi", "snocks-analytics"]:
        try:
            # In Functions/Run → ADC
            creds, _ = default()
            return creds
        except Exception:
            # Fallback für VMs (alte Logik eurer Data-Leute)
            return compute_engine_Credentials()
    else:
        file_path = os.environ.get(
            "GCP_SERVICE_ACCOUNT_FILEPATH",
            os.path.expanduser("~/documents/creds/keyfile.json"),
        )
        return service_account.Credentials.from_service_account_file(file_path)

def get_credentials_from_secret_manager(secret_name, secret_version:str = 'latest', project:str = 'snocks-analytics') -> str:
    client = secretmanager.SecretManagerServiceClient(credentials=get_service_account_credentials())
    t = client.access_secret_version(request={'name':
        f'projects/{project}/secrets/{secret_name}/versions/{secret_version}'}).payload.data.decode("UTF-8")
    return(t)

def get_bq_client() -> bigquery.Client:
    return(bigquery.Client(credentials=get_service_account_credentials(),project='snocks-analytics'))

def get_meta_key_value_firestore_improved(table_id:str, filter_value:str, meta_key_name:str):
    db = firestore.Client(credentials=get_service_account_credentials()
                          ,database='snocks-apart-data-engineering')
    
    meta_collection = db.collection('api_meta_data_improved')

    query_result = meta_collection.where(
                        filter=firestore.FieldFilter("table_id", "==", table_id)).where(
                            filter=firestore.FieldFilter("filter_value", "==", filter_value)).where(
                                filter=firestore.FieldFilter("meta_key_name", "==", meta_key_name)).get()
    
    if len(query_result) != 1:
        raise Exception('more or less than one matching document found in firestore')
    
    for doc in query_result:
        value_ = doc.to_dict()['value_']
        return (value_)

def set_meta_key_value_firestore_improved(table_id:str, filter_value:str, meta_key_name:str, meta_key_value:str):
    function_that_updated = str(inspect.getframeinfo(inspect.currentframe().f_back)[2])
    db = firestore.Client(credentials=get_service_account_credentials()
                          ,database='snocks-apart-data-engineering')
    
    meta_collection = db.collection('api_meta_data_improved')

    query_result = meta_collection.where(
                        filter=firestore.FieldFilter("table_id", "==", table_id)).where(
                            filter=firestore.FieldFilter("filter_value", "==", filter_value)).where(
                                filter=firestore.FieldFilter("meta_key_name", "==", meta_key_name)).get()
    
    if len(query_result) > 1:
        raise Exception('more than one matching document found in firestore')
    elif len(query_result) == 0:
        data = {"table_id": table_id, "filter_value": filter_value, "meta_key_name": meta_key_name
                ,"value_": meta_key_value, "record_uploaded_at_utc": pdl.now('UTC')}
        meta_collection.add(data)
        return(f'{function_that_updated} created {meta_key_name} with {meta_key_value} for table {table_id} with filter value {filter_value}.')
    elif len(query_result) == 1:
        for doc in query_result:
            doc_id = doc.id
        print(f'doc_id to update is {doc_id}') 
        meta_collection.document(doc_id).update({"value_": meta_key_value, 'record_uploaded_at_utc': pdl.now('UTC')})
        return(f'{function_that_updated} updated {meta_key_name} with {meta_key_value} for table {table_id}.')
    
    # logically all possible cases covered above, so raising an exception if not
    raise Exception('something unexpected happened with firestore updata function')

def get_meta_key_value_firestore(table_id:str, filter_value:str, meta_key_name:str):
    db = firestore.Client(credentials=get_service_account_credentials()
                          ,database='snocks-apart-data-engineering')

    doc_dict = (db.collection('api_meta_data')
                  .document(table_id).collection(filter_value)
                  .document(meta_key_name).get().to_dict())
    
    meta_key_value = doc_dict['value_']

    return(meta_key_value)

def set_meta_key_value_firestore(table_id:str, filter_value:str, meta_key_name:str, meta_key_value:str):
    function_that_updated = str(inspect.getframeinfo(inspect.currentframe().f_back)[2])
    db = firestore.Client(credentials=get_service_account_credentials()
                          ,database='snocks-apart-data-engineering')

    doc = (db.collection('api_meta_data')
                  .document(table_id).collection(filter_value)
                  .document(meta_key_name))

    doc.update({"value_": meta_key_value, 'record_uploaded_at_utc': pdl.now('UTC')})
    
    return(f'{function_that_updated} updated {meta_key_name} with {meta_key_value} for table {table_id}.')

# def connect_unix_socket() -> db.engine.base.Engine: # from googles official documentation https://cloud.google.com/sql/docs/postgres/connect-functions
#     """Initializes a Unix socket connection pool for a Cloud SQL instance of Postgres."""
#     # Note: Saving credentials in environment variables is convenient, but not
#     # secure - consider a more secure solution such as
#     # Cloud Secret Manager (https://cloud.google.com/secret-manager) to help
#     # keep secrets safe. crds['user'],password=crds['pw']
#     crds = json.loads(get_credentials_from_secret_manager('airflow-variables-postgres_creds'))
#     db_user = crds['user']  # e.g. 'my-database-user'
#     db_pass = crds['pw']  # e.g. 'my-database-password'
#     db_name = 'db_data_engineering'
#     unix_socket_path = '/cloudsql/snocks-analytics:europe-west3:snocks-apart-data-engineering'  # e.g. '/cloudsql/project:region:instance'

#     pool = db.create_engine(
#         # Equivalent URL:
#         # postgresql+pg8000://<db_user>:<db_pass>@/<db_name>
#         #                         ?unix_sock=<INSTANCE_UNIX_SOCKET>/.s.PGSQL.5432
#         # Note: Some drivers require the `unix_sock` query parameter to use a different key.
#         # For example, 'psycopg2' uses the path set to `host` in order to connect successfully.
#         db.engine.url.URL.create(
#             drivername="postgresql+pg8000",
#             username=db_user,
#             password=db_pass,
#             database=db_name,
#             query={"unix_sock": f"{unix_socket_path}/.s.PGSQL.5432"},
#         ),
#         # ...
#     )
#     return pool

# def connect_with_connector() -> db.engine.base.Engine:
#     """
#     Initializes a connection pool for a Cloud SQL instance of Postgres.

#     Uses the Cloud SQL Python Connector package.
#     """
#     # Note: Saving credentials in environment variables is convenient, but not
#     # secure - consider a more secure solution such as
#     # Cloud Secret Manager (https://cloud.google.com/secret-manager) to help
#     # keep secrets safe.

#     crds = json.loads(get_credentials_from_secret_manager('airflow-variables-postgres_creds'))

#     instance_connection_name = 'snocks-analytics:europe-west3:snocks-apart-data-engineering'  # e.g. 'project:region:instance'
#     db_user = crds['user']  # e.g. 'my-db-user'
#     db_pass = crds['pw']  # e.g. 'my-db-password'
#     db_name = 'db_data_engineering'  # e.g. 'my-database'

#     ip_type = IPTypes.PUBLIC

#     # initialize Cloud SQL Python Connector object
#     connector = Connector(refresh_strategy="LAZY",credentials=get_service_account_credentials())

#     def getconn() -> pg8000.dbapi.Connection:
#         conn: pg8000.dbapi.Connection = connector.connect(
#             instance_connection_name,
#             "pg8000",
#             user=db_user,
#             password=db_pass,
#             db=db_name,
#             ip_type=ip_type,
#         )
#         return conn

#     # The Cloud SQL Python Connector can be used with SQLAlchemy
#     # using the 'creator' argument to 'create_engine'
#     pool = db.create_engine(
#         "postgresql+pg8000://",
#         creator=getconn,
#         # ...
#     )
#     return pool


# # review https://cloud.google.com/sql/docs/postgres/connect-functions
# def connect_to_pg():
#     # 'GCP_PROJECT': 'riseupbi' runtime env variable set automatically in python 3.7... set manually in google cloud, when creating the function for other python versions
#     crds = json.loads(get_credentials_from_secret_manager('airflow-variables-postgres_creds'))
#     unix_socket_path = '/cloudsql/snocks-analytics:europe-west3:snocks-apart-data-engineering' #  '/cloudsql/project:region:instance'
#     if os.environ.get('GCP_PROJECT') in ['riseupbi','snocks-analytics']:
#         db_url = db.engine.url.URL.create(drivername="postgresql+pg8000",username=crds['user'],password=crds['pw']
#             ,database='db_data_engineering'
#             ,query={"unix_sock": f"{unix_socket_path}/.s.PGSQL.5432"}) # https://cloud.google.com/sql/docs/postgres/connect-functions#connect_to
#             # ,query={"host": "/cloudsql/riseupbi:europe-west3:oa-data-engineering"})
#     # if os.environ.get('GCP_PROJECT') in ['snocks-analytics']:
#     #     engine = connect_with_connector()
#     elif os.environ.get('HOMEPATH',os.environ.get('HOME')) is not None:
# #        crds = json.loads(open('C:' + os.environ.get('HOMEPATH') + '\Documents\Google and API Keys\postgres_creds.json', "r").read())
#         db_url = db.engine.url.URL.create(drivername="postgresql+pg8000"
#             ,host='34.159.159.34',port=5432,database='db_data_engineering'
#             ,username=crds['user'],password=crds['pw'])

#     engine = db.create_engine(url=db_url, poolclass=db.pool.NullPool) # https://stackoverflow.com/a/8705750 https://stackoverflow.com/a/74497688 , future=True
#     return(engine)

# def get_meta_key_value_new(table_id:str, filter_value:str, meta_key_name:str): 
#     sql_code = ("select value_ from db_data_engineering.bq_dw_operations.api_meta_data"
#                 + f" where data_table = '{table_id}' and filter_value = '{filter_value}' and key_ = '{meta_key_name}';") # print(f'sql code is {sql_code}')
        
#     pg_engine = connect_to_pg()
#     # session = sessionmaker(pg_engine)

#     # with pg_engine.begin() as connection:
#     with Session(pg_engine) as session:
#         resultset = session.execute(db.text(sql_code)).fetchone()
    
#     pg_engine.dispose()

#     meta_key_value = resultset[0]

#     return(meta_key_value)

# def set_meta_key_value_new(table_id:str, filter_value:str, meta_key_name:str, meta_key_value:str):
#     function_that_updated = str(inspect.getframeinfo(inspect.currentframe().f_back)[2])
#     # meta_tbl_id = 'db_data_engineering.bq_dw_operations.api_meta_data_legacy' # postgres table
    
#     # after successfull upload save meta data for future api calls
#     ## meta key value as string to not break anything
#     ## ATTENTION: value is set as string, if other types are needed correct it in the function
#     sql_code = (f"update db_data_engineering.bq_dw_operations.api_meta_data"
#                 + f" set value_ = '{meta_key_value}', record_uploaded_at_utc = (now() at time zone 'utc')"
#                 + f" where data_table = '{table_id}' and filter_value = '{filter_value}' and key_ = '{meta_key_name}';") # print(sql_code)

#     pg_engine = connect_to_pg()

#     with pg_engine.begin() as connection:
#         result = connection.execute(db.text(sql_code))
#         # connection.commit()
    
#     pg_engine.dispose()

#     return(f'{function_that_updated} updated {meta_key_name} with {meta_key_value} for table {table_id}.')

# def get_meta_key_value(table_id:str, webshop:str, meta_key_name:str): 
#     sql_code = ("select value_ from db_data_engineering.bq_dw_operations.api_meta_data_legacy"
#                 + f" where data_table = '{table_id}' and webshop = '{webshop}' and key_ = '{meta_key_name}';") # print(f'sql code is {sql_code}')
        
#     pg_engine = connect_to_pg()

#     with pg_engine.begin() as connection:
#         resultset = connection.execute(db.text(sql_code)).fetchone()
    
#     pg_engine.dispose()

#     meta_key_value = resultset[0]

#     return(meta_key_value)

# def set_meta_key_value(table_id:str, webshop:str, meta_key_name:str, meta_key_value:str):
#     function_that_updated = str(inspect.getframeinfo(inspect.currentframe().f_back)[2])
#     # meta_tbl_id = 'db_data_engineering.bq_dw_operations.api_meta_data_legacy' # postgres table
    
#     # after successfull upload save meta data for future api calls
#     ## meta key value as string to not break anything
#     ## ATTENTION: value is set as string, if other types are needed correct it in the function
#     sql_code = (f"update db_data_engineering.bq_dw_operations.api_meta_data_legacy"
#                 + f" set value_ = '{meta_key_value}', record_uploaded_at_utc = (now() at time zone 'utc')"
#                 + f" where data_table = '{table_id}' and webshop = '{webshop}' and key_ = '{meta_key_name}';") # print(sql_code)

#     pg_engine = connect_to_pg()

#     with pg_engine.begin() as connection:
#         result = connection.execute(db.text(sql_code))
#         # connection.commit()
    
#     pg_engine.dispose()

#     return(f'{function_that_updated} updated {meta_key_name} with {meta_key_value} for table {table_id}.')

def clean_up_column_names(old_column_names:list) -> list:
    # clean up column names... replace special characters, all lower case and rename duplicate column names with a counter https://stackoverflow.com/a/44957247
    new_column_names = []
    for old_column_name in old_column_names:
        clmn_counter = 1
        cleaned_column_name = re.sub("[^a-zA-Z0-9]", "_", old_column_name).lower()
        new_column_name = cleaned_column_name
        while new_column_name in new_column_names:
            clmn_counter += 1
            new_column_name = cleaned_column_name + '_' + str(clmn_counter)    # "{}_{}".format(item, counter)
        new_column_names.append(new_column_name)
    return(new_column_names)

def upload_dataframe_to_bigquery_table(df_to_upload: DataFrame, table_id:str, clmns_to_number:list=[], wrt_dspstn='WRITE_APPEND'): # upload_dataframe_to_bigquery_table upload_dataframe_to_bigquery_table_test
    function_that_uploaded = str(inspect.getframeinfo(inspect.currentframe().f_back)[2])
    
    if len(df_to_upload.index) == 0:
        return(f'{function_that_uploaded} uploaded nothing to {table_id} as dataframe was empty.')    
    
    client = get_bq_client()

    # clean up data frame
    df_to_upload['record_uploaded_at_utc']= pdl.now('UTC').format('YYYY-MM-DD HH:mm:ss.SSSSSS') # 2023-05-08 10:33:39.220560
    df_to_upload = df_to_upload.astype(str)
    df_to_upload['ts_record_upload_at']= pdl.now('UTC')

    # clean up column names... replace special characters, all lower case and rename duplicate column names with a counter https://stackoverflow.com/a/44957247
    new_column_names = clean_up_column_names(old_column_names=df_to_upload.columns.to_list())
    # for old_column_name in df_to_upload.columns:
    #     clmn_counter = 1
    #     cleaned_column_name = re.sub("[^a-zA-Z0-9]", "_", old_column_name).lower()
    #     new_column_name = cleaned_column_name
    #     while new_column_name in new_column_names:
    #         clmn_counter += 1
    #         new_column_name = cleaned_column_name + '_' + str(clmn_counter)    # "{}_{}".format(item, counter)
    #     new_column_names.append(new_column_name)
    df_to_upload.columns = new_column_names

    # convert webshop_partition_id and columns passed in clmns_to_number to number
    clmns_to_number.extend(['shopify_shop_partition_id','company_partition_id'])
    for clmn in clmns_to_number:
        if clmn in df_to_upload.columns:
            df_to_upload[clmn] = to_numeric(df_to_upload[clmn])

    # now upload dataframe
    if wrt_dspstn == 'WRITE_APPEND' or '$' in table_id: # Schema update options should only be specified with WRITE_APPEND disposition, or with WRITE_TRUNCATE disposition on a table partition.
                                        # when '$' character is in the table_id string, it means, a specific partition is targeted
        upload_config = bigquery.LoadJobConfig(write_disposition=wrt_dspstn,schema_update_options='ALLOW_FIELD_ADDITION')
    else:
        upload_config = bigquery.LoadJobConfig(write_disposition=wrt_dspstn)
    query_job = client.load_table_from_dataframe(dataframe=df_to_upload, destination=table_id, job_config=upload_config, project='snocks-analytics') # df_to_upload.to_excel('C:' + os.environ.get('HOMEPATH') + '/Documents/df_to_upload'+ utc_now.strftime("%Y-%m-%dT%H%M%S") +'.xlsx', index=False)
    results = query_job.result() # Waits for job to complete. https://stackoverflow.com/a/53073290 print('bq upload result value is: ' + str(results))

    return(function_that_uploaded + ' uploaded ' + str(len(df_to_upload.index)) + ' records to ' + table_id) # get information about the caller function https://stackoverflow.com/a/3711243


# upload_json_to_table not reviewed/QAd by Christian yet
def upload_json_to_table(rows_to_upload_str: list, table_id: str, autodetect: bool = True):
    client = get_bq_client()

    job_config = bigquery.LoadJobConfig()
    if not autodetect:
        # get the table schema
        schema = get_table_schema(client=client, table_id=table_id)
        if schema:
            job_config.autodetect = False
            job_config.schema = schema
        else:
            job_config.autodetect = True
    else:
        job_config.autodetect = True
    job_config.autodetect = True
    job_config.write_disposition = "WRITE_APPEND"
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.schema_update_options = [
        bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
    ]
    job = client.load_table_from_json(
        json_rows=rows_to_upload_str,
        destination=table_id,
        location="europe-west3",
        job_config=job_config,
    )
    try:
        job.result()
        print(f"LoadJob {job.job_id} successful.")
        print(f"{job.output_rows} uploaded to {table_id}")
        return ("success", job.output_rows, None)
    except BadRequest:
        for error in job.errors:
            print(error)
        return ("failure", None, job.errors)
    
def get_table_schema(client: bigquery.Client, table_id: str):
    table = client.get_table(table_id)
    if table:
        return table.schema
    return None