import boto3

from datetime import datetime
import pytz
import json


import uuid

import psycopg2
from pyspark.sql.types import *
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# AWS Secrets Manager에서 시크릿 값을 가져오기
def get_secret(secret_name):
    """
    AWS Secrets Manager에서 시크릿 값을 가져오는 함수입니다.

    :param secret_name: 가져올 시크릿의 이름
    :return: 시크릿 값 (JSON 형식)
    """
    try:
        secrets_manager = boto3.client('secretsmanager', region_name='ap-northeast-2')
        response = secrets_manager.get_secret_value(SecretId=secret_name)
        return json.loads(response['SecretString'])
    except secrets_manager.exceptions.ResourceNotFoundException:
        error_info = f"시크릿 '{secret_name}'을(를) 찾을 수 없습니다."
        print(error_info)
        raise Exception(error_info)
    except Exception as e:
        error_info = f"시크릿 '{secret_name}'을(를) 가져오는 중 오류 발생: {e}"
        print(error_info)
        raise Exception(error_info)

# SQL Server 및 PostgreSQL 시크릿 값 가져오기
sqlserver_secret = get_secret("dfocus_sqlserver")
postgres_secret = get_secret("dfocus_aurora_postgres")
# SQL Server 및 PostgreSQL 데이터베이스의 시크릿을 가져오기
username_sqlserver = sqlserver_secret.get("username", "")
password_sqlserver = sqlserver_secret.get("password", "")

username_postgres = postgres_secret.get("username", "")
password_postgres = postgres_secret.get("password", "")
# 태블로서버: SQL Server 연결 정보
sqlserver_connection_options = {
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "url": f"jdbc:sqlserver://{sqlserver_secret['host']}:{sqlserver_secret['port']};databaseName={sqlserver_secret['dbname']}",
    "user": sqlserver_secret["username"],
    "password": sqlserver_secret["password"]
}
# Aurora PostgreSQL 연결 정보
postgres_connection_options = {
    "driver": "org.postgresql.Driver",
    "url": f"jdbc:postgresql://{postgres_secret['host']}:{postgres_secret['port']}/datalake",
    "user": postgres_secret["username"],
    "password": postgres_secret["password"]
}
# psycopg2을 위한 PostgreSQL 연결 정보
db_conn_params = {
    'host': "datalake-postgresql-test.cluster-c1k4agnbsi7h.ap-northeast-2.rds.amazonaws.com",
    'database': "datalake",
    "user": postgres_secret["username"],
    "password": postgres_secret["password"]
}


# 로컬 데이터베이스 연결 정보
LOCAL_USER = 'id_kimjm'
LOCAL_PASSWORD = 'pw_kimjm'
LOCAL_HOST = 'postgres'
LOCAL_PORT = '5432'
LOCAL_DATABASE = 'postgres'

# 스파크 연결 시 사용
local_dw_connection_options = {
    "driver": "org.postgresql.Driver",
    "url": f"jdbc:postgresql://postgres:5432/postgres",
    "user": LOCAL_USER,
    "password":LOCAL_PASSWORD,
}

# psycopg2 연결 시 사용
local_dw_jdbc_url = "jdbc:postgresql://{host}:{port}/{database}".format(
    host=LOCAL_HOST,
    port=LOCAL_PORT,
    database=LOCAL_DATABASE
)
local_dw_connection_properties = {
    "user": LOCAL_USER,
    "password": LOCAL_PASSWORD,
    "driver": "org.postgresql.Driver"
}

local_dm_jdbc_url = "jdbc:postgresql://{host}:{port}/{database}".format(
    host=LOCAL_HOST,
    port=LOCAL_PORT,
    database=LOCAL_DATABASE
)
local_dm_connection_properties = {
    "user": LOCAL_USER,
    "password": LOCAL_PASSWORD,
    "driver": "org.postgresql.Driver"
}

dw_db_config = {'dw_jdbc_url': local_dw_jdbc_url, 'dw_connection_properties': local_dw_connection_properties}

dm_db_config = {'dm_jdbc_url': local_dm_jdbc_url, 'dw_connection_properties': local_dm_connection_properties}


# 로그용 클래스
class LogVariables:
    def __init__(self):
        self.log_variables = {
            'job_name': None,
            'job_run_id': None,
            'seq': None,
            'job_type': 'DW_MASTER_ETL',
            
            'total_start_time': None,
            'total_end_time': None,
            'total_elapsed_time': None,

            's3_start_time': None,
            's3_end_time': None,
            's3_elapsed_time': None,
            
            'dw_start_time': None,
            'dw_end_time': None,
            'dw_elapsed_time': None,
            
            'dm_start_time': None,
            'dm_end_time': None,
            'dm_elapsed_time': None,
            
            'error_id': None,
            'error_desc': None,
            'error_time': None,
            'job_exec_info': None,
            'main_table': 'dm_sales',
            'ingest_cnt': None,
            'insert_cnt': None,
            'update_cnt': None,
            'delete_cnt': None,
            'process_insert_cnt': None,
            'process_update_cnt': None,
            'process_delete_cnt': None,
            'process_total_cnt': None
        }

# 로그 스키마를 정의
log_schema = StructType([
    StructField('job_name', StringType(), True), 
    StructField('job_run_id', StringType(), True), 
    StructField('seq', IntegerType(), True), 
    StructField('job_type', StringType(), True),
    
    StructField('total_start_time', TimestampType(), True), 
    StructField('total_end_time', TimestampType(), True), 
    StructField('total_elapsed_time', StringType(), True), 
    
    StructField('s3_start_time', TimestampType(), True), 
    StructField('s3_end_time', TimestampType(), True), 
    StructField('s3_elapsed_time', StringType(), True), 
    
    StructField('dw_start_time', TimestampType(), True), 
    StructField('dw_end_time', TimestampType(), True), 
    StructField('dw_elapsed_time', StringType(), True), 
    
    StructField('dm_start_time', TimestampType(), True), 
    StructField('dm_end_time', TimestampType(), True), 
    StructField('dm_elapsed_time', StringType(), True), 
    
    StructField('error_id', StringType(), True), 
    StructField('error_desc', StringType(), True), 
    StructField('error_time', TimestampType(), True), 
    StructField('job_exec_info', StringType(), True), 
    StructField('main_table', StringType(), True), 
    StructField('ingest_cnt', IntegerType(), True), 
    StructField('insert_cnt', IntegerType(), True), 
    StructField('update_cnt', IntegerType(), True), 
    StructField('delete_cnt', IntegerType(), True), 
    StructField('process_insert_cnt', IntegerType(), True), 
    StructField('process_update_cnt', IntegerType(), True), 
    StructField('process_delete_cnt', IntegerType(), True), 
    StructField('process_total_cnt', IntegerType(), True)
])

def write_logs_to_postgresql(spark, log_variables: Dict, log_schema: StructType, properties: Dict):
    """
    PostgreSQL에 로그 데이터를 저장하는 함수
    
    Args:
        spark (SparkSession): Spark 세션 객체
        log_variables (Dict): 로그 변수를 담은 딕셔너리
        log_schema (StructType): 로그 데이터 프레임의 스키마
        properties (Dict): PostgreSQL 연결 정보를 담은 딕셔너리
        
    Returns:
        None
    """
    
    # 로그 데이터를 Row 객체로 변환
    log_row = Row(**log_variables)
    
    # 로그 데이터프레임 생성
    log_df = spark.createDataFrame([log_row], log_schema)
    
    # PostgreSQL에 로그 데이터 저장
    try:
        log_df.write.jdbc(url=properties["url"], table="etl_job_log", mode="append", properties=properties)
    except Exception as e:
        error_info = f"PostgreSQL에 데이터를 저장하는 중 오류 발생: {e}"
        print(error_info)
        raise Exception(error_info)
        

# 데이터 마트 빌더 클래스 정의
class DataMartSalesBuilder(LogVariables):
    def __init__(self, spark_session: SparkSession, 
                 source_db_config: dict,
                 dw_db_config: dict,
                 dm_db_config: dict
                ):
        super().__init__()
        # SparkSession 
        self.spark = spark_session

        # DB Config
        self.source_db_config = source_db_config
        self.dw_db_config = dw_db_config
        self.dm_db_config = dm_db_config

        # Tables
        self.orders_df = None
        self.order_items_df = None
        self.products_df = None
        self.product_categories_df = None
        self.employees_df = None
        self.customers_df = None
        self.order_items_joined_df = None

        # utils
        self.today_str = datetime.today().strftime("%Y%m%d")

    '''
    테이블 추출(source, dw)
    '''

    def extract_tables(self, db_config):
        # 데이터베이스에서 필요한 테이블 로드
        self.orders_df = self.extract_table("orders", db_config=db_config)
        self.order_items_df = self.extract_table("order_items", db_config=db_config)
        self.products_df = self.extract_table("products", db_config=db_config)
        self.product_categories_df = self.extract_table("product_categories", db_config=db_config)
        self.employees_df = self.extract_table("employees", db_config=db_config)
        self.customers_df = self.extract_table("customers", db_config=db_config)

    def extract_table(self, table_name: str, db_config: dict) -> DataFrame:
        # 테이블 추출 로직을 구현
        try:
            df = self.spark.read.format("jdbc") \
                .options(**db_config) \
                .option("dbtable", table_name) \
                .load()
            return df
            
        except Exception as e:
            return None
            
    '''
    전처리 로직
    1. SALESMAN_ID : Null -> 0, 
        전략: fillna
    2. FULL_NAME : FIRST_NAME + LAST_NAME, 
        전략: concat_ws(with separator)
    3. PHONE: 010_4027_9501 -> ***_****_**** , 
        전략: regexp_replace (정규표현식 기반 문자열 대치)
    4. ORDER_DATE: YYYY_MM_DD -> YYYYMM (집계 레벨에서 파티셔닝할때 처리해야되므로 제외)
        전략: date_format 
    '''
    
    def preprocess_salesman_id_fill_zero(self):
        self.order_items_df = self.order_items_df.fillna({'SALESMAN_ID': 0})

    def preprocess_employees_name(self):
        self.employees_df = self.employees_df.withColumn(
            "FULL_NAME",
            concat_ws(" ", self.employees_df["FIRST_NAME"], self.employees_df["LAST_NAME"])
        )

    def preprocess_phone_masking(self):
        # 전화번호에서 숫자(decimal)를 '*'로, '.'을 '_'로 대체합니다.
        self.employees_df = self.employees_df.withColumn(
            "PHONE",
            regexp_replace(
                regexp_replace(self.employees_df["PHONE"], r'\d', '*'),
                r'\.', '_'
            )
        )

    def preprocess_order_date(self):
        self.order_items_df = self.order_items_df.withColumn(
            "ORDER_DATE",
            date_format("ORDER_DATE", "yyyyMM")
        )
    
    '''
    join, merge 전략
    '''
    def transform_merge(self) -> DataFrame:
        '''
        left outer
        '''
    
        # order_items 데이터프레임을 기준으로 조인 조건 정의
        join_conditions = [
            self.customers_df['CUSTOMER_ID'] == self.order_items_df['CUSTOMER_ID'],
            self.employees_df['EMPLOYEE_ID'] == self.order_items_df['SALESMAN_ID'],
            self.products_df['PRODUCT_ID'] == self.order_items_df['PRODUCT_ID'],
            self.product_categories_df['CATEGORY_ID'] == self.products_df['CATEGORY_ID'],
        ]
    
        # 판매실적(order_items) 데이터프레임을 기준으로 LEFT OUTER JOIN 수행
        df_joined = self.order_items_df.join(
            self.customers_df, on=join_conditions[0], how='left_outer'
        ).join(
            self.employees_df, on=join_conditions[1], how='left_outer'
        ).join(
            self.products_df, on=join_conditions[2], how='left_outer'
        ).join(
            self.product_categories_df, on=join_conditions[3], how='inner'
        )

    
        # Select desired columns
        df_joined = df_joined.select(
            self.order_items_df['CUSTOMER_ID'],
            self.customers_df['NAME'].alias('CUSTOMER_NAME'),  # Use alias for clarity
            self.order_items_df['SALESMAN_ID'],
            self.employees_df['FULL_NAME'].alias('SALESMAN_NAME'),  # Use alias for clarity
            self.employees_df['PHONE'],
            self.employees_df['JOB_TITLE'],
            self.order_items_df['ORDER_DATE'],
            self.order_items_df['PRODUCT_ID'],
            self.products_df['PRODUCT_NAME'],
            self.products_df['DESCRIPTION'],
            self.products_df['CATEGORY_ID'],
            self.product_categories_df['CATEGORY_NAME'],
            self.order_items_df['QUANTITY'],
            self.order_items_df['UNIT_PRICE'],
        )
    
        return df_joined
        
    def transform_join_order_items(self):
        self.order_items_df = self.orders_df.join(self.order_items_df, on='ORDER_ID', how='outer')


    '''
    이관 로직
    source -> landing_zone
        1. extract_data_from_source
        2. load_data_to_landing_zone
    landing_zone -> dw (현재는 추출, 적재가 뭉쳐있는데, 추후 분리 필요..)
        1. extract_data_from_landing_zone_and_load_data_to_dw
    dw -> dm
        1. extract_data_from_dw
        2. load_data_to_dm
    '''
    def extract_data_from_source(self):
        self.extract_tables(db_config=self.source_db_config)
        
    def load_data_to_landing_zone(self):
        self.transform_join_order_items()
        tables = {
            'customers': self.customers_df,
            'employees': self.employees_df,
            'products': self.products_df,
            'product_categories': self.product_categories_df,
            'order_items': self.order_items_df,   
        }
        # S3 버킷 경로 설정
        s3_bucket_path = "s3://kimjm/landing-zone/"
        
        # 각 데이터프레임을 S3에 쓰기
        for table_name, df in tables.items():
            df.write \
              .format("parquet") \
              .mode("overwrite") \
              .option("path", f"{s3_bucket_path}{table_name}/SUCCESS/") \
              .save()
        return

    def extract_data_from_landing_zone_and_load_data_to_dw(self):
        table_names = ['customers', 
                       'employees', 
                       'products', 
                       'product_categories', 
                       'order_items']
        for table_name in table_names:
            s3_path = f"s3://kimjm/landing-zone/{table_name}/SUCCESS"
            df = self.spark.read.format("parquet").load(s3_path)
            # 구성된 PostgreSQL 연결 속성 사용
            df.write \
                .jdbc(url=self.dw_db_config['dw_jdbc_url'], 
                      table=f'{table_name}', 
                      mode="overwrite",
                      properties=self.dw_db_config['dw_connection_properties'])

    def extract_data_from_dw(self):
        self.extract_tables(db_config=local_dw_connection_options)
        
    def load_data_to_dm(self):
        self.preprocess_salesman_id_fill_zero()
        self.preprocess_employees_name()
        self.preprocess_phone_masking()
        # self.preprocess_order_date()
        df = self.transform_merge()
        final_df = self.aggregate_dm(df=df)
        final_df.write \
            .jdbc(url=self.dm_db_config['dm_jdbc_url'], 
                    table=f'dm_sales', 
                    mode="overwrite",
                    properties=self.dm_db_config['dw_connection_properties'])
    
    def aggregate_dm(self, df: DataFrame) -> DataFrame:
        # ORDER_DATE 컬럼을 연월(yyyymm) 형태로 변환
        df = df.withColumn("ORDER_DATE", date_format(df["ORDER_DATE"], "yyyyMM"))
    
        # 당월 금액 (QUANTITY * UNIT_PRICE) 컬럼 생성
        df = df.withColumn("CURR_MM_AMOUNT", col("QUANTITY") * col("UNIT_PRICE"))
    
        # Window 정의: PARTITION BY CUSTOMER_ID, SALESMAN_ID, PRODUCT_ID, ORDER_YYYYMM; ORDER BY ORDER_DATE
        windowSpec = Window.partitionBy("CUSTOMER_ID", "SALESMAN_ID", "PRODUCT_ID", "ORDER_DATE").orderBy("ORDER_DATE")
    
        # 이전 달과 누적 금액을 계산
        # lag 함수 사용하여 이전 달의 금액 계산하고, 결과가 null이면 0으로 바꾸기
        df = df.withColumn("PREV_MM_AMOUNT", coalesce(lag("CURR_MM_AMOUNT").over(windowSpec), lit(0)))
            
        # 당해 당월까지 누적금액
        df = df.withColumn("CURR_MM_ACCM_AMOUNT", sum("CURR_MM_AMOUNT").over(windowSpec.rowsBetween(Window.unboundedPreceding, Window.currentRow)))
    
        # 당해 전월까지 누적금액
        df = df.withColumn("PREV_MM_ACCM_AMOUNT", df["CURR_MM_ACCM_AMOUNT"] - df["CURR_MM_AMOUNT"])
    
        # 최종 DataFrame 반환 또는 저장
        return df


def job_init():
    spark = SparkSession.builder.appName('EtlBuilder').getOrCreate()
    etl_builder = DataMartSalesBuilder(spark_session=spark,
                                    source_db_config=sqlserver_connection_options,
                                    dw_db_config=dw_db_config,
                                    dm_db_config=dm_db_config)
    etl_builder.log_variables['job_name'] = 'kimjm-etl-log-local'
    etl_builder.log_variables['job_run_id'] = uuid.uuid4()
    etl_builder.log_variables['total_start_time'] = datetime.now(pytz.timezone('Asia/Seoul'))
    return etl_builder


## Source(SQL_SERVER) -> Landing Zone(AWS S3)
def job1(etl_builder: object):
    etl_builder.log_variables['s3_start_time'] = datetime.now(pytz.timezone('Asia/Seoul'))
    try:
        etl_builder.extract_data_from_source()
    except Exception as e:
        error_info = f"SQL Server에서 데이터를 읽는 중 오류 발생: {e}"
        print(error_info)
        etl_builder.log_variables['seq'] = 10
        etl_builder.log_variables['error_id'] = 'ERROR_010'  # 오류 ID를 지정합니다.
        etl_builder.log_variables['error_desc'] = str(error_info)  # 오류 설명을 지정합니다.
        etl_builder.log_variables['error_time'] = datetime.now(pytz.timezone('Asia/Seoul'))
        write_logs_to_postgresql(etl_builder.spark, etl_builder.log_variables, log_schema, local_dw_connection_options)
        raise Exception(error_info)
    
    try:
        etl_builder.load_data_to_landing_zone()
    except Exception as e:
        error_info = f"Landing Zone에  적재하는 중 오류 발생: {e}"
        print(error_info)
        etl_builder.log_variables['seq'] = 20
        etl_builder.log_variables['error_id'] = 'ERROR_020'  # 오류 ID를 지정합니다.
        etl_builder.log_variables['error_desc'] = str(error_info)  # 오류 설명을 지정합니다.
        etl_builder.log_variables['error_time'] = datetime.now(pytz.timezone('Asia/Seoul'))
        write_logs_to_postgresql(etl_builder.spark, etl_builder.log_variables, log_schema, local_dw_connection_options)
        
        raise Exception(error_info)
    
    # S3 저장 완료 시간
    etl_builder.log_variables['s3_end_time'] = datetime.now(pytz.timezone('Asia/Seoul'))
    
    etl_builder.log_variables['s3_elapsed_time'] = str(etl_builder.log_variables['s3_end_time'] - etl_builder.log_variables['s3_start_time'])


    ## Landing Zone(AWS S3) -> DW (Aurora Postgres), 로컬에서는 로컬 PG에 업로드중
def job2(etl_builder: object):
    etl_builder.log_variables['dw_start_time'] = datetime.now(pytz.timezone('Asia/Seoul'))
    try:    
        etl_builder.extract_data_from_landing_zone_and_load_data_to_dw()
        etl_builder.log_variables['dw_end_time'] = datetime.now(pytz.timezone('Asia/Seoul'))
        etl_builder.log_variables['dw_elapsed_time'] = str(etl_builder.log_variables['dw_end_time'] - etl_builder.log_variables['dw_start_time'])
    
        
    except Exception as e:
        error_info = f"Landing Zone에서 DW로 적재하는 중 오류 발생: {e}"
        print(error_info)
        etl_builder.log_variables['seq'] = 30
        etl_builder.log_variables['error_id'] = 'ERROR_030'  # 오류 ID를 지정합니다.
        etl_builder.log_variables['error_desc'] = str(error_info)  # 오류 설명을 지정합니다.
        etl_builder.log_variables['error_time'] = datetime.now(pytz.timezone('Asia/Seoul'))
        write_logs_to_postgresql(etl_builder.spark, etl_builder.log_variables, log_schema, local_dw_connection_options)
        
        raise Exception(error_info)
    
    ## DW -> DM
def job3(etl_builder: object):
    etl_builder.log_variables['dm_start_time'] = datetime.now(pytz.timezone('Asia/Seoul'))
    try:
        etl_builder.extract_data_from_dw()
        etl_builder.load_data_to_dm()
        etl_builder.log_variables['dm_end_time'] = datetime.now(pytz.timezone('Asia/Seoul'))
        etl_builder.log_variables['dm_elapsed_time'] = str(etl_builder.log_variables['dm_end_time'] - etl_builder.log_variables['dm_start_time'])
    
    except Exception as e:
        error_info = f"DW에서 DW로 적재하는 중 오류 발생: {e}"
        print(error_info)
        etl_builder.log_variables['seq'] = 40
        etl_builder.log_variables['error_id'] = 'ERROR_040'  # 오류 ID를 지정합니다.
        etl_builder.log_variables['error_desc'] = str(error_info)  # 오류 설명을 지정합니다.
        etl_builder.log_variables['error_time'] = datetime.now(pytz.timezone('Asia/Seoul'))
        write_logs_to_postgresql(etl_builder.spark, etl_builder.log_variables, log_schema, local_dw_connection_options)
        
        raise Exception(error_info)
    
    etl_builder.log_variables['total_end_time'] = datetime.now(pytz.timezone('Asia/Seoul'))
    etl_builder.log_variables['total_elapsed_time'] = str(etl_builder.log_variables['total_end_time'] - etl_builder.log_variables['total_start_time'])
    write_logs_to_postgresql(etl_builder.spark, etl_builder.log_variables, log_schema, local_dw_connection_options)

etl_builder = job_init()
job1(etl_builder=etl_builder)
job2(etl_builder=etl_builder)
job3(etl_builder=etl_builder)