from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators import DummyOperator, PythonOperator
from airflow.operators.redshift_upsert_plugin import RedshiftUpsertOperator
from airflow.operators.redshift_load_plugin import S3ToRedshiftOperator
from airflow.operators.redshift_create_db_plugin import CreateDbTablesOperator
import airflow.hooks.S3_hook
import os
import pandas as pd
import csv

aws_secret_access_key = Variable.get("aws_secret_access_key")
aws_access_key_id = Variable.get("aws_access_key_id")

def upload_file_to_S3_with_hook(filename, key, bucket_name):
    hook = airflow.hooks.S3_hook.S3Hook('S3_conn')
    hook.load_file(filename, key, bucket_name, replace=True)

def clean_reviews_dataset(filename):
    df = pd.read_csv (filename)
    df.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=["",""], regex=True, inplace=True)

    df['review_comment_message'] = df['review_comment_message'].str.replace(',', ' ')
    df['review_comment_message'] = df['review_comment_message'].str.replace(r'[^\x00-\x7F]+', ' ')
    df['review_comment_message'] = df['review_comment_message'].str.replace('\'', ' ')
    df['review_comment_message'] = df['review_comment_message'].str.replace('[^a-zA-Z]', ' ')
    df['review_comment_title'] = df['review_comment_title'].str.replace(',', ' ')

    df.to_csv(filename,  index=False)


default_args = {
    'owner': 'ramsha',
    'start_date': datetime(2021, 3, 2),
    'retry_delay': timedelta(minutes=5)
}
# Using the context manager alllows you not to duplicate the dag parameter in each operator
with DAG('e_commerce_dag_split', default_args=default_args, schedule_interval='@daily',catchup=False) as dag:

    clean_order_reviews_dataset_1 = PythonOperator(
            task_id='clean_order_reviews_dataset_1',
            python_callable=clean_reviews_dataset,
            op_kwargs={'filename' :'/usr/local/airflow/dags/dags/data_files_split/order_reviews_part_1.csv'}
    )

    clean_order_reviews_dataset_2 = PythonOperator(
            task_id='clean_order_reviews_dataset_2',
            python_callable=clean_reviews_dataset,
            op_kwargs={'filename' :'/usr/local/airflow/dags/dags/data_files_split/order_reviews_part_2.csv'}
    )

    clean_order_reviews_dataset_3 = PythonOperator(
            task_id='clean_order_reviews_dataset_3',
            python_callable=clean_reviews_dataset,
            op_kwargs={'filename' :'/usr/local/airflow/dags/dags/data_files_split/order_reviews_part_3.csv'}
    )

    upload_S3_customers_1 = PythonOperator(
                    task_id='upload_S3_customers_1',
                    python_callable=upload_file_to_S3_with_hook,
                    op_kwargs={'filename' : '/usr/local/airflow/dags/dags/data_files_split/customers_part_1.csv',
                                'key': 'customers_part_1.csv',
                                'bucket_name': 'brazillian-e-commerce-data'}
            )

    upload_S3_customers_2 = PythonOperator(
                    task_id='upload_S3_customers_2',
                    python_callable=upload_file_to_S3_with_hook,
                    op_kwargs={'filename' : '/usr/local/airflow/dags/dags/data_files_split/customers_part_2.csv',
                                'key': 'customers_part_2.csv',
                                'bucket_name': 'brazillian-e-commerce-data'}
            )

    upload_S3_customers_3 = PythonOperator(
                    task_id='upload_S3_customers_3',
                    python_callable=upload_file_to_S3_with_hook,
                    op_kwargs={'filename' : '/usr/local/airflow/dags/dags/data_files_split/customers_part_3.csv',
                                'key': 'customers_part_3.csv',
                                'bucket_name': 'brazillian-e-commerce-data'}
            )


    upload_S3_geolocation_1 = PythonOperator(
                    task_id='upload_S3_geolocation_1',
                    python_callable=upload_file_to_S3_with_hook,
                    op_kwargs={'filename' : '/usr/local/airflow/dags/dags/data_files_split/geolocation_part_1.csv',
                                'key': 'geolocation_part_1.csv',
                                'bucket_name': 'brazillian-e-commerce-data'}
            )

    upload_S3_geolocation_2 = PythonOperator(
                    task_id='upload_S3_geolocation_2',
                    python_callable=upload_file_to_S3_with_hook,
                    op_kwargs={'filename' : '/usr/local/airflow/dags/dags/data_files_split/geolocation_part_2.csv',
                                'key': 'geolocation_part_2.csv',
                                'bucket_name': 'brazillian-e-commerce-data'}
            )

    upload_S3_geolocation_3 = PythonOperator(
                    task_id='upload_S3_geolocation_3',
                    python_callable=upload_file_to_S3_with_hook,
                    op_kwargs={'filename' : '/usr/local/airflow/dags/dags/data_files_split/geolocation_part_3.csv',
                                'key': 'geolocation_part_3.csv',
                                'bucket_name': 'brazillian-e-commerce-data'}
            )


    upload_S3_order_items_1 = PythonOperator(
                    task_id='upload_S3_order_items_1',
                    python_callable=upload_file_to_S3_with_hook,
                    op_kwargs={'filename' : '/usr/local/airflow/dags/dags/data_files_split/order_items_part_1.csv',
                                'key': 'order_items_part_1.csv',
                                'bucket_name': 'brazillian-e-commerce-data'}
            )

    upload_S3_order_items_2 = PythonOperator(
                    task_id='upload_S3_order_items_2',
                    python_callable=upload_file_to_S3_with_hook,
                    op_kwargs={'filename' : '/usr/local/airflow/dags/dags/data_files_split/order_items_part_2.csv',
                                'key': 'order_items_part_2.csv',
                                'bucket_name': 'brazillian-e-commerce-data'}
            )

    upload_S3_order_items_3 = PythonOperator(
                    task_id='upload_S3_order_items_3',
                    python_callable=upload_file_to_S3_with_hook,
                    op_kwargs={'filename' : '/usr/local/airflow/dags/dags/data_files_split/order_items_part_3.csv',
                                'key': 'order_items_part_3.csv',
                                'bucket_name': 'brazillian-e-commerce-data'}
            )


    upload_S3_order_payments_1 = PythonOperator(
                    task_id='upload_S3_order_payments_1',
                    python_callable=upload_file_to_S3_with_hook,
                    op_kwargs={'filename' : '/usr/local/airflow/dags/dags/data_files_split/order_payments_part_1.csv',
                                'key': 'order_payments_part_1.csv',
                                'bucket_name': 'brazillian-e-commerce-data'}
            )

    upload_S3_order_payments_2 = PythonOperator(
                    task_id='upload_S3_order_payments_2',
                    python_callable=upload_file_to_S3_with_hook,
                    op_kwargs={'filename' : '/usr/local/airflow/dags/dags/data_files_split/order_payments_part_2.csv',
                                'key': 'order_payments_part_2.csv',
                                'bucket_name': 'brazillian-e-commerce-data'}
            )

    upload_S3_order_payments_3 = PythonOperator(
                    task_id='upload_S3_order_payments_3',
                    python_callable=upload_file_to_S3_with_hook,
                    op_kwargs={'filename' : '/usr/local/airflow/dags/dags/data_files_split/order_payments_part_3.csv',
                                'key': 'order_payments_part_3.csv',
                                'bucket_name': 'brazillian-e-commerce-data'}
            )


    upload_S3_order_reviews_1 = PythonOperator(
                    task_id='upload_S3_order_reviews_1',
                    python_callable=upload_file_to_S3_with_hook,
                    op_kwargs={'filename' : '/usr/local/airflow/dags/dags/data_files_split/order_reviews_part_1.csv',
                                'key': 'order_reviews_part_1.csv',
                                'bucket_name': 'brazillian-e-commerce-data'}
            )

    upload_S3_order_reviews_2 = PythonOperator(
                    task_id='upload_S3_order_reviews_2',
                    python_callable=upload_file_to_S3_with_hook,
                    op_kwargs={'filename' : '/usr/local/airflow/dags/dags/data_files_split/order_reviews_part_2.csv',
                                'key': 'order_reviews_part_2.csv',
                                'bucket_name': 'brazillian-e-commerce-data'}
            )

    upload_S3_order_reviews_3 = PythonOperator(
                    task_id='upload_S3_order_reviews_3',
                    python_callable=upload_file_to_S3_with_hook,
                    op_kwargs={'filename' : '/usr/local/airflow/dags/dags/data_files_split/order_reviews_part_3.csv',
                                'key': 'order_reviews_part_3.csv',
                                'bucket_name': 'brazillian-e-commerce-data'}
            )


    upload_S3_orders_1 = PythonOperator(
                    task_id='upload_S3_orders_1',
                    python_callable=upload_file_to_S3_with_hook,
                    op_kwargs={'filename' : '/usr/local/airflow/dags/dags/data_files_split/orders_part_1.csv',
                                'key': 'orders_part_1.csv',
                                'bucket_name': 'brazillian-e-commerce-data'}
            )

    upload_S3_orders_2 = PythonOperator(
                    task_id='upload_S3_orders_2',
                    python_callable=upload_file_to_S3_with_hook,
                    op_kwargs={'filename' : '/usr/local/airflow/dags/dags/data_files_split/orders_part_2.csv',
                                'key': 'orders_part_2.csv',
                                'bucket_name': 'brazillian-e-commerce-data'}
            )

    upload_S3_orders_3 = PythonOperator(
                    task_id='upload_S3_orders_3',
                    python_callable=upload_file_to_S3_with_hook,
                    op_kwargs={'filename' : '/usr/local/airflow/dags/dags/data_files_split/orders_part_3.csv',
                                'key': 'orders_part_3.csv',
                                'bucket_name': 'brazillian-e-commerce-data'}
            )


    upload_S3_products_1 = PythonOperator(
                    task_id='upload_S3_products_1',
                    python_callable=upload_file_to_S3_with_hook,
                    op_kwargs={'filename' : '/usr/local/airflow/dags/dags/data_files_split/products_part_1.csv',
                                'key': 'products_part_1.csv',
                                'bucket_name': 'brazillian-e-commerce-data'}
            )

    upload_S3_products_2 = PythonOperator(
                    task_id='upload_S3_products_2',
                    python_callable=upload_file_to_S3_with_hook,
                    op_kwargs={'filename' : '/usr/local/airflow/dags/dags/data_files_split/products_part_2.csv',
                                'key': 'products_part_2.csv',
                                'bucket_name': 'brazillian-e-commerce-data'}
            )

    upload_S3_products_3 = PythonOperator(
                    task_id='upload_S3_products_3',
                    python_callable=upload_file_to_S3_with_hook,
                    op_kwargs={'filename' : '/usr/local/airflow/dags/dags/data_files_split/products_part_3.csv',
                                'key': 'products_part_3.csv',
                                'bucket_name': 'brazillian-e-commerce-data'}
            )


    upload_S3_sellers_1 = PythonOperator(
                    task_id='upload_S3_sellers_1',
                    python_callable=upload_file_to_S3_with_hook,
                    op_kwargs={'filename' : '/usr/local/airflow/dags/dags/data_files_split/sellers_part_1.csv',
                                'key': 'sellers_part_1.csv',
                                'bucket_name': 'brazillian-e-commerce-data'}
            )

    upload_S3_sellers_2 = PythonOperator(
                    task_id='upload_S3_sellers_2',
                    python_callable=upload_file_to_S3_with_hook,
                    op_kwargs={'filename' : '/usr/local/airflow/dags/dags/data_files_split/sellers_part_2.csv',
                                'key': 'sellers_part_2.csv',
                                'bucket_name': 'brazillian-e-commerce-data'}
            )

    upload_S3_sellers_3 = PythonOperator(
                    task_id='upload_S3_sellers_3',
                    python_callable=upload_file_to_S3_with_hook,
                    op_kwargs={'filename' : '/usr/local/airflow/dags/dags/data_files_split/sellers_part_3.csv',
                                'key': 'sellers_part_3.csv',
                                'bucket_name': 'brazillian-e-commerce-data'}
            )


    upload_S3_product_category_name_translation_1 = PythonOperator(
                    task_id='upload_S3_product_category_name_translation_1',
                    python_callable=upload_file_to_S3_with_hook,
                    op_kwargs={'filename' : '/usr/local/airflow/dags/dags/data_files_split/product_category_name_translation_part_1.csv',
                                'key': 'product_category_name_translation_1.csv',
                                'bucket_name': 'brazillian-e-commerce-data'}
            )

    upload_S3_product_category_name_translation_2 = PythonOperator(
                    task_id='upload_S3_product_category_name_translation_2',
                    python_callable=upload_file_to_S3_with_hook,
                    op_kwargs={'filename' : '/usr/local/airflow/dags/dags/data_files_split/product_category_name_translation_part_2.csv',
                                'key': 'product_category_name_translation_2.csv',
                                'bucket_name': 'brazillian-e-commerce-data'}
            )

    create_db = CreateDbTablesOperator(
        task_id = 'create_db',
        redshift_conn_id='redshift_conn')

    load_customers = S3ToRedshiftOperator(
        task_id="load_customers",
        redshift_conn_id="redshift_conn",
        table="customers_staging",
        s3_bucket="brazillian-e-commerce-data",
        s3_path="customers",
        s3_access_key_id=aws_access_key_id,
        s3_secret_access_key=aws_secret_access_key,
        delimiter=",",
        region="ca-central-1")

    load_geolocation = S3ToRedshiftOperator(
        task_id="load_geolocation",
        redshift_conn_id="redshift_conn",
        table="geolocation_staging",
        s3_bucket="brazillian-e-commerce-data",
        s3_path="geolocation",
        s3_access_key_id=aws_access_key_id,
        s3_secret_access_key=aws_secret_access_key,
        delimiter=",",
        region="ca-central-1")

    load_order_items = S3ToRedshiftOperator(
        task_id="load_order_items",
        redshift_conn_id="redshift_conn",
        table="order_items_staging",
        s3_bucket="brazillian-e-commerce-data",
        s3_path="order_items",
        s3_access_key_id=aws_access_key_id,
        s3_secret_access_key=aws_secret_access_key,
        delimiter=",",
        region="ca-central-1")

    load_order_payments = S3ToRedshiftOperator(
        task_id="load_order_payments",
        redshift_conn_id="redshift_conn",
        table="order_payments_staging",
        s3_bucket="brazillian-e-commerce-data",
        s3_path="order_payments",
        s3_access_key_id=aws_access_key_id,
        s3_secret_access_key=aws_secret_access_key,
        delimiter=",",
        region="ca-central-1")

    load_order_reviews = S3ToRedshiftOperator(
        task_id="load_order_reviews",
        redshift_conn_id="redshift_conn",
        table="order_reviews_staging",
        s3_bucket="brazillian-e-commerce-data",
        s3_path="order_reviews",
        s3_access_key_id=aws_access_key_id,
        s3_secret_access_key=aws_secret_access_key,
        delimiter=",",
        region="ca-central-1")

    load_orders = S3ToRedshiftOperator(
        task_id="load_orders",
        redshift_conn_id="redshift_conn",
        table="orders_staging",
        s3_bucket="brazillian-e-commerce-data",
        s3_path="orders",
        s3_access_key_id=aws_access_key_id,
        s3_secret_access_key=aws_secret_access_key,
        delimiter=",",
        region="ca-central-1")

    load_products = S3ToRedshiftOperator(
        task_id="load_products",
        redshift_conn_id="redshift_conn",
        table="products_staging",
        s3_bucket="brazillian-e-commerce-data",
        s3_path="products",
        s3_access_key_id=aws_access_key_id,
        s3_secret_access_key=aws_secret_access_key,
        delimiter=",",
        region="ca-central-1")

    load_sellers = S3ToRedshiftOperator(
        task_id="load_sellers",
        redshift_conn_id="redshift_conn",
        table="sellers_staging",
        s3_bucket="brazillian-e-commerce-data",
        s3_path="sellers",
        s3_access_key_id=aws_access_key_id,
        s3_secret_access_key=aws_secret_access_key,
        delimiter=",",
        region="ca-central-1")

    load_product_category_name_translation = S3ToRedshiftOperator(
        task_id="load_product_category_name_translation",
        redshift_conn_id="redshift_conn",
        table="product_category_name_translation_staging",
        s3_bucket="brazillian-e-commerce-data",
        s3_path="product_category_name_translation",
        s3_access_key_id=aws_access_key_id,
        s3_secret_access_key=aws_secret_access_key,
        delimiter=",",
        region="ca-central-1")

	
    upsert_customers = RedshiftUpsertOperator(
        task_id='upsert_customers',
        src_redshift_conn_id="redshift_conn",
        dest_redshift_conn_id="redshift_conn",
        src_table="customers_staging",
        dest_table="customers",
        src_keys=["customer_id"],
        dest_keys=["customer_id"]
        )

    upsert_geolocation = RedshiftUpsertOperator(
        task_id='upsert_geolocation',
        src_redshift_conn_id="redshift_conn",
        dest_redshift_conn_id="redshift_conn",
        src_table="geolocation_staging",
        dest_table="geolocation",
        src_keys=["geolocation_zip_code_prefix"],
        dest_keys=["geolocation_zip_code_prefix"],
        dag = dag
        )

    upsert_order_items = RedshiftUpsertOperator(
        task_id='upsert_order_items',
        src_redshift_conn_id="redshift_conn",
        dest_redshift_conn_id="redshift_conn",
        src_table="order_items_staging",
        dest_table="order_items",
        src_keys=["order_id"],
        dest_keys=["order_id"],
        dag = dag
        )

    upsert_order_payments = RedshiftUpsertOperator(
        task_id='upsert_order_payments',
        src_redshift_conn_id="redshift_conn",
        dest_redshift_conn_id="redshift_conn",
        src_table="order_payments_staging",
        dest_table="order_payments",
        src_keys=["order_id"],
        dest_keys=["order_id"],
        dag = dag
        )

    upsert_order_reviews = RedshiftUpsertOperator(
        task_id='upsert_order_reviews',
        src_redshift_conn_id="redshift_conn",
        dest_redshift_conn_id="redshift_conn",
        src_table="order_reviews_staging",
        dest_table="order_reviews",
        src_keys=["review_id"],
        dest_keys=["review_id"],
        dag = dag
        )

    upsert_orders = RedshiftUpsertOperator(
        task_id='upsert_orders',
        src_redshift_conn_id="redshift_conn",
        dest_redshift_conn_id="redshift_conn",
        src_table="orders_staging",
        dest_table="orders",
        src_keys=["order_id"],
        dest_keys=["order_id"],
        dag = dag
        )

    upsert_products = RedshiftUpsertOperator(
        task_id='upsert_products',
        src_redshift_conn_id="redshift_conn",
        dest_redshift_conn_id="redshift_conn",
        src_table="products_staging",
        dest_table="products",
        src_keys=["product_id"],
        dest_keys=["product_id"],
        dag = dag
        )

    upsert_sellers = RedshiftUpsertOperator(
        task_id='upsert_sellers',
        src_redshift_conn_id="redshift_conn",
        dest_redshift_conn_id="redshift_conn",
        src_table="sellers_staging",
        dest_table="sellers",
        src_keys=["seller_id"],
        dest_keys=["seller_id"],
        dag = dag
        )

    upsert_product_category_name_translation = RedshiftUpsertOperator(
        task_id='upsert_product_category_name_translation',
        src_redshift_conn_id="redshift_conn",
        dest_redshift_conn_id="redshift_conn",
        src_table="product_category_name_translation_staging",
        dest_table="product_category_name_translation",
        src_keys=["product_category_name"],
        dest_keys=["product_category_name"],
        dag = dag
        )

    upload_S3_customers_1 >> upload_S3_customers_2 >> upload_S3_customers_3 >> create_db
    upload_S3_order_payments_1 >> upload_S3_order_payments_2 >> upload_S3_order_payments_3 >> create_db
    upload_S3_geolocation_1 >> upload_S3_geolocation_2 >> upload_S3_geolocation_3 >> create_db
    clean_order_reviews_dataset_1 >> upload_S3_order_reviews_1 >> create_db
    clean_order_reviews_dataset_2 >> upload_S3_order_reviews_2 >> create_db
    clean_order_reviews_dataset_3 >> upload_S3_order_reviews_3 >> create_db
    upload_S3_orders_1 >> upload_S3_orders_2 >> upload_S3_orders_3 >> create_db
    upload_S3_order_items_1 >> upload_S3_order_items_2 >> upload_S3_order_items_3 >> create_db
    upload_S3_products_1 >> upload_S3_products_2 >> upload_S3_products_3 >> create_db
    upload_S3_sellers_1 >> upload_S3_sellers_2 >> upload_S3_sellers_3 >> create_db
    upload_S3_product_category_name_translation_1 >> upload_S3_product_category_name_translation_2 >> create_db
    
    create_db >> load_customers >> upsert_customers 
    create_db >> load_geolocation >> upsert_geolocation 
    create_db >> load_order_items >> upsert_order_items 
    create_db >> load_order_payments >> upsert_order_payments 
    create_db >> load_order_reviews >> upsert_order_reviews 
    create_db >> load_orders >> upsert_orders
    create_db >> load_products >> upsert_products 
    create_db >> load_sellers >> upsert_sellers 
    create_db >> load_product_category_name_translation >> upsert_product_category_name_translation 


