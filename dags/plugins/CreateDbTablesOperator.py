import logging
 
from airflow.hooks.postgres_hook import PostgresHook
from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
 
log = logging.getLogger(__name__)
 
class CreateDbTablesOperator(BaseOperator):
  
  @apply_defaults
  def __init__(self, redshift_conn_id, *args, **kwargs):
  
    self.redshift_conn_id = redshift_conn_id
    super(CreateDbTablesOperator, self).__init__(*args, **kwargs)
  
  
  def execute(self, context):
    self.hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
    conn = self.hook.get_conn() 
    cursor = conn.cursor()
    log.info("Connected with " + self.redshift_conn_id)
  
    self.sql_statements = """
          	  drop table IF EXISTS customers_staging CASCADE;
              drop table IF EXISTS geolocation_staging CASCADE;
              drop table IF EXISTS order_items_staging CASCADE;
              drop table IF EXISTS order_payments_staging CASCADE;
              drop table IF EXISTS order_reviews_staging CASCADE;
              drop table IF EXISTS orders_staging CASCADE;
              drop table IF EXISTS products_staging CASCADE;
              drop table IF EXISTS sellers_staging CASCADE;
              drop table IF EXISTS product_category_name_translation_staging CASCADE;

              drop table  IF EXISTS customers CASCADE;
              drop table IF EXISTS geolocation CASCADE;
              drop table IF EXISTS order_items CASCADE;
              drop table IF EXISTS order_payments CASCADE;
              drop table IF EXISTS order_reviews CASCADE;
              drop table IF EXISTS orders CASCADE;
              drop table IF EXISTS products CASCADE;
              drop table IF EXISTS sellers CASCADE;
              drop table IF EXISTS product_category_name_translation CASCADE;

              CREATE TABLE customers_staging
                (
                  customer_id  VARCHAR(100) NOT NULL UNIQUE,
                  customer_unique_id  VARCHAR(100) NOT NULL UNIQUE,
                  customer_zip_code_prefix  INT,
                  customer_city  VARCHAR(100),
                  customer_state  VARCHAR(100),
                  PRIMARY KEY  (customer_id, customer_unique_id)
                )
                DISTSTYLE AUTO
                SORTKEY AUTO;

              CREATE TABLE orders_staging 
                (
                  order_id  VARCHAR(100) NOT NULL UNIQUE,
                  customer_id  VARCHAR(100) NOT NULL UNIQUE,
                  order_status  VARCHAR(100) NOT NULL,
                  order_purchase_timestamp  timestamp NOT NULL,
                  order_approved_at  timestamp DEFAULT NULL,
                  order_delivered_carrier_date  timestamp DEFAULT NULL,
                  order_delivered_customer_date  timestamp DEFAULT NULL,
                  order_delivered_delivery_date  timestamp NOT NULL,
                  PRIMARY KEY (order_id, customer_id)
                )
                DISTSTYLE AUTO
                SORTKEY AUTO;

              CREATE TABLE products_staging 
                (
                  product_id  VARCHAR(100) NOT NULL UNIQUE,
                  product_category_name  VARCHAR(100),
                  product_name_lenght  INT,
                  product_description_lenght  INT,
                  product_photos_qty  INT,
                  product_weight_g  INT,
                  product_length_cm  INT,
                  product_height_cm  INT,
                  product_width_cm  INT,
                  PRIMARY KEY (product_id)
                )
                DISTSTYLE AUTO
                SORTKEY AUTO;

              CREATE TABLE sellers_staging 
                (
                  seller_id  VARCHAR(100) NOT NULL UNIQUE,
                  seller_zip_code_prefix  INT NOT NULL,
                  seller_city  VARCHAR(100) NOT NULL,
                  seller_state  VARCHAR(100) NOT NULL,
                  PRIMARY KEY (seller_id)
                )
                DISTSTYLE AUTO
                SORTKEY AUTO;

              CREATE TABLE geolocation_staging 
                (
                  geolocation_zip_code_prefix  INT NOT NULL,
                  geolocation_lat  VARCHAR(100) NOT NULL,
                  geolocation_lng  VARCHAR(100) NOT NULL,
                  geolocation_city  VARCHAR(255) NOT NULL,
                  geolocation_state  VARCHAR(2) NOT NULL
                )
                DISTSTYLE AUTO
                SORTKEY AUTO;

              CREATE TABLE order_items_staging 
                (
                  order_id  VARCHAR(100) NOT NULL UNIQUE,
                  order_item_id  INT NOT NULL,
                  product_id  VARCHAR(100) NOT NULL UNIQUE,
                  seller_id  VARCHAR(100) NOT NULL UNIQUE,
                  shipping_limit_date  timestamp NOT NULL,
                  price  FLOAT NOT NULL,
                  freight_value  FLOAT NOT NULL,
                  FOREIGN KEY (order_id) REFERENCES orders_staging(order_id),
                  FOREIGN KEY (product_id) REFERENCES products_staging(product_id),
                  FOREIGN KEY (seller_id) REFERENCES sellers_staging(seller_id)
                )
                DISTSTYLE AUTO
                SORTKEY AUTO;

              CREATE TABLE order_payments_staging 
                (
                  order_id  VARCHAR(100) NOT NULL UNIQUE,
                  payment_sequential  INT NOT NULL,
                  payment_type  VARCHAR(100) NOT NULL,
                  payment_installments  INT NOT NULL,
                  payment_value  FLOAT NOT NULL,
                  FOREIGN KEY (order_id) REFERENCES orders_staging(order_id)
                )
                DISTSTYLE AUTO
                SORTKEY AUTO;

              CREATE TABLE order_reviews_staging 
                (
                  review_id  VARCHAR NOT NULL UNIQUE,
                  order_id  VARCHAR NOT NULL UNIQUE,
                  review_score  INT NOT NULL,
                  review_comment_title  VARCHAR,
                  review_comment_message  VARCHAR,
                  review_creation_date  timestamp,
                  review_answer_timestamp  timestamp,
                  PRIMARY KEY (review_id),
                  FOREIGN KEY (order_id) REFERENCES orders_staging(order_id)
                )
                DISTSTYLE AUTO
                SORTKEY AUTO;

              CREATE TABLE product_category_name_translation_staging
                (
                  product_category_name  VARCHAR(100) NOT NULL UNIQUE,
                  product_category_name_english  VARCHAR(100) NOT NULL UNIQUE
                )
                DISTSTYLE AUTO
                SORTKEY AUTO;

              CREATE TABLE customers
                (
                  customer_id  VARCHAR(100) NOT NULL UNIQUE,
                  customer_unique_id  VARCHAR(100) NOT NULL UNIQUE,
                  customer_zip_code_prefix  INT,
                  customer_city  VARCHAR(100),
                  customer_state  VARCHAR(100),
                  PRIMARY KEY  (customer_id, customer_unique_id)
                )
                DISTSTYLE AUTO
                SORTKEY AUTO;

              CREATE TABLE orders
                (
                  order_id  VARCHAR(100) NOT NULL UNIQUE,
                  customer_id  VARCHAR(100) NOT NULL UNIQUE,
                  order_status  VARCHAR(100) NOT NULL,
                  order_purchase_timestamp  timestamp NOT NULL,
                  order_approved_at  timestamp DEFAULT NULL,
                  order_delivered_carrier_date  timestamp DEFAULT NULL,
                  order_delivered_customer_date  timestamp DEFAULT NULL,
                  order_delivered_delivery_date  timestamp NOT NULL,
                  PRIMARY KEY (order_id, customer_id)
                )
                DISTSTYLE AUTO
                SORTKEY AUTO;

              CREATE TABLE products 
                (
                  product_id  VARCHAR(100) NOT NULL UNIQUE,
                  product_category_name  VARCHAR(100),
                  product_name_lenght  INT,
                  product_description_lenght  INT,
                  product_photos_qty  INT,
                  product_weight_g  INT,
                  product_length_cm  INT,
                  product_height_cm  INT,
                  product_width_cm  INT,
                  PRIMARY KEY (product_id)
                )
                DISTSTYLE AUTO
                SORTKEY AUTO;

              CREATE TABLE sellers 
                (
                  seller_id  VARCHAR(100) NOT NULL UNIQUE,
                  seller_zip_code_prefix  INT NOT NULL,
                  seller_city  VARCHAR(100) NOT NULL,
                  seller_state  VARCHAR(100) NOT NULL,
                  PRIMARY KEY (seller_id)
                )
                DISTSTYLE AUTO
                SORTKEY AUTO;

              CREATE TABLE geolocation 
                (
                  geolocation_zip_code_prefix  INT NOT NULL,
                  geolocation_lat  VARCHAR(100) NOT NULL,
                  geolocation_lng  VARCHAR(100) NOT NULL,
                  geolocation_city  VARCHAR NOT NULL,
                  geolocation_state  VARCHAR NOT NULL
                )
                DISTSTYLE AUTO
                SORTKEY AUTO;

              CREATE TABLE order_items 
                (
                  order_id  VARCHAR(100) NOT NULL UNIQUE,
                  order_item_id  INT NOT NULL,
                  product_id  VARCHAR(100) NOT NULL UNIQUE,
                  seller_id  VARCHAR(100) NOT NULL UNIQUE,
                  shipping_limit_date  timestamp NOT NULL,
                  price  FLOAT NOT NULL,
                  freight_value  FLOAT NOT NULL,
                  FOREIGN KEY (order_id) REFERENCES orders_staging(order_id),
                  FOREIGN KEY (product_id) REFERENCES products_staging(product_id),
                  FOREIGN KEY (seller_id) REFERENCES sellers_staging(seller_id)
                )
                DISTSTYLE AUTO
                SORTKEY AUTO;

              CREATE TABLE order_payments 
                (
                  order_id  VARCHAR(100) NOT NULL UNIQUE,
                  payment_sequential  INT NOT NULL,
                  payment_type  VARCHAR(100) NOT NULL,
                  payment_installments  INT NOT NULL,
                  payment_value  FLOAT NOT NULL,
                  FOREIGN KEY (order_id) REFERENCES orders_staging(order_id)
                )
                DISTSTYLE AUTO
                SORTKEY AUTO;

              CREATE TABLE order_reviews
                (
                  review_id  VARCHAR(100) NOT NULL UNIQUE,
                  order_id  VARCHAR(100) NOT NULL UNIQUE,
                  review_score  INT NOT NULL,
                  review_comment_title  VARCHAR(100),
                  review_comment_message  VARCHAR,
                  review_creation_date  timestamp,
                  review_answer_timestamp  timestamp,
                  PRIMARY KEY (review_id),
                  FOREIGN KEY (order_id) REFERENCES orders_staging(order_id)
                )
                DISTSTYLE AUTO
                SORTKEY AUTO;

              CREATE TABLE product_category_name_translation
                (
                  product_category_name  VARCHAR(100) NOT NULL UNIQUE,
                  product_category_name_english  VARCHAR(100) NOT NULL UNIQUE
                )
                DISTSTYLE AUTO
                SORTKEY AUTO;"""
    
    self.log.info("Recreating tables...")
    self.hook.run(self.sql_statements)
    self.log.info("Finished recreating tables...")
    return True
  
  
class CreateDbTablesOperatorPlugin(AirflowPlugin):
  name = "redshift_create_db_plugin"
  operators = [CreateDbTablesOperator]





