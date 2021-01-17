# Udacity Capstone Project

## Project Dataset : Brazilian E-Commerce Dataset by Olist

For the Udacity Data Engineer Nanodegree capstone project I chose to work on the Brazilian ecommerce public dataset of orders made at [Olist Store](https://olist.com/). The dataset has information of 100k orders from 2016 to 2018 made at multiple marketplaces in Brazil. Its features allows viewing an order from multiple dimensions: from order status, price, payment and freight performance to customer location, product attributes and finally reviews written by customers. For more details go to the [dataset](https://www.kaggle.com/olistbr/brazilian-ecommerce) available at Kaggle.

Since this is real commercial data, it has been anonymised, and references to the companies and partners in the review text have been replaced with the names of Game of Thrones great houses (interesting!!).

## Exploratory Data Analysis

The dataset has been explored to identify missing values and duplicate data. Please refer to `eda.ipynb` notebook for a detailed analysis on the dataset.

## Olist Sales Data Warehouse Schema

***Data Warehouse/ Mart Scope:***  For Sales analysis targeted to serve the sales department.

***Granularity/ Level of Detail of the Data Warehouse:***  Each record in the FACT table contains an order item in a particular order given by `order_id`. So the granularity is order item level for all orders from 2016 to 2018.

***Kinds of questions to be answered by the Warehouse:***  

- No. of orders received in each month/ year
- Avg total of orders received in each day/ month/ year
- Weekly/ Monthly/ yearly Avg spendings of each customer
- Weekly/ Monthly/ yearly Avg sales of each product type
- Weekly/ Monthly/ yearly Avg sales made by each seller
- Weekly/ Monthly/ yearly city/ state wise sales 

***Type of dimensional model/ schema:*** `STAR SCHEMA`

#### **DIMENSION Tables:**

```plsql
customer(customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state)  
  seller(seller_id, seller_zip_code_prefix, seller_city, seller_state)
 product(product_id,
         product_category,            --> Translated using "product_category_name_translation.csv"
         product_photos_qty,
         product_weight_g,
         product_length_cm,
         product_height_cm,
         product_width_cm)
calender(calender_id, date, day,
         dayofweek, month, year)
```

#### FACT Table:

```plsql
order_item(
           order_item_id,                          --> Artificial PK
           order_id,                               --> orders
           customer_id,                            --> orders
           product_id,                             --> order_items
           seller_id,                              --> order_items
           purchase_date,                          --> orders (from "order_purchase_timestamp")
           purchase_year,                          --> orders (from "order_purchase_timestamp")
           purchase_month,                         --> orders (from "order_purchase_timestamp")
           order_purchase_timestamp,               --> orders (from "order_purchase_timestamp")
           order_approved_timestamp,               --> orders (from "order_approved_at")
           order_delivered_carrier_timestamp,      --> orders (EXTRACT DATE from "order_delivered_carrier_date")
           order_delivered_customer_timestamp,     --> orders (EXTRACT DATE from "order_delivered_customer_date")
           order_estimated_delivery_timestamp,     --> orders (EXTRACT DATE from "order_estimated_delivery_date")
           order_status,                           --> orders
           shipping_limit_date,                    --> order_items
           freight_value,                          --> order_items
           unit_price,                             --> order_items (rename "price" -> "unit_price")
           qty,                                    --> order_items (derived measure)
           total_product_price,                    --> (qty * product_unit_price)
           total_order_price,                      --> order_payments (derived measure)
           avg_review_score,                       --> order_reviews (derived measure)
          )
```

***Possible Project Extension:*** Additionally, we could also have a data mart for Accounting department with Payments as the FACT table and a data mart for Marketing department with Reviews as the FACT table.

## ETL Pipeline

Pciture here UML before section and Pipeline this  setion

#### Data Quality Checks

Data quality checks are performed on both EMR Spark transformed (staging) data as well as the after moving the data to Redshift tables. The fact and dimension tables are checked to ensure they are not empty. The row count can be seen in the airflow logs as shown below.

*********************here image*

## Requirements

1. Ubuntu 16.04.7 LTS

2. Python 3.6 or later

3. AWS account

4. AWS CLI and Boto 3

5. PostgreSQL

6. Airflow 2.0
   
   Note:- PostgreSQL is needed to serve as backend for Airflow when `[core] executor = LocalExecutor` in`AIRFLOW_HOME/airflow.cfg`. The `LocalExecutor` mode allows Airflow to run multiple tasks simultaneously.

## Instructions

1. Start AWS Redshift cluster and note the cluster details

2. Fill the `config.cfg` file with appropriate details. For example,
   
   
   
   ```roboconf
   [S3]
   ROOT_BUCKET=my-udacity-capstone

   RAW_DATA_KEY=raw_data
   STAGING_DATA_KEY=staging_data
   SPARK_SCRIPTS_KEY=spark_scripts
   
   
   [REDSHIFT]
   HOST='olist-cluster.cctnumob5jbt.us-west-2.redshift.amazonaws.com'  
   DB_NAME='olistdb'  
   DB_USERNAME='awsuser'  
   DB_PASSWORD='Jaipara_2402'  
   DB_PORT=5439
   ```

3. Set `PROJECT_BASE` environment variable. Place all the repository files in the `PROJECT_BASE` directory.
   
   ```shell
   root@0eb606276dd1:/home/workspace# export PROJECT_BASE=/home/workspace
   root@0eb606276dd1:/home/workspace# echo $PROJECT_BASE
   /home/workspace
   root@0eb606276dd1:/home/workspace# ls -hal
   total 128K
   drwxr-xr-x 6 root root 4.0K Jan 16 23:54 .
   drwxr-xr-x 1 root root 4.0K Jan 16 22:02 ..
   drwxr-xr-x 6 root root 4.0K Jan 16 23:51 airflow
   -rw-r--r-- 1 root root 288 Jan 16 23:54 config.cfg
   -rw-r--r-- 1 root root 2.6K Jan 16 21:52 configure_airflow.py
   -rw-r--r-- 1 root root 84K Jan 16 20:45 eda.ipynb
   drwxr-xr-x 2 root root 4.0K Jan 16 21:57 .ipynb_checkpoints
   -rwxrwxrwx 1 root root 3.8K Jan 16 23:54 setup.sh
   drwxr-xr-x 3 root root 4.0K Jan 16 07:28 spark_scripts
   -rwxrwxrwx 1 root root 468 Jan 16 08:07 teardown.sh
   drwxr-xr-x 4 root root 4.0K Jan 16 08:07 utils
   ```

4. Run `./setup.sh`. The script does the following:
   
   - Install AWS CLI v2
   
   - Add custom AWS EMR and Redshift commands to launch and terminate clusters from the terminal
   
   - Install Airflow v2.0 and set Airflow variables and connections
   
   - Install nano editor
   
   `Note:- The Udacity provided project workspace had a few stale components like AWS CLI v1 which had conflicting dependencies with Airflow v2. The `setup.sh` was developed to make sure Airflow 2.0 installs properly on the Udacity provided workspace. Feel free to remove undesired sections according to your requirement from `setup.sh` before running.`

5. Make sure that AWS is configured with your `AWS Access Key ID` , `AWS Secret Access Key` and `Default region`

6. Go to Airflow UI and Unpause the `OLIST_ETL` DAG to start execution

## TO DO

- Propose how often the data should be updated and why.
- Post your write-up and final data model in a GitHub repo.
- Include a description of how you would approach the problem differently under the following scenarios:
  - If the data was increased by 100x.
  - If the pipelines were run on a daily basis by 7am.
  - If the database needed to be accessed by 100+ people.


