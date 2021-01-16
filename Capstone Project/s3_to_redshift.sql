COPY staging_customer
FROM 's3://jaipara-udacity-capstone/staging_data/customer/'
IAM_ROLE 'arn:aws:iam::608432393717:role/dwhRole'
FORMAT AS PARQUET;


COPY staging_seller
FROM 's3://jaipara-udacity-capstone/staging_data/seller/'
IAM_ROLE 'arn:aws:iam::608432393717:role/dwhRole'
FORMAT AS PARQUET;


COPY staging_product
FROM 's3://jaipara-udacity-capstone/staging_data/product/'
IAM_ROLE 'arn:aws:iam::608432393717:role/dwhRole'
FORMAT AS PARQUET;


COPY staging_calender
FROM 's3://jaipara-udacity-capstone/staging_data/calender/'
IAM_ROLE 'arn:aws:iam::608432393717:role/dwhRole'
FORMAT AS PARQUET;


COPY staging_order_item
FROM 's3://jaipara-udacity-capstone/staging_data/order_item/'
IAM_ROLE 'arn:aws:iam::608432393717:role/dwhRole'
FORMAT AS PARQUET;