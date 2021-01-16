CREATE TABLE IF NOT EXISTS public.customer (
  customer_id			VARCHAR PRIMARY KEY,
  customer_unique_id 		VARCHAR,
  customer_zip_code_prefix	INT,
  customer_city			VARCHAR,
  customer_state		CHAR(2)
);

CREATE TABLE IF NOT EXISTS public.seller (
  seller_id			VARCHAR PRIMARY KEY,
  seller_zip_code_prefix	INT,
  seller_city			VARCHAR,
  seller_state			CHAR(2)
);

CREATE TABLE IF NOT EXISTS public.product(
  product_id			VARCHAR PRIMARY KEY,
  product_category		VARCHAR,
  product_description_length	SMALLINT,
  product_photos_qty		SMALLINT,
  product_weight_g		SMALLINT,
  product_length_cm		SMALLINT,
  product_height_cm		SMALLINT,
  product_width_cm		SMALLINT
);

CREATE TABLE IF NOT EXISTS public.calender(
  date		DATE PRIMARY KEY,
  year		SMALLINT,
  month		SMALLINT,
  day		SMALLINT,
  day_of_week	SMALLINT
);

CREATE TABLE IF NOT EXISTS public.order_item(
  order_item_id				BIGINT NOT NULL,
  order_id				VARCHAR,
  customer_id				VARCHAR,
  product_id				VARCHAR,
  seller_id				VARCHAR,
  purchase_date				DATE,
  purchase_year				SMALLINT,
  purchase_month			SMALLINT,
  order_purchase_timestamp		TIMESTAMP,
  order_approved_timestamp		TIMESTAMP,
  order_delivered_carrier_timestamp	TIMESTAMP,
  order_delivered_customer_timestamp	TIMESTAMP,
  order_estimated_delivery_timestamp	TIMESTAMP,
  order_status				VARCHAR,
  shipping_limit_date			TIMESTAMP,
  freight_value				DECIMAL(5, 2),
  unit_price				DECIMAL(10, 2),
  qty					SMALLINT,
  total_product_price			DECIMAL(10, 2),
  total_order_price			DECIMAL(10, 2),
  avg_review_score			DECIMAL(2, 1)
)
diststyle key distkey (purchase_year);