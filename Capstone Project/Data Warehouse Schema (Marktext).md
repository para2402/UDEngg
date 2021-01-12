# Data Warehouse Schema

Data Warehouse/ Mart Scope:

For Sales analysis targeted towards the sales deparment. 

Possible Project Extension: Additionally we could also have a mart for Accounting (fact-payments) and Marketing (fact-reviews) departments

Granularity/ Level of Detail of the Data Warehouse: Daily

Kinds of questions to be answered by the warehouse:

- No. of orders received in each month/ year
- Avg total of orders received in each day/ month/ year
- Monthly/ yearly Avg spendings of each customer
- Monthly/ yearly Avg sales of each product type
- Monthly/ yearly Avg sales made by each seller
- Monthly/ yearly Region wise sales

##### Type of dimensional model: STAR SCHEMA

## DIMENSIONS:

```plsql
customer(customer_id, customer_zip_code_prefix,
         customer_city, customer_state)
  seller(seller_id, seller_zip_code_prefix,
         seller_city, seller_state)
 product(product_id,
         product_category_name,    --> Translated using "product_category_name_translation.csv"
         product_name_length,
         product_description_length,
         product_photos_qty,
         product_weight_g,
         product_length_cm,
         product_height_cm,
         product_width_cm)
calender(calender_id, date, day, dayofweek, month, year)
```

## FACT Table:

```sql
order_item(order_item_id[PK],
           product_id, seller_id,        --> order_items
           shipping_limit_date,          --> order_items
           freight_value,                --> order_items
           qty,                          --> order_items (derived measure)
           unit_price,                   --> order_items (rename "price" -> "unit_price")
           total_product_price,          --> (qty * product_unit_price)
           total_order_price,            --> order_payments (derived measure)
           calender_id,                  --> orders (from "order_purchase_timestamp" & calender dim)
           customer_id, order_status,    --> orders
           order_purchase_date,          --> orders (from "order_purchase_timestamp")
           order_approved_date,          --> orders (from "order_approved_at")
           order_delivered_carrier_date, --> orders (EXTRACT DATE from "order_delivered_carrier_date")
           order_delivered_customer_date,--> orders (EXTRACT DATE from "order_delivered_customer_date")
           order_estimated_delivery_date,--> orders (EXTRACT DATE from "order_estimated_delivery_date")
           avg_review_score,             --> order_reviews (derived measure)
 )
```
