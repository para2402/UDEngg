first_set PROJECT_WORKSPACE variable 
run ./setup.sh
run aws configure





# Sales Data Warehouse Schema
***Data Warehouse/ Mart Scope:*** For Sales analysis targeted to serve the sales deparment.  

***Granularity/ Level of Detail of the Data Warehouse:*** Daily  

***Kinds of questions to be answered by the warehouse:***  
- No. of orders received in each month/ year
- Avg total of orders received in each day/ month/ year
- Weekly/ Monthly/ yearly Avg spendings of each customer
- Weekly/ Monthly/ yearly Avg sales of each product type
- Weekly/ Monthly/ yearly Avg sales made by each seller
- Weekly/ Monthly/ yearly city/ state wise sales 

***Possible Project Extension:*** Additionally we could also have a mart for Accounting (fact-payments) and Marketing (fact-reviews) departments  

***Type of dimensional model:*** STAR SCHEMA

## DIMENSIONS:

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

## FACT Table:

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