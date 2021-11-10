-- Databricks notebook source
create database if not exists retailer_db

-- COMMAND ----------

create table if not exists retailer_db.customer(c_customer_sk LONG comment "this is the primary key",c_customer_id STRING,c_current_cdemo_sk LONG,c_current_hdemo_sk LONG,c_current_addr_sk LONG,c_first_shipto_date_sk LONG,c_first_sales_date_sk LONG,c_salutation STRING,c_first_name STRING,c_last_name STRING,c_preferred_cust_flag STRING,c_birth_day INT,c_birth_month INT,c_birth_year INT,c_birth_country STRING,c_login STRING,c_email_address STRING,c_last_review_date LONG)
using csv
options(
path '/FileStore/tables/retailer/customer.dat',
sep '|',
header true
)

-- COMMAND ----------

create table if not exists retailer_db.customer_address(ca_address_sk LONG,ca_address_id STRING,ca_street_number STRING,ca_street_name STRING,ca_street_type STRING,ca_suite_number STRING,ca_city STRING,ca_county STRING,ca_state STRING,ca_zip STRING,ca_country STRING,ca_gmt_offset decimal(5,2),ca_location_type STRING)
using csv
options(
path '/FileStore/tables/retailer/customer_address.dat',
sep '|',
header true
)

-- COMMAND ----------

create table if not exists retailer_db.customer_demographics(cd_demo_sk LONG,cd_gender STRING,cd_marital_status STRING,cd_education_status STRING,cd_purchase_estimate INT,cd_credit_rating STRING,cd_dep_count INT,cd_dep_employed_count INT,cd_dep_college_count INT)
using csv
options(
path '/FileStore/tables/retailer/customer_demographics.dat',
sep '|',
header true
)

-- COMMAND ----------

create table if not exists retailer_db.household_demographics(hd_demo_sk LONG,hd_income_band_sk LONG,hd_buy_potential STRING,hd_dep_count INT,hd_vehicle_count INT)
using csv
options(
path '/FileStore/tables/retailer/household_demographics.dat',
sep '|',
header true
)

-- COMMAND ----------

create table if not exists retailer_db.income_band(ib_income_band_sk LONG,ib_lower_bound INT,ib_upper_bound INT)
using csv
options(
path '/FileStore/tables/retailer/income_band.dat',
sep '|',
header true
)

-- COMMAND ----------

create table if not exists retailer_db.store_sales(ss_sold_date_sk LONG,ss_sold_time_sk LONG,ss_item_sk LONG,ss_customer_sk LONG,ss_cdemo_sk LONG,ss_hdemo_sk LONG,ss_addr_sk LONG,ss_store_sk LONG,ss_promo_sk LONG,ss_ticket_number LONG,ss_quantity INT,ss_wholesale_cost decimal(7,2),ss_list_price decimal(7,2),ss_sales_price decimal(7,2),ss_ext_discount_amt decimal(7,2),ss_ext_sales_price decimal(7,2),ss_ext_wholesale_cost decimal(7,2),ss_ext_list_price decimal(7,2),ss_ext_tax decimal(7,2),ss_coupon_amt decimal(7,2),ss_net_paid decimal(7,2),ss_net_paid_inc_tax decimal(7,2),ss_net_profit decimal(7,2))
using csv
options(
path '/FileStore/tables/retailer/store_sales.dat',
sep '|',
header true
)

-- COMMAND ----------

create table if not exists retailer_db.store_returns(sr_returned_date_sk LONG,sr_return_time_sk LONG,sr_item_sk LONG,sr_customer_sk LONG,sr_cdemo_sk LONG,sr_hdemo_sk LONG,sr_addr_sk LONG,sr_store_sk LONG,sr_reason_sk LONG,sr_ticket_number LONG,sr_return_quantity INT,sr_return_amt decimal(7,2),sr_return_tax decimal(7,2),sr_return_amt_inc_tax decimal(7,2),sr_fee decimal(7,2),sr_return_ship_cost decimal(7,2),sr_refunded_cash decimal(7,2),sr_reversed_charge decimal(7,2),sr_store_credit decimal(7,2),sr_net_loss decimal(7,2))
using csv
options(
path '/FileStore/tables/retailer/store_returns.dat',
sep '|',
header true
)

-- COMMAND ----------

create table if not exists retailer_db.catalog_sales(cs_sold_date_sk LONG,cs_sold_time_sk LONG,cs_ship_date_sk LONG,cs_bill_customer_sk LONG,cs_bill_cdemo_sk LONG,cs_bill_hdemo_sk LONG,cs_bill_addr_sk LONG,cs_ship_customer_sk LONG,cs_ship_cdemo_sk LONG,cs_ship_hdemo_sk LONG,cs_ship_addr_sk LONG,cs_call_center_sk LONG,cs_catalog_page_sk LONG,cs_ship_mode_sk LONG,cs_warehouse_sk LONG,cs_item_sk LONG,cs_promo_sk LONG,cs_order_number LONG,cs_quantity INT,cs_wholesale_cost decimal(7,2),cs_list_price decimal(7,2),cs_sales_price decimal(7,2),cs_ext_discount_amt decimal(7,2),cs_ext_sales_price decimal(7,2),cs_ext_wholesale_cost decimal(7,2),cs_ext_list_price decimal(7,2),cs_ext_tax decimal(7,2),cs_coupon_amt decimal(7,2),cs_ext_ship_cost decimal(7,2),cs_net_paid decimal(7,2),cs_net_paid_inc_tax decimal(7,2),cs_net_paid_inc_ship decimal(7,2),cs_net_paid_inc_ship_tax decimal(7,2),cs_net_profit decimal(7,2))
using csv
options(
path '/FileStore/tables/retailer/catalog_sales.dat',
sep '|',
header true
)

-- COMMAND ----------

create table if not exists retailer_db.catalog_returns(cr_returned_date_sk LONG,cr_returned_time_sk LONG,cr_item_sk LONG,cr_refunded_customer_sk LONG,cr_refunded_cdemo_sk LONG,cr_refunded_hdemo_sk LONG,cr_refunded_addr_sk LONG,cr_returning_customer_sk LONG,cr_returning_cdemo_sk LONG,cr_returning_hdemo_sk LONG,cr_returning_addr_sk LONG,cr_call_center_sk LONG,cr_catalog_page_sk LONG,cr_ship_mode_sk LONG,cr_warehouse_sk LONG,cr_reason_sk LONG,cr_order_number LONG,cr_return_quantity INT,cr_return_amount decimal(7,2),cr_return_tax decimal(7,2),cr_return_amt_inc_tax decimal(7,2),cr_fee decimal(7,2),cr_return_ship_cost decimal(7,2),cr_refunded_cash decimal(7,2),cr_reversed_charge decimal(7,2),cr_store_credit decimal(7,2),cr_net_loss decimal(7,2))
using csv
options(
path '/FileStore/tables/retailer/catalog_returns.dat',
sep '|',
header true
)

-- COMMAND ----------

create table if not exists retailer_db.web_sales( ws_sold_date_sk LONG,ws_sold_time_sk LONG,ws_ship_date_sk LONG,ws_item_sk LONG,ws_bill_customer_sk LONG,ws_bill_cdemo_sk LONG,ws_bill_hdemo_sk LONG,ws_bill_addr_sk LONG,ws_ship_customer_sk LONG,ws_ship_cdemo_sk LONG,ws_ship_hdemo_sk LONG,ws_ship_addr_sk LONG,ws_web_page_sk LONG,ws_web_site_sk LONG,ws_ship_mode_sk LONG,ws_warehouse_sk LONG,ws_promo_sk LONG,ws_order_number LONG,ws_quantity INT,ws_wholesale_cost decimal(7,2),ws_list_price decimal(7,2),ws_sales_price decimal(7,2),ws_ext_discount_amt decimal(7,2),ws_ext_sales_price decimal(7,2),ws_ext_wholesale_cost decimal(7,2),ws_ext_list_price decimal(7,2),ws_ext_tax decimal(7,2),ws_coupon_amt decimal(7,2),ws_ext_ship_cost decimal(7,2),ws_net_paid decimal(7,2),ws_net_paid_inc_tax decimal(7,2),ws_net_paid_inc_ship decimal(7,2),ws_net_paid_inc_ship_tax decimal(7,2),ws_net_profit decimal(7,2))
using csv
options(
path '/FileStore/tables/retailer/web_sales.dat',
sep '|',
header true
)

-- COMMAND ----------

create table if not exists retailer_db.web_returns(wr_returned_date_sk LONG,wr_returned_time_sk LONG,wr_item_sk LONG,wr_refunded_customer_sk LONG,wr_refunded_cdemo_sk LONG,wr_refunded_hdemo_sk LONG,wr_refunded_addr_sk LONG,wr_returning_customer_sk LONG,wr_returning_cdemo_sk LONG,wr_returning_hdemo_sk LONG,wr_returning_addr_sk LONG,wr_web_page_sk LONG,wr_reason_sk LONG,wr_order_number LONG,wr_return_quantity INT,wr_return_amt decimal(7,2),wr_return_tax decimal(7,2),wr_return_amt_inc_tax decimal(7,2),wr_fee decimal(7,2),wr_return_ship_cost decimal(7,2),wr_refunded_cash decimal(7,2),wr_reversed_charge decimal(7,2),wr_account_credit decimal(7,2),wr_net_loss decimal(7,2))
using csv
options(
path '/FileStore/tables/retailer/web_returns.dat',
sep '|',
header true
)

-- COMMAND ----------

create table if not exists retailer_db.inventory(inv_date_sk LONG,inv_item_sk LONG,inv_warehouse_sk LONG,inv_quantity_on_hand INT)
using csv
options(
path '/FileStore/tables/retailer/inventory.dat',
sep '|',
header true
)

-- COMMAND ----------

create table if not exists retailer_db.store(s_store_sk LONG,s_store_id STRING,s_rec_start_date date,s_rec_end_date date,s_closed_date_sk LONG,s_store_name STRING,s_number_employees INT,s_floor_space INT,s_hours STRING,s_manager STRING,s_market_id INT,s_geography_class STRING,s_market_desc STRING,s_market_manager STRING,s_division_id INT,s_division_name STRING,s_company_id INT,s_company_name STRING,s_street_number INT,s_street_name STRING,s_street_type STRING,s_suite_number STRING,s_city STRING,s_county STRING,s_state STRING,s_zip DOUBLE,s_country STRING,s_gmt_offset decimal(5,2),s_tax_precentage decimal(5,2))
using csv
options(
path '/FileStore/tables/retailer/store.dat',
sep '|',
header true
)

-- COMMAND ----------

create table if not exists retailer_db.call_center(cc_call_center_sk LONG,cc_call_center_id STRING,cc_rec_start_date date,cc_rec_end_date date,cc_closed_date_sk LONG,cc_open_date_sk LONG,cc_name STRING,cc_class STRING,cc_employees INT,cc_sq_ft INT,cc_hours STRING,cc_manager STRING,cc_mkt_id INT,cc_mkt_class STRING,cc_mkt_desc STRING,cc_market_manager STRING,cc_division INT,cc_division_name STRING,cc_company INT,cc_company_name STRING,cc_street_number STRING,cc_street_name STRING,cc_street_type STRING,cc_suite_number STRING,cc_city STRING,cc_county STRING,cc_state STRING,cc_zip STRING,cc_country STRING,cc_gmt_offset decimal(5,2),cc_tax_percentage decimal(5,2))
using csv
options(
path '/FileStore/tables/retailer/call_center.dat',
sep '|',
header true
)

-- COMMAND ----------

create table if not exists retailer_db.catalog_page(cp_catalog_page_sk LONG,cp_catalog_page_id STRING,cp_start_date_sk LONG,cp_end_date_sk LONG,cp_department STRING,cp_catalog_number INT,cp_catalog_page_number INT,cp_description STRING,cp_type STRING)
using csv
options(
path '/FileStore/tables/retailer/catalog_page.dat',
sep '|',
header true
)

-- COMMAND ----------

create table if not exists retailer_db.web_site(web_site_sk LONG,web_site_id STRING,web_rec_start_date date,web_rec_end_date date,web_name STRING,web_open_date_sk LONG,web_close_date_sk LONG,web_class STRING,web_manager STRING,web_mkt_id INT,web_mkt_class STRING,web_mkt_desc STRING,web_market_manager STRING,web_company_id INT,web_company_name STRING,web_street_number STRING,web_street_name STRING,web_street_type STRING,web_suite_number STRING,web_city STRING,web_county STRING,web_state STRING,web_zip STRING,web_country STRING,web_gmt_offset decimal(5,2),web_tax_percentage decimal(5,2))
using csv
options(
path '/FileStore/tables/retailer/web_site.dat',
sep '|',
header true
)

-- COMMAND ----------

create table if not exists retailer_db.web_page(wp_web_page_sk LONG,wp_web_page_id STRING,wp_rec_start_date date,wp_rec_end_date date,wp_creation_date_sk LONG,wp_access_date_sk LONG,wp_autogen_flag STRING,wp_customer_sk LONG,wp_url STRING,wp_type STRING,wp_char_count INT,wp_link_count INT,wp_image_count INT,wp_max_ad_count INT)
using csv
options(
path '/FileStore/tables/retailer/web_page.dat',
sep '|',
header true
)

-- COMMAND ----------

create table if not exists retailer_db.warehouse(w_warehouse_sk LONG,w_warehouse_id STRING,w_warehouse_name STRING,w_warehouse_sq_ft INT,w_street_number STRING,w_street_name STRING,w_street_type STRING,w_suite_number STRING,w_city STRING,w_county STRING,w_state STRING,w_zip STRING,w_country STRING,w_gmt_offset decimal(5,2))
using csv
options(
path '/FileStore/tables/retailer/warehouse.dat',
sep '|',
header true
)

-- COMMAND ----------

create table if not exists retailer_db.date_dim( d_date_sk LONG,d_date_id STRING,d_date date,d_month_seq INT,d_week_seq INT,d_quarter_seq INT,d_year INT,d_dow INT,d_moy INT,d_dom INT,d_qoy INT,d_fy_year INT,d_fy_quarter_seq INT,d_fy_week_seq INT,d_day_name STRING,d_quarter_name STRING,d_holiday STRING,d_weekend STRING,d_following_holiday STRING,d_first_dom INT,d_last_dom INT,d_same_day_ly INT,d_same_day_lq INT,d_current_day STRING,d_current_week STRING,d_current_month STRING,d_current_quarter STRING,d_current_year STRING)
using csv
options(
path '/FileStore/tables/retailer/date_dim.dat',
sep '|',
header true
)

-- COMMAND ----------

create table if not exists retailer_db.item(i_item_sk LONG,i_item_id STRING,i_rec_start_date date,i_rec_end_date date,i_item_desc STRING,i_current_price decimal(7,2),i_wholesale_cost decimal(7,2),i_brand_id INT,i_brand STRING,i_class_id INT,i_class STRING,i_category_id INT,i_category STRING,i_manufact_id INT,i_manufact STRING,i_size STRING,i_formulation STRING,i_color STRING,i_units STRING,i_container STRING,i_manager_id INT,i_product_name STRING
)
using csv
options(
path '/FileStore/tables/retailer/item.dat',
sep '|',
header true
)

-- COMMAND ----------

create table if not exists retailer_db.promotion(p_promo_sk LONG,p_promo_id STRING,p_start_date_sk LONG,p_end_date_sk LONG,p_item_sk LONG,p_cost decimal(15,2),p_response_target INT,p_promo_name STRING,p_channel_dmail STRING,p_channel_email STRING,p_channel_catalog STRING,p_channel_tv STRING,p_channel_radio STRING,p_channel_press STRING,p_channel_event STRING,p_channel_demo STRING,p_channel_details STRING,p_purpose STRING,p_discount_active STRING)
using csv
options(
path '/FileStore/tables/retailer/promotion.dat',
sep '|',
header true
)

-- COMMAND ----------

create table if not exists retailer_db.reason(r_reason_sk LONG,r_reason_id STRING,r_reason_desc STRING)
using csv
options(
path '/FileStore/tables/retailer/reason.dat',
sep '|',
header true
)

-- COMMAND ----------

create table if not exists retailer_db.ship_mode(sm_ship_mode_sk LONG,sm_ship_mode_id STRING,sm_type STRING,sm_code STRING,sm_carrier STRING,sm_contract STRING)
using csv
options(
path '/FileStore/tables/retailer/ship_mode.dat',
sep '|',
header true
)

-- COMMAND ----------

create table if not exists retailer_db.time_dim(t_time_sk LONG,t_time_id STRING,t_time INT,t_hour INT,t_minute INT,t_second INT,t_am_pm STRING,t_shift STRING,t_sub_shift STRING,t_meal_time STRING)
using csv
options(
path '/FileStore/tables/retailer/time_dim.dat',
sep '|',
header true
)
