/*
Based on - Getting Started with Snowflake Dynamic Tables
https://quickstarts.snowflake.com/guide/getting_started_with_dynamic_tables/#0
Includes:
Sample data setup - create tables with python faker scripts
Build data pipeline using Dynamic Tables and SQL
Python based transformations
Data validation with dynamic tables
Monitoring dynamic tables - email alert
*/

CREATE or replace DATABASE  DEMO_DYNAMIC_TABLES;
CREATE SCHEMA DT_DEMO;
USE DATABASE DEMO_DYNAMIC_TABLES;
USE SCHEMA DT_DEMO;
USE WAREHOUSE XS_WH;

--------------------------------------
-- Create_table_CUST_INFO
--------------------------------------
create or replace function gen_cust_info(num_records number)
returns table (custid number(10), cname varchar(100), spendlimit number(10,2))
language python
runtime_version=3.8
handler='CustTab'
packages = ('Faker')
as $$
from faker import Faker
import random

fake = Faker()
# Generate a list of customers  

class CustTab:
    # Generate multiple customer records
    def process(self, num_records):
        customer_id = 1000 # Starting customer ID                 
        for _ in range(num_records):
            custid = customer_id + 1
            cname = fake.name()
            spendlimit = round(random.uniform(1000, 10000),2)
            customer_id += 1
            yield (custid,cname,spendlimit)

$$;

-- Create table and insert records 
create or replace table cust_info as select * from table(gen_cust_info(1000)) order by 1;



--------------------------------------
-- Create_table_PROD_STOCK_INV
--------------------------------------
create or replace function  gen_prod_inv(num_records number)
returns table (pid number(10), pname varchar(100), stock number(10,2), stockdate date)
language python
runtime_version=3.8
handler='ProdTab'
packages = ('Faker')
as $$
from faker import Faker
import random
from datetime import datetime, timedelta
fake = Faker()

class ProdTab:
    # Generate multiple product records
    def process(self, num_records):
        product_id = 100 # Starting customer ID                 
        for _ in range(num_records):
            pid = product_id + 1
            pname = fake.catch_phrase()
            stock = round(random.uniform(500, 1000),0)
            # Get the current date
            current_date = datetime.now()
            
            # Calculate the maximum date (3 months from now)
            min_date = current_date - timedelta(days=90)
            
            # Generate a random date within the date range
            stockdate = fake.date_between_dates(min_date,current_date)

            product_id += 1
            yield (pid,pname,stock,stockdate)

$$;

-- Create table and insert records 
create or replace table prod_stock_inv as select * from table(gen_prod_inv(100)) order by 1;

--------------------------------------
-- Create_table_SALESDATA
--------------------------------------
create or replace function gen_cust_purchase(num_records number,ndays number)
returns table (custid number(10), purchase variant)
language python
runtime_version=3.8
handler='genCustPurchase'
packages = ('Faker')
as $$
from faker import Faker
import random
from datetime import datetime, timedelta

fake = Faker()

class genCustPurchase:
    # Generate multiple customer purchase records
    def process(self, num_records,ndays):       
        for _ in range(num_records):
            c_id = fake.random_int(min=1001, max=1999)
            
            #print(c_id)
            customer_purchase = {
                'custid': c_id,
                'purchased': []
            }
            # Get the current date
            current_date = datetime.now()
            
            # Calculate the maximum date (days from now)
            min_date = current_date - timedelta(days=ndays)
            
            # Generate a random date within the date range
            pdate = fake.date_between_dates(min_date,current_date)
            
            purchase = {
                'prodid': fake.random_int(min=101, max=199),
                'quantity': fake.random_int(min=1, max=5),
                'purchase_amount': round(random.uniform(10, 1000),2),
                'purchase_date': pdate
            }
            customer_purchase['purchased'].append(purchase)
            
            #customer_purchases.append(customer_purchase)
            yield (c_id,purchase)

$$;

-- Create table and insert records 
create or replace table salesdata as select * from table(gen_cust_purchase(10000,10));

--------------------------------------
-- Sanity Checks
--------------------------------------
-- customer information table, each customer has spending limits
select * from cust_info limit 10;

-- product stock table, each product has stock level from fulfilment day
select * from prod_stock_inv limit 10;

-- sales data for products purchsaed online by various customers
select * from salesdata limit 10;

--------------------------------------
--------------------------------------
--------------------------------------
----------------------------------------------------------------------------


----------------------------------------------------------------------------
-- Create Dynamic Table - CUSTOMER_SALES_DATA_HISTORY 
----------------------------------------------------------------------------

CREATE OR REPLACE DYNAMIC TABLE customer_sales_data_history
    LAG='DOWNSTREAM'
    WAREHOUSE=XS_WH
AS
select 
    s.custid as customer_id,
    c.cname as customer_name,
    s.purchase:"prodid"::number(5) as product_id,
    s.purchase:"purchase_amount"::number(10) as saleprice,
    s.purchase:"quantity"::number(5) as quantity,
    s.purchase:"purchase_date"::date as salesdate
from
    cust_info c inner join salesdata s on c.custid = s.custid;



-- quick sanity checks
select * from customer_sales_data_history limit 10;
select count(*) from customer_sales_data_history;



----------------------------------------------------------------------------
-- Create Dynamic Table - SALESREPORT
----------------------------------------------------------------------------

CREATE OR REPLACE DYNAMIC TABLE salesreport
    LAG = '1 MINUTE'
    WAREHOUSE=XS_WH
AS
    Select
        t1.customer_id,
        t1.customer_name, 
        t1.product_id,
        p.pname as product_name,
        t1.saleprice,
        t1.quantity,
        (t1.saleprice/t1.quantity) as unitsalesprice,
        t1.salesdate as CreationTime,
        customer_id || '-' || t1.product_id  || '-' || t1.salesdate AS CUSTOMER_SK,
        LEAD(CreationTime) OVER (PARTITION BY t1.customer_id ORDER BY CreationTime ASC) AS END_TIME
    from 
        customer_sales_data_history t1 inner join prod_stock_inv p 
        on t1.product_id = p.pid ;

-- quick sanity checks
select * from salesreport limit 10;
select count(*) from salesreport;


----------------------------------------------------------------------------
-- TEST - Let's test this DAG by adding some raw data in the base tables.
----------------------------------------------------------------------------

-- Add new records
insert into salesdata select * from table(gen_cust_purchase(10000,2));

-- Check raw base table
select count(*) from salesdata;


-- Check Dynamic Tables after a minute
select count(*) from customer_sales_data_history;
select count(*) from salesreport;


-- You can check the Dynamic table graph and refresh history from the Snowsight. We will learn more about this dashboard later in its own section


--------------------------------------------------------
-- 4. Use case: Using Snowpark UDTF in Dynamic table
--------------------------------------------------------

USE DATABASE DEMO_DYNAMIC_TABLES;
USE SCHEMA DT_DEMO;

CREATE OR REPLACE FUNCTION sum_table (INPUT_NUMBER number)
  returns TABLE (running_total number)
  language python
  runtime_version = '3.8'
  handler = 'gen_sum_table'
as
$$

# Define handler class
class gen_sum_table :

  ## Define __init__ method ro initilize the variable
  def __init__(self) :    
    self._running_sum = 0
  
  ## Define process method
  def process(self, input_number: float) :
    # Increment running sum with data from the input row
    new_total = self._running_sum + input_number
    self._running_sum = new_total

    yield(new_total,)
  
$$
;



CREATE OR REPLACE DYNAMIC TABLE cumulative_purchase
    LAG = '1 MINUTE'
    WAREHOUSE=XS_WH
AS
    select 
        month(creationtime) monthNum,
        year(creationtime) yearNum,
        customer_id, 
        saleprice,
        running_total 
    from 
        salesreport,
        table(sum_table(saleprice) over (partition by creationtime,customer_id order by creationtime, customer_id))  ;


select * from  cumulative_purchase limit 10;


--------------------------------------------------------
-- 5. Use case: Data validation using Dynamic table
--------------------------------------------------------

CREATE OR REPLACE DYNAMIC TABLE PROD_INV_ALERT
    LAG = '1 MINUTE'
    WAREHOUSE=XS_WH
AS
    SELECT 
        S.PRODUCT_ID, 
        S.PRODUCT_NAME,CREATIONTIME AS LATEST_SALES_DATE,
        STOCK AS BEGINING_STOCK,
        SUM(S.QUANTITY) OVER (PARTITION BY S.PRODUCT_ID ORDER BY CREATIONTIME) TOTALUNITSOLD, 
        (STOCK - TOTALUNITSOLD) AS UNITSLEFT,
        ROUND(((STOCK-TOTALUNITSOLD)/STOCK) *100,2) PERCENT_UNITLEFT,
        CURRENT_TIMESTAMP() AS ROWCREATIONTIME
    FROM SALESREPORT S JOIN PROD_STOCK_INV ON PRODUCT_ID = PID
    QUALIFY ROW_NUMBER() OVER (PARTITION BY PRODUCT_ID ORDER BY CREATIONTIME DESC) = 1
;


-- check products with low inventory and alert
select * from prod_inv_alert where percent_unitleft < 10;


---------------------------------------------------------------------------------------
--  Alerts - can help you send email alerts to your product procurement and inventory 
--     team to restock the required product. Remember to update the email address and
--     warehouse in the below code.
---------------------------------------------------------------------------------------






CREATE NOTIFICATION INTEGRATION IF NOT EXISTS
    notification_emailer
    TYPE=EMAIL
    ENABLED=TRUE
    ALLOWED_RECIPIENTS=('ralph.braun@snowflake.com')
    COMMENT = 'email integration to update on low product inventory levels'
;

CREATE OR REPLACE ALERT alert_low_inv
  WAREHOUSE = XS_WH
  SCHEDULE = '30 MINUTE'
  IF (EXISTS (
      SELECT *
      FROM prod_inv_alert
      WHERE percent_unitleft < 10 and ROWCREATIONTIME > SNOWFLAKE.ALERT.LAST_SUCCESSFUL_SCHEDULED_TIME()
  ))
  THEN CALL SYSTEM$SEND_EMAIL(
                'notification_emailer', -- notification integration to use
                'ralph.braun@snowflake.com', -- Email
                'Email Alert: Low Inventory of products', -- Subject
                'Inventory running low for certain products. Please check the inventory report in Snowflake table prod_inv_alert' -- Body of email
);

-- Alerts are pause by default, so let's resume it first
ALTER ALERT alert_low_inv RESUME;

-- Add new records
insert into salesdata select * from table(gen_cust_purchase(10000,2));

-- Monitor alerts in detail
SHOW ALERTS; 

SELECT *
FROM
  TABLE(INFORMATION_SCHEMA.ALERT_HISTORY(
    SCHEDULED_TIME_RANGE_START
      =>dateadd('hour',-1,current_timestamp())))
WHERE
    NAME = 'ALERT_LOW_INV'
ORDER BY SCHEDULED_TIME DESC;



SELECT * 
FROM 
    TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY())
WHERE 
    NAME IN ('SALESREPORT','CUSTOMER_SALES_DATA_HISTORY','PROD_INV_ALERT','CUMULATIVE_PURCHASE')
    -- AND REFRESH_ACTION != 'NO_DATA'
ORDER BY 
    DATA_TIMESTAMP DESC, REFRESH_END_TIME DESC LIMIT 10;


-- Suspend Alerts 
-- Important step to suspend alert and stop consuming the warehouse credit
ALTER ALERT alert_low_inv SUSPEND;

    
-----------------------------------------
-- SUSPEND and RESUME Dynamic Tables
-----------------------------------------

-- Resume the data pipeline
alter dynamic table customer_sales_data_history RESUME;
alter dynamic table salesreport RESUME;
alter dynamic table prod_inv_alert RESUME;

-- Suspend the data pipeline
alter dynamic table customer_sales_data_history SUSPEND;
alter dynamic table salesreport SUSPEND;
alter dynamic table prod_inv_alert SUSPEND;

    
