#----------------------------------------------------------------------------------------------------------------#
# CREATE & SELECT DATABASE
create database olist;
use olist;

#----------------------------------------------------------------------------------------------------------------#

# CREATE TABLE
drop table if exists orders;
CREATE TABLE orders
(
	order_id VARCHAR(100) PRIMARY KEY not null,
	customer_id VARCHAR(100) default null,
	order_status VARCHAR(50) default null,
	order_purchase_timestamp TIMESTAMP default null,
	order_approved_at TIMESTAMP default null,
	order_delivered_carrier_date TIMESTAMP default null,
	order_delivered_customer_date TIMESTAMP default null,
	order_estimated_delivery_date TIMESTAMP default null
);

# LOAD DATA FROM CSV FILE INTO TABLE
LOAD DATA INFILE 'H:/Project/Dataset/olist_orders_dataset.csv'
INTO TABLE orders
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(
	order_id, 
    customer_id, 
	@vorder_status, 
    order_purchase_timestamp, 
    @vorder_approved_at, 
    @vorder_delivered_carrier_date, 
    @vorder_delivered_customer_date, 
    @vorder_estimated_delivery_date
)
SET
	order_status = NULLIF(@vorder_status,''),
	order_delivered_customer_date = NULLIF(@vorder_delivered_customer_date,''),
	order_estimated_delivery_date = NULLIF(@vorder_estimated_delivery_date,''); 

# REMOVE UNWANTED COLUMNS
ALTER TABLE orders
DROP COLUMN order_approved_at,
DROP COLUMN order_delivered_carrier_date;

Select count(order_id) from orders;


#----------------------------------------------------------------------------------------------------------------#

# CREATE TABLE
drop table if exists order_items;
CREATE TABLE order_items 
(
	order_id varchar(100) not null,
	order_item_id varchar (50) default null,
	product_id varchar (100),
	seller_id varchar (100) default null,
	shipping_limit_date timestamp default null,
	price float default null,
	freight_value float default null
);

# LOAD DATA FROM CSV FILE INTO TABLE
LOAD DATA INFILE 'H:/Project/Dataset/olist_order_items_dataset.csv'
INTO TABLE order_items
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(
	order_id,
    @vorder_item_id,
    @vproduct_id,
    @vseller_id,
    @vshipping_limit_date,
    @vprice,
    @vfreight_value
)
SET
	product_id = IFNULL(@vproduct_id, ''),
	price = IFNULL(@vprice, NULL);

# REMOVE UNWANTED COLUMNS
ALTER TABLE order_items
DROP COLUMN order_item_id,
DROP COLUMN seller_id,
DROP COLUMN shipping_limit_date,
DROP COLUMN freight_value;

SELECT COUNT(*) from order_items;

#----------------------------------------------------------------------------------------------------------------#

# CREATE TABLE
drop table if exists payments;	
CREATE TABLE payments 
(
	order_id varchar(100), 
	payment_sequential int default null, 
	payment_type varchar (50), 
	payment_installments int default null, 
	payment_value float
);

# LOAD DATA FROM CSV FILE INTO TABLE
LOAD DATA INFILE 'H:/Project/Dataset/olist_order_payments_dataset.csv'
INTO TABLE payments
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(
	order_id, 
    @vpayment_sequential, 
    @vpayment_type, 
    @vpayment_installments, 
    @vpayment_value
)
SET
	payment_type = IFNULL(@vpayment_type, ''),
	payment_value = IFNULL(@vpayment_value, null);

# REMOVE UNWANTED COLUMNS
ALTER TABLE payments
DROP COLUMN payment_sequential,
DROP COLUMN payment_installments;

SELECT COUNT(*) from payments;
SELECT 
	ROUND(SUM(payment_value)) as "Total Payment Value",
    ROUND(AVG(payment_value),2) as "Avg Payment Value"
FROM payments;

#----------------------------------------------------------------------------------------------------------------#

# CREATE TABLE
drop table if exists customers;
CREATE TABLE customers
(
	customer_id varchar (100) NOT NULL PRIMARY KEY,
	customer_unique_id varchar(100) default null,
	customer_zip_code int default null,
	customer_city varchar (100) default null,
	customer_state varchar (50) default null
);

# LOAD DATA FROM CSV FILE INTO TABLE
LOAD DATA INFILE 'H:/Project/Dataset/olist_customers_dataset.csv'
INTO TABLE customers
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(
	customer_id ,
	@vcustomer_unique_id ,
	@vcustomer_zip_code,
	@vcustomer_city,
	@vcustomer_state
)
SET
	customer_city = IFNULL(@vcustomer_city, NULL)
;

# REMOVE UNWANTED COLUMNS
ALTER TABLE customers
DROP COLUMN customer_unique_id,
DROP COLUMN customer_zip_code,
DROP COLUMN customer_state;

SELECT COUNT(*) from customers;

#----------------------------------------------------------------------------------------------------------------#

# CREATE TABLE
drop table if exists reviews;
CREATE TABLE reviews
(
	review_id varchar (100) default null,
	order_id varchar (100), 
	review_score int, 
	review_comment_title varchar (1000) default null, 
	review_comment_message varchar (1000) default null, 
	review_creation_date timestamp default null, 
	review_answer_timestamp timestamp default null
);

# LOAD DATA FROM CSV FILE INTO TABLE
LOAD DATA INFILE 'H:/Project/Dataset/olist_order_reviews_dataset.csv'
INTO TABLE reviews
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(
	@vreview_id, 
    @vorder_id, 
    @vreview_score, 
    @vreview_comment_title, 
    @vreview_comment_message, 
    @vreview_creation_date, 
    @vreview_answer_timestamp
)
SET
	order_id = IFNULL(@vorder_id, ''),
	review_score = IFNULL(@vreview_score, null);

# REMOVE UNWANTED COLUMNS
ALTER TABLE reviews 
DROP COLUMN review_answer_timestamp,
DROP COLUMN review_creation_date,
DROP COLUMN review_comment_message,
DROP COLUMN review_comment_title,
DROP COLUMN review_id;

SELECT COUNT(*) from reviews;

#----------------------------------------------------------------------------------------------------------------#

# CREATE TABLE
drop table if exists products;
CREATE TABLE products 
(
	product_id varchar(100) NOT NULL PRIMARY KEY,
	product_category_name VARCHAR(500) default NULL,
	product_name_length int default NULL,
	product_description_length int default NULL,
	product_photos_qty int default NULL,
	product_weight_g int default NULL,
	product_length_cm int default NULL,
	product_height_cm int default NULL,
	product_width_cm int default NULL
);

# LOAD DATA FROM CSV FILE INTO TABLE
LOAD DATA INFILE 'H:/Project/Dataset/olist_products_dataset.csv'
INTO TABLE products
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(
	product_id, 
	@vproduct_category_name, 
    @vproduct_name_length, 
    @vproduct_description_length, 
    @vproduct_photos_qty, 
    @vproduct_weight_g, 
    @vproduct_length_cm, 
    @vproduct_height_cm, 
    @vproduct_width_cm
)
SET product_category_name = IFNULL(@vproduct_category_name, NULL);
	
# REMOVE UNWANTED COLUMNS
ALTER TABLE products 
DROP COLUMN product_name_length,
DROP COLUMN product_description_length,
DROP COLUMN product_photos_qty,
DROP COLUMN product_weight_g,
DROP COLUMN product_length_cm,
DROP COLUMN product_height_cm,
DROP COLUMN product_width_cm;

SELECT COUNT(*) from products;

#----------------------------------------------------------------------------------------------------------------#


SELECT
	COUNT(DISTINCT order_id) AS "Total Orders",
    ROUND(SUM(payment_value)) AS "Total Payment Value",
    ROUND(AVG(payment_value), 2) AS "Avg Payment Value",
	ROUND(AVG(DATEDIFF(order_delivered_customer_date, order_purchase_timestamp)),2) as "Avg Delivery Days"
FROM
	orders LEFT JOIN payments USING(order_id);


#--------------------------------------------------KPI_1-----------------------------------------------------------#


DROP VIEW IF EXISTS KPI_1;

CREATE VIEW KPI_1 AS

	SELECT
		CASE WHEN DAYOFWEEK(order_purchase_timestamp) IN (1, 7) THEN 'Weekend' ELSE 'Weekday' END AS Day_Type,
		COUNT(DISTINCT orders.order_id) AS "Total Orders",
		ROUND(SUM(payment_value)) AS "Total Payment Value"
	FROM 
		orders LEFT JOIN payments USING(order_id)
	GROUP BY Day_Type
	union
	SELECT
		"Total =",
		COUNT(DISTINCT order_id),
		ROUND(SUM(payment_value))
	FROM
		orders LEFT JOIN payments USING(order_id);

#--------------------------------------------------KPI_2-----------------------------------------------------------#

DROP VIEW IF EXISTS KPI_2;

CREATE VIEW KPI_2 AS

	SELECT
		COUNT(DISTINCT orders.order_id) as "Orders with payment_type as credit_card & review_score = 5"
	FROM
		orders LEFT JOIN payments USING(order_id) LEFT JOIN reviews USING(order_id)
	WHERE
		payments.payment_type = "credit_card" AND reviews.review_score = 5;
    

#--------------------------------------------------KPI_3-----------------------------------------------------------#


DROP VIEW IF EXISTS KPI_3;

CREATE VIEW KPI_3 AS

	SELECT
		ROUND(AVG(DATEDIFF(orders.order_delivered_customer_date, orders.order_purchase_timestamp)),2) as "Avg Delivery Days for Pet Shop"
	FROM
		orders LEFT JOIN order_items USING(order_id) LEFT JOIN products USING(product_id)
	WHERE
		products.product_category_name = "pet_shop";
    

#--------------------------------------------------KPI_4-----------------------------------------------------------#


DROP VIEW IF EXISTS KPI_4;

CREATE VIEW KPI_4 AS

	WITH product_price AS
	(
		SELECT
			ROUND(AVG(price), 2) AS avg_price
		FROM
			products JOIN order_items USING(product_id) JOIN orders USING(order_id) JOIN customers USING(customer_id)
		WHERE
			customers.customer_city = "sao paulo"
	)
	SELECT
		(SELECT avg_price FROM product_price) AS "Avg Price for Sao Paulo City",
		ROUND(AVG(payment_value), 2) AS "Avg Payment Value for Sao Paulo City"
	FROM
		payments JOIN orders USING(order_id) JOIN customers USING(customer_id)
	WHERE
		customers.customer_city = "sao paulo";
    
    
#--------------------------------------------------KPI_5-----------------------------------------------------------#


DROP VIEW IF EXISTS KPI_5;

CREATE VIEW KPI_5 AS

	SELECT
		DISTINCT review_score as "Review Score",
		ROUND(AVG(DATEDIFF(order_delivered_customer_date, order_purchase_timestamp))) as "Avg Delivery Days"
	FROM
		reviews JOIN orders USING(order_id)
	GROUP BY review_score
	ORDER BY review_score;
    

#----------------------------------------------------------------------------------------------------------------#
#----------------------------------------------------------------------------------------------------------------#
#----------------------------------------------------------------------------------------------------------------#


#--------------------------------------------------KPI_4_SP-----------------------------------------------------------#


DROP FUNCTION IF EXISTS avg_price_for_city;
DELIMITER //
CREATE FUNCTION avg_price_for_city(city_name VARCHAR(100)) RETURNS FLOAT DETERMINISTIC
BEGIN
	DECLARE avg_price FLOAT;
    
	SELECT
		ROUND(AVG(price), 2) INTO avg_price
	FROM
		products JOIN order_items USING(product_id) JOIN orders USING(order_id) JOIN customers USING(customer_id)
	WHERE
		customers.customer_city = city_name;
        
	return avg_price;
END //
DELIMITER ;

DROP VIEW IF EXISTS KPI_4_SP;
CREATE VIEW KPI_4_SP AS
	SELECT
		avg_price_for_city("sao paulo") AS "Avg Price for Sao Paulo City",
		ROUND(AVG(payment_value), 2) AS "Avg Payment Value for Sao Paulo City"
	FROM
		payments JOIN orders USING(order_id) JOIN customers USING(customer_id)
	WHERE
		customers.customer_city = "sao paulo";