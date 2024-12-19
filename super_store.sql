use super_store;
CREATE TABLE orders (
    Row_ID INT,
    Order_ID VARCHAR(20),
    Order_Date varchar(255),
    Ship_Date varchar(255),
    Ship_Mode VARCHAR(20),
    Customer_ID VARCHAR(20),
    Customer_Name VARCHAR(50),
    Segment VARCHAR(20),
    Country VARCHAR(50),
    City VARCHAR(50),
    State VARCHAR(50),
    Postal_Code VARCHAR(10),
    Region VARCHAR(20),
    Product_ID VARCHAR(20),
    Category VARCHAR(20),
    Sub_Category VARCHAR(50),
    Product_Name VARCHAR(255),
    Sales DECIMAL(10,2)
);


LOAD DATA INFILE "C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\super_store.csv"
INTO TABLE orders FIELDS TERMINATED BY ','
LINES TERMINATED BY '/n' IGNORE 1 LINES;
select * from orders;

SET SQL_SAFE_UPDATES = 0;
UPDATE orders
SET Ship_Date = DATE_FORMAT(STR_TO_DATE(Ship_Date, '%d-%m-%Y'), '%Y-%m-%d');




