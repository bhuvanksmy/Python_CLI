use creditcard_capstone;

SELECT 
    distinct cr.CREDIT_CARD_NO, cr.TIMEID, cr.CUST_SSN, cr.BRANCH_CODE,cr.TRANSACTION_ID, cr.TRANSACTION_TYPE, cr.TRANSACTION_VALUE
FROM
    cdw_sapp_credit_card AS cr
        INNER JOIN
    cdw_sapp_customer cu ON cr.CREDIT_CARD_NO = cu.CREDIT_CARD_NO
        AND cr.CUST_SSN = cu.SSN
WHERE
SUBSTRING(TIMEID, 1, 4) = 2018
        AND SUBSTRING(TIMEID, 5, 1) = 5
        AND cu.cust_zip = 19438;


SELECT 
    distinct cr.CREDIT_CARD_NO, cr.TIMEID, cr.CUST_SSN, cr.BRANCH_CODE,cr.TRANSACTION_ID, cr.TRANSACTION_TYPE, cr.TRANSACTION_VALUE
FROM
    cdw_sapp_credit_card AS cr
        INNER JOIN
    cdw_sapp_customer cu ON cr.CREDIT_CARD_NO = cu.CREDIT_CARD_NO
        AND cr.CUST_SSN = cu.SSN
WHERE cu.cust_zip = 19438;

select * from  cdw_sapp_customer where CREDIT_CARD_NO = 4210653349028689;
select SUBSTRING(TIMEID,1,4) year,SUBSTRING(TIMEID,5,1) month,SUBSTRING(TIMEID,6,7) day, TIMEID from  cdw_sapp_credit_card where CREDIT_CARD_NO = 4210653349028689 and year(TIMEID) IS NULL;

SELECT cr.* FROM cdw_sapp_credit_card AS cr INNER JOIN cdw_sapp_customer cu ON cr.CREDIT_CARD_NO = cu.CREDIT_CARD_NO AND cr.CUST_SSN = cu.SSN WHERE SUBSTRING(TIMEID, 1, 4) = 2018 AND SUBSTRING(TIMEID, 5, 1) = 2 AND cu.cust_zip =12345;

SELECT cr.* FROM cdw_sapp_credit_card AS cr INNER JOIN cdw_sapp_customer cu ON 
cr.CREDIT_CARD_NO = cu.CREDIT_CARD_NO AND cr.CUST_SSN = cu.SSN 
WHERE SUBSTRING(TIMEID, 1, 4) = 2018 AND SUBSTRING(TIMEID, 5, 1) = 5 AND 
cu.cust_zip =19438 order by CAST(SUBSTRING(TIMEID, 5, 1) as UNSIGNED),CAST(SUBSTRING(TIMEID, 6, 7) as UNSIGNED),
CAST(SUBSTRING(TIMEID, 1, 4) as UNSIGNED) desc;

select SSN, FIRST_NAME, MIDDLE_NAME, LAST_NAME, CREDIT_CARD_NO, FULL_STREET_ADDRESS, CUST_CITY, CUST_STATE, CUST_COUNTRY, CUST_ZIP, CUST_PHONE, CUST_EMAIL, LAST_UPDATED
 from creditcard_capstone.cdw_sapp_customer ;
 
 select * from creditcard_capstone.cdw_sapp_customer where CREDIT_CARD_NO = '4210653310356919';
 
 -- where CREDIT_CARD_NO = '4210653349028689';
 
 UPDATE creditcard_capstone.cdw_sapp_customer 
 SET FIRST_NAME = "Willfred" 
 WHERE CREDIT_CARD_NO = 4210653310356919;
 
 UPDATE Customers
SET ContactName = 'Alfred Schmidt', City = 'Frankfurt'
WHERE CustomerID = 1;

select distinct substring(timeid,5,1) from cdw_sapp_credit_card;

 select * from creditcard_capstone.cdw_sapp_customer WHERE 
 CREDIT_CARD_NO = 4210653310356919 and FIRST_NAME='Wilfred_test' and substring(SSN,6,4)='4431';
 
 select CONCAT(FULL_STREET_ADDRESS,',',CUST_CITY,',',CUST_STATE) as CUST_ADDRESS from creditcard_capstone.cdw_sapp_customer WHERE 
 CREDIT_CARD_NO = 4210653365537472;
 
 select distinct(CUST_ZIP) from cdw_sapp_customer order by CUST_ZIP ;
 
 update creditcard_capstone.cdw_sapp_customer set last_name='Ayers Test' WHERE CREDIT_CARD_NO = '4210653310356919' and FIRST_NAME='Wilfred' and substring(SSN,6,4)='4431';
 
 UPDATE creditcard_capstone.cdw_sapp_customer SET FIRST_NAME = "William" WHERE CREDIT_CARD_NO = 4210653310356919;
 ALTER TABLE `cdw_sapp_customer` ADD PRIMARY KEY (`FIRST_NAME`, `CREDIT_CARD_NO`,`SSN`) ;
 ALTER TABLE `cdw_sapp_customer` MODIFY COLUMN `FIRST_NAME` VARCHAR(20);
 ALTER TABLE `cdw_sapp_customer` MODIFY COLUMN `CREDIT_CARD_NO` VARCHAR(17);
 ALTER TABLE `cdw_sapp_customer` MODIFY COLUMN `SSN` VARCHAR(11);
 
 select * from creditcard_capstone.cdw_sapp_customer WHERE 
 CREDIT_CARD_NO = 4210653310356919;
 
 select case when length(substring(timeid,5* from cdw_sapp_credit_card;
 
 CREATE TABLE `cdw_sapp_customer` (
  `SSN` bigint DEFAULT NULL,
  `FIRST_NAME` longtext,
  `MIDDLE_NAME` longtext,
  `LAST_NAME` longtext,
  `CREDIT_CARD_NO` longtext,
  `FULL_STREET_ADDRESS` longtext,
  `CUST_CITY` longtext,
  `CUST_STATE` longtext,
  `CUST_COUNTRY` longtext,
  `CUST_ZIP` longtext,
  `CUST_PHONE` longtext,
  `CUST_EMAIL` longtext,
  `LAST_UPDATED` longtext
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select CREDIT_CARD_NO, TIMEID, CUST_SSN, BRANCH_CODE, TRANSACTION_TYPE, TRANSACTION_VALUE, TRANSACTION_ID from cdw_sapp_credit_card
where CREDIT_CARD_NO = %s AND month(TIMEID) = %s AND year(TIMEID) = %s;
-- getting month and year from timeid
select month(timeid),year(timeid) from cdw_sapp_credit_card;
-- distinct zipcode from customer table 151 records (01810, 02127, 02155, 02169)
select distinct CUST_ZIP from cdw_sapp_customer;
-- count of distinct zipcode 151 records
select count(distinct CUST_ZIP) from cdw_sapp_customer;

select distinct CREDIT_CARD_NO, TIMEID, CUST_SSN, BRANCH_CODE, TRANSACTION_TYPE, TRANSACTION_VALUE, TRANSACTION_ID 
from cdw_sapp_credit_card where TIMEID BETWEEN '20180501' AND '20180530';

select distinct count(*) 
from cdw_sapp_credit_card where TIMEID BETWEEN '20180501' AND '20180630';

select * from cdw_sapp_credit_card where credit_card_no = '4210653310356919'
order by TIMEID asc;

select transaction_type, count(transaction_type) as transaction_count from cdw_sapp_credit_card
group by transaction_type
order by transaction_count desc;

select count(transaction_type) from cdw_sapp_credit_card where transaction_type ='Gas';

select * from  cdw_sapp_credit_card where CREDIT_CARD_NO = '4210653310356919';

select * from cdw_sapp_customer where CREDIT_CARD_NO = '4210653310356919';

SELECT sum(cr.TRANSACTION_VALUE) as Total_AMount
             FROM cdw_sapp_credit_card AS cr INNER JOIN cdw_sapp_customer 
             cu ON cr.CREDIT_CARD_NO = cu.CREDIT_CARD_NO AND cr.CUST_SSN = cu.SSN WHERE cu.CUST_ZIP = '79930' AND month(cr.TIMEID) = '5' 
             AND year(cr.TIMEID) = '2018' 
             ;