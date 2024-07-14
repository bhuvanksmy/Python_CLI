use creditcard_capstone;
select distinct CUST_STATE,count(CUST_STATE) from cdw_sapp_customer group by CUST_STATE order by count(CUST_STATE) desc limit 10;

select distinct cus.FIRST_NAME as CUSTOMER_NAME,cc.CREDIT_CARD_NO,round(sum(TRANSACTION_VALUE),2) from cdw_sapp_credit_card as cc INNER JOIN cdw_sapp_customer as cus on cc.CREDIT_CARD_NO = cus.CREDIT_CARD_NO AND cc.CUST_SSN = cus.SSN group by cc.CREDIT_CARD_NO,cus.FIRST_NAME order by sum(TRANSACTION_VALUE) desc LIMIT 10;
-- 5.4 - Calculate and plot the top three months with the largest volume of transaction data
select month(TIMEID),round(sum(TRANSACTION_VALUE)) from cdw_sapp_credit_card group by month(TIMEID) order by round(sum(TRANSACTION_VALUE)) desc limit 3;

-- 5.5 - Calculate and plot which branch processed the highest total dollar value of healthcare transactions
select distinct cr.BRANCH_CODE,br.BRANCH_CITY,cr.TRANSACTIONAL_VALUE from cdw_sapp_branch br inner join (select BRANCH_CODE,round(sum(TRANSACTION_VALUE)) as TRANSACTIONAL_VALUE from cdw_sapp_credit_card where TRANSACTION_TYPE = 'Healthcare' group by BRANCH_CODE order by round(sum(TRANSACTION_VALUE)) desc ) cr ON  br.BRANCH_CODE = cr.BRANCH_CODE order by cr.TRANSACTIONAL_VALUE desc;
select distinct cr.BRANCH_CODE,br.BRANCH_CITY,cr.TRANSACTIONAL_VALUE from cdw_sapp_branch br inner join (select BRANCH_CODE,round(sum(TRANSACTION_VALUE)) as TRANSACTIONAL_VALUE from cdw_sapp_credit_card where TRANSACTION_TYPE = 'Healthcare' group by BRANCH_CODE order by round(sum(TRANSACTION_VALUE)) desc ) cr ON  br.BRANCH_CODE = cr.BRANCH_CODE order by cr.TRANSACTIONAL_VALUE desc;

select BRANCH_CODE,round(sum(TRANSACTION_VALUE)) as TRANSACTIONAL_VALUE from cdw_sapp_credit_card where TRANSACTION_TYPE = 'Healthcare'
group by BRANCH_CODE
order by round(sum(TRANSACTION_VALUE)) desc;

select * from cdw_sapp_branch;

select * from cdw_sapp_branch;

 -- 5.4 - Calculate and plot the top three months with the largest volume of transaction data
select month(TIMEID),count(TRANSACTION_ID) from cdw_sapp_credit_card 
group by month(TIMEID) order by count(TRANSACTION_ID) desc limit 3