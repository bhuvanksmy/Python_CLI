use creditcard_capstone;
select distinct CUST_STATE,count(CUST_STATE) from cdw_sapp_customer group by CUST_STATE order by count(CUST_STATE) desc limit 10;

select distinct cus.FIRST_NAME as CUSTOMER_NAME,cc.CREDIT_CARD_NO,round(sum(TRANSACTION_VALUE),2) from cdw_sapp_credit_card as cc INNER JOIN cdw_sapp_customer as cus on cc.CREDIT_CARD_NO = cus.CREDIT_CARD_NO AND cc.CUST_SSN = cus.SSN group by cc.CREDIT_CARD_NO,cus.FIRST_NAME order by sum(TRANSACTION_VALUE) desc LIMIT 10;