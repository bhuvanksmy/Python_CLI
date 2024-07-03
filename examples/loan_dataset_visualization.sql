use creditcard_capstone;
# using group by
select Self_Employed,count(Self_Employed) as count from cdw_sapp_loan_application group by Self_Employed;
-- req - 5.1 - percentage of applications approved for self-employed applicants
select Application_Status,count(Application_Status) as count from cdw_sapp_loan_application where Self_Employed = 'Yes' group by Application_Status;
-- req - 5.2 - 
select Application_Status,count(*) from cdw_sapp_loan_application where Gender = 'Male' AND Married = 'Yes' group by Application_Status;

select * from cdw_sapp_loan_application where Gender = 'Male' AND Married = 'Yes' ;
