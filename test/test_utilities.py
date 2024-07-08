from unittest import TestCase
from src.utilities import *


class Test(TestCase):
    # 2.1.1 - Prompt the user for a zip code, provide contextual cues for valid input, and verify it is in the
    # correct format.
    def test_get_valid_zipcode(self):
        zipcode = get_valid_zipcode()
        self.assertIsNotNone(zipcode)

    # 2.1.2 - Ask for a month and year,  and provide contextual cues for valid input and verify it is in the correct
    # format.
    def test_get_valid_month(self):
        month = get_valid_month()
        self.assertIsNotNone(month)

    # 2.1.2 - Ask for a month and year,  and provide contextual cues for valid input and verify it is in the correct
    # format.
    def test_get_valid_year(self):
        year = get_valid_year()
        self.assertIsNotNone(year)

    # 2.1.3 - Use the provided inputs to query the database and retrieve a list of transactions made by customers in the specified zip code for the given month and year.
    # 2.1.4 - Sort the transactions by day in descending order.
    def test_get_transaction(self):
        result = get_transaction(60142, 5, 2018)
        self.assertIsNone(result)

    # 2.2.1.	Used to check the existing account details of a customer.
    def test_get_existing_acc_details(self):
        result = get_existing_acc_details('4210653369092976','4276')
        self.assertIsNone(result)

    # 2.2.2.	Used to modify the existing account details of a customer.
    def test_modify_existing_acc_details(self):
        result = modify_existing_acc_details('4210653369092976','Allan','4276','Allan_test','Osborn_test',
                                             'AOsborn_test@example.com','(123)503-3235_test')
        self.assertIsNone(result)
        result = get_existing_acc_details('4210653369092976', '4276')
        self.assertIsNone(result)
        result = modify_existing_acc_details('4210653369092976', 'Allan_test', '4276', 'Allan', 'Osborn',
                                             'AOsborn@example.com', '(123)503-3235')
        self.assertIsNone(result)
        result = get_existing_acc_details('4210653369092976', '4276')
        self.assertIsNone(result)

    # 2.2.3.	Used to generate a monthly bill for a credit card number for a given month and year.
    def test_get_monthly_bill(self):
        result = get_monthly_bill('4210653369092976', '05','2018')
        self.assertIsNone(result)

    # 2.2.4.    Used to display the transactions made by a customer between two dates. Order by year, month, and day in descending order.
    def test_get_transactions_within_range(self):
        result = get_transactions_within_range(20180517, 20180528)
        self.assertIsNone(result)