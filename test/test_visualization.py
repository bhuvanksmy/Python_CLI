from unittest import TestCase
from src.visualization import *

class Test(TestCase):

    # Functional Requirements 3.1	Create an appropriate visualization to perform the following task -
    # ●	Calculate and plot which transaction type has the highest transaction count.
    # Note: Take a screenshot of the graph.  Save a copy of the visualization, making sure it is PROPERLY NAMED!
    def test_plot_highest_transaction_count(self):
        result = plot_highest_transaction_count()
        self.assertIsNone(result)

    # Functional Requirements 3.2	Create an appropriate visualization to perform the following task -
    # ●	Calculate and plot top 10 states with the highest number of customers.
    # Note: Take a screenshot of the graph.  Save a copy of the visualization, making sure it is PROPERLY NAMED!
    def test_top10_States_with_high_customers(self):
        result = top10_States_with_high_customers()
        self.assertIsNone(result)

    # Functional Requirements 3.3	Create a single appropriate visualization to perform the following task -
    # ●	Calculate the total transaction sum for each customer based on their individual transactions. Identify the top 10 customers with the highest transaction amounts (in dollar value). Create a plot to showcase these top customers and their transaction sums.
    # Hint (use CUST_SSN).
    # Note: Take a screenshot of the graph.  Save a copy of the visualization, making sure it is PROPERLY NAMED!
    def test_top10_customers_with_high_transaction_amount(self):
        result = top10_customers_with_high_transaction_amount()
        self.assertIsNone(result)

    # Functional Requirements 5.1	Create an appropriate visualization to perform the following task -
    # ●	Calculate and plot the percentage of applications approved for self-employed applicants. Use the appropriate chart or graph to represent this data.
    # Note: Take a screenshot of the graph.  Save a copy of the visualization, making sure it is PROPERLY NAMED!
    def test_plot_percentage_for_applications_approved_for_selfemployed(self):
        result = plot_percentage_for_applications_approved_for_selfemployed()
        self.assertIsNone(result)

    # Functional Requirements 5.2	Create an appropriate visualization to perform the following task -
    # ●	Calculate the percentage of rejection for married male applicants. Use the ideal chart or graph to represent this data.
    # Note: Take a screenshot of the graph.  Save a copy of the visualization, making sure it is PROPERLY NAMED!
    def test_plot_percentage_of_rejections_for_married_male_applicants(self):
        result = plot_percentage_of_rejections_for_married_male_applicants()
        self.assertIsNone(result)

    # Functional Requirements 5.3	Create an appropriate visualization to perform the following task -
    # ●	Calculate and plot the top three months with the largest volume of transaction data. Use the ideal chart or graph to represent this data.
    # (hint: use `CDW_SAPP_CREDIT_CARD` table)
    # Note: Take a screenshot of the graph.  Save a copy of the visualization, making sure it is PROPERLY NAMED!
    def test_top10_customers_with_high_transaction_amount(self):
        result = top3_months_with_high_transactional_data()
        self.assertIsNone(result)

    # Functional Requirements 5.4	Create an appropriate visualization to perform the following task -
    # ●	Calculate and plot which branch processed the highest total dollar value of healthcare transactions. Use the ideal chart or graph to represent this data.
    # (hint: use `CDW_SAPP_CREDIT_CARD` table)
    # Note: Take a screenshot of the graph.  Save a copy of the visualization, making sure it is PROPERLY NAMED!
    def test_branch_with_highest_dollarvalue_healthcare_transactions(self):
        result = branch_with_highest_dollarvalue_healthcare_transactions()
        self.assertIsNone(result)

