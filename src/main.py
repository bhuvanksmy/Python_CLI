from os import system

from utilities import *
from visualization import *


def display_menu(menu):
    print("Menu List: ")
    """
    Display a menu where the key identifies the name of a function.
    :param menu: dictionary, key identifies a value which is a function name
    :return:
    """
    for k, function in menu.items():
        print(k, function.__name__)


def main():
    # Create a menu dictionary where the key is an integer number and the
    # value is a function name.
    functions_names = [load_json_data_to_database, get_valid_zipcode, get_valid_month, get_valid_year,
                       get_transaction,
                       get_existing_acc_details, modify_existing_acc_details, get_monthly_bill,
                       get_transactions_within_range, plot_highest_transaction_count,
                       top10_States_with_high_customers,
                       top10_customers_with_high_transaction_amount,
                       plot_percentage_for_applications_approved_for_selfemployed,
                       plot_percentage_of_rejections_for_married_male_applicants,
                       top3_months_with_high_transactional_data,
                       get_branch_with_highest_dollarvalue_healthcare_transactions, done]
    menu_items = dict(enumerate(functions_names, start=1))
    while True:
        display_menu(menu_items)
        selection = int(
            input("Please enter your selection number: "))  # Get function key
        selected_value = menu_items[selection]  # Gets the function name
        selected_value()  # add parentheses to call the function
        # system('cls')


def done():
    print("Exiting from application. Thank you!")
    sys.exit()


if __name__ == "__main__":
    print("Application Started")
    main()
