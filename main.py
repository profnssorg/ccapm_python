import update_db
import query_db
import calculate_returns
import calculate_regression
import pandas as pd

"""
AUTHOR: Caetano Florian Roberti
Date: 04 May 2019

    Main file of the CCAPM Regression program. It will import all the functions used in here from the respective
    modules:
    
    update_db -> is used to create or update the database which contains the stock and consumption data
    
    query_db -> is used to query the data from the database
    
    calculate_returns -> is used to calculate returns from both stock and consumption
    
    calculate_regression -> is used to regress the stock returns against the consumption returns
    
    pandas -> is used to configure the DataFrame print at the end of the program
    
    It will first ask if the person wants to update the database.
    
    After, it will ask the user for which stocks he wants to estimate.
    
    Than, it will import the data from database e estimate the regression for all the stocks.
    
    After that, it will display the regression_df.
    
    The regression_df contains:
    
    - parameters coefficients, standard deviation and p values,
    - r squared and adjusted r squared
    - f statistic and its p value
    - durbin watson test
    
    After, it will ask if the user want to export to a .csv.
    
    If is choose to do so, it will ask for the name of the file and than export both the selected stock and the
    residues from the regression. 

"""

# Program config
db_path = '/miniconda3/envs/ccapm_regression/Code/ccapm_regression/yahoo_financials_daily.db' # path to the Database
price = 'adjclose' # can be 'adjclose' or 'close'
filter_zero_volume = True # filter stock prices where volume = 0, it is advised to set True

# welcoming

print(""" \n
    Welcome to CCAPM Regression \n""")

# Checking if the user want to update the database
answer_try_update = input('Do you need to update or create the database? Yes or No: ').upper()
while not (answer_try_update[0].upper() == 'Y' or answer_try_update[0].upper() == 'N'):
    answer_try_update = input('Enter Yes or No: ').upper()

if answer_try_update[0].upper() == 'Y':
    has_to_update_bd = 1
else:
    has_to_update_bd = 0

# Updating the Database
if has_to_update_bd == 1:

    print('\nThe database will be updated, it will take a moment.\n')
    answer_terminate_update = input('Do you want to cancel? Yes or No: ')
    while not (answer_terminate_update[0].upper() == 'Y' or
               answer_terminate_update[0].upper() == 'N'):
        answer_terminate_update = input('Enter Yes or No: ').upper()

    if answer_terminate_update[0].upper() == 'N':

        try_update_db_consumption = update_db.update_db_consumption(db_path)

        # if it was not able to update consumption database
        if try_update_db_consumption == False:
            print('It was not possible to update Consumption Database')
            answer_update_error_consumption = input('Do you want to terminate? Yes or No: ').upper()

            while not(answer_update_error_consumption[0].upper() == 'Y' or answer_update_error_consumption[0].upper() == 'N'):
                answer_update_error_consumption = input('Enter Yes or No: ').upper()

            if answer_update_error_consumption[0].upper() == 'Y':
                raise RuntimeError('It was not possible to update Consumption Database')

        try_update_db_stocks = update_db.update_db_stocks(db_path)

        # if it was not able to update stock database
        if try_update_db_stocks == False:
            print('It was not possible to update Stocks Price Database')
            answer_update_error_stocks = input('Do you want to terminate? Yes or No: ').upper()

            while not(answer_update_error_stocks[0].upper() == 'Y' or answer_update_error_stocks[0].upper() == 'N'):
                answer_update_error_stocks = input('Enter Yes or No: ').upper()

            if answer_update_error_stocks[0].upper() == 'Y':
                raise RuntimeError('It was not possible to update Stocks Price Database')
    else:
        pass


# querying all the available tickers
all_tickers = query_db.get_all_available_stock_table(db_path)

# printing the instructions
print("""
    It will be printed every available ticker that is available in the Database.\n
    Choose one or more to compute the Ordinary Least Square estimation of the model.\n
    To stop selecting, just write: done\n
    If you want all of the listed, write: all stocks \n   
""")

# printing all the available ticker
all_tickers.sort()

print('%s' % ', '.join(map(str, all_tickers)))

print()

answers_lst = []


# selection the tickers
answer = input("Write the name of the ticker: ")

if answer.lower() == 'done':
    print()
    print()
    print("Program will be finalized")
    print()
    print()
    exit()

while answer.lower() != 'done':
    if answer == 'all stocks':
        answers_lst = all_tickers
        break

    if answer not in all_tickers:
        print()
        print(answer, ' is not a valid ticker, enter only the ones showed above\n')
        answer = input("Write the name of a valid ticker or write done to finish selection: ")
        continue

    if answer in answers_lst:
        answer = input("You have already selected this one.\n"
                        "Write another ticker or write done to finish selection: ")
        continue

    answers_lst.append(answer)
    answer = input("Write the name of the ticker: ")


print('\nCalculating...\n')

# querying consumption
consumption_df = query_db.query_household_consumption_index(db_path)

#querying stock prices
stocks_prices_df = query_db.query_stock_prices(db_path, answers_lst, price, filter_zero_volume)

# calculating consumption returns
consumption_returns_df = calculate_returns.calculate_consumption_return(consumption_df)


# calculating stock returns
stocks_returns_df = calculate_returns.calculate_stock_returns_df(stocks_prices_df)

# calculating the regressions and extracting the residues
regression_dict, regression_residues_df = calculate_regression.calculate_linear_regression(stocks_returns_df,
                                                                                           consumption_returns_df)

# extracting the regression dataframe
regression_df = calculate_regression.generate_regression_dataframe(regression_dict)


# configuring the pandas display to show the entire dataframe
pd.options.display.max_rows = 999
pd.options.display.max_columns = 20


print("""
    It will be printed below a pandas DataFrame which contains the following information:\n
    - parameters coefficients, standard deviation and p values,\n
    - r squared and adjusted r squared\n
    - f statistic and its p value\n
    - durbin watson test\n    
""")

print(regression_df)
print()


# checking if want to export as .csv
answer_export = input('\nDo you want to export into a .csv? Yes or No: ').upper()

while not(answer_export[0].upper() == 'Y' or answer_export[0].upper() == 'N'):
    answer_export = input('Enter Yes or No ').upper()

if answer_export[0].upper() == 'Y':
    answer_export_name = input("Enter the name to the file be saved with: ")
    if answer_export_name[-4:] != '.csv':
        answer_export_name_residuals = answer_export_name + '_residuals.csv'
        answer_export_name += '.csv'
    else:
        answer_export_name_residuals = answer_export_name[:-4] + '_residuals.csv'

    answer_export_col_separator = input("Enter the .csv column separator, ; or , : ")
    while not (answer_export_col_separator == ';' or answer_export_col_separator == ','):
        answer_export_col_separator = input('Enter ; or ,: ')

    regression_df.to_csv(answer_export_name, sep=answer_export_col_separator)
    regression_residues_df.to_csv(answer_export_name_residuals, sep=answer_export_col_separator)

    print()
    print('\nExported queried regression(s) and residuals with success\n')

print("\nThe program will be finalized!\n")

