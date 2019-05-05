"""
AUTHOR: CAETANO ROBERTI
DATE: 02/05/2019
"""

import pandas as pd
import sqlite3

def create_db_connection(db_path):

    """
    Try to create connection with the dabase which contains the stock data.

    :param db_file: path to the database
    :return: if suceed: returns a sqlite3 dabatase connection else: print error and return False
    """

    try:
        con = sqlite3.connect(db_path)

    except sqlite3.Error as e:
        print(e)
        return False

    return con


def get_all_available_stock_table(db_path):

    """
    Get all the stock available tables (tickers) in the Database and returns a list containing the tables
    names, which are the tickers names.

    :param db_path: path to the database
    :return: a list that cointains all the available tables
    """

    try:
        db_connection = sqlite3.connect(db_path)
    except sqlite3.Error as e:
        print(e)
        return False

    cursor = db_connection.cursor()

    available_tables_1 = cursor.execute("SELECT name FROM sqlite_master WHERE type ='table'").fetchall()
    available_tables = [tab[0] for tab in available_tables_1 if tab[0] != 'Consumption_Index' and len(tab[0])<=5]

    db_connection.close()

    return available_tables

def query_household_consumption_index(db_path):

    """
    Query from the database the household consumption data issued from IBGE.

    :param db_path: path to the database
    :return: return a Pandas DataFrame which contains the household consumption index by quarter
    """

    try:
        db_connection = sqlite3.connect(db_path)
    except sqlite3.Error as e:
        print(e)
        return False

    table_name = "Consumption_Index"

    consumption_index = pd.read_sql_query('SELECT Trimestre, Valor FROM %s' % (table_name),db_connection)
    consumption_index.columns = ['Quarter', 'Consumption']
    consumption_index.set_index('Quarter', drop=True, inplace=True)


    db_connection.close()

    return consumption_index

def query_stock_prices(db_path, ticker, price, filter_zero_volume):

    """
    Query the daily stock price from the database.

    If ticker is a string, it will query the ticker data and return in a pandas DataFrame.

    If ticker is a string == 'all_available', it will use the functiion get_all_available_stock_table(db_path)
    to get all the available stock tables ticker will become a list.

    If ticker is a list with one or more tickers, it will loop through it and query the data of each one
    storing in a pandas DataFrame which will be stored in list. At the end, will concatenate all the tickers
    in one main pandas DataFrame

    If filter_zero_volume is True, it will filter out every price which contains volume equal or less than zero.

    It will remove every date that is not in the inverval from 2000 to 2018.

    :param db_path: path to the database
    :param ticker: string or list that contains the stocks tickers
    :param price: can be 'adjclose' or 'close'
    :param filter_zero_volume: boolean, if true will remove any price with volume equal or less than zero
    :return: a pandas DataFrame that contains the name and price of the stocks queried
    """

    try:
        db_connection = sqlite3.connect(db_path)
    except sqlite3.Error as e:
        print(e)
        return False

    if type(ticker) == str:
        if ticker.lower() == 'all_available': # if is all_available, transform ticker into a list using the function
            ticker = get_all_available_stock_table(db_path)
        else:                               # if not, will consider the string is a stock ticker
            if filter_zero_volume == True:
                main_df = pd.read_sql_query("SELECT formatted_date,%s FROM %s WHERE volume>0" % (price,ticker), db_connection)
            else:
                main_df = pd.read_sql_query("SELECT formatted_date,%s FROM %s" % (price, ticker), db_connection)
            main_df['formatted_date'] = pd.to_datetime(main_df['formatted_date'])
            main_df.set_index('formatted_date',drop=True,inplace=True)
            main_df.columns = [ticker]

    else:
        lst_ = []
        for t_ in ticker:
            if filter_zero_volume == True:
                df_ = pd.read_sql_query("SELECT formatted_date,%s FROM %s WHERE volume>0" % (price, t_), db_connection)
            else:
                df_ = pd.read_sql_query("SELECT formatted_date,%s FROM %s" % (price, t_), db_connection)

            df_['formatted_date'] = pd.to_datetime(df_['formatted_date'])
            df_.drop_duplicates(subset='formatted_date', inplace=True)
            df_.set_index('formatted_date', drop=True, inplace=True)
            df_.columns = [t_]
            lst_.append(df_) # storing the DataFrame into a list

        main_df = pd.concat(lst_, axis=1, sort=False) # concatenating the list into one main DataFrame

    main_df.sort_index(inplace=True)
    main_df = main_df[(main_df.index.year >= 2000) & (main_df.index.year <= 2018)] # filtra ano < 2000 e > 2018

    db_connection.close()

    return main_df
