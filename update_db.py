"""
AUTHOR: CAETANO ROBERTI
DATE: 02/05/2019
"""

import requests
import pandas as pd
from yahoofinancials import YahooFinancials
import sqlite3
import datetime

def get_yahoo_hist_data(ticker,start_date,end_date,period):

    """
    Use yahoofinancials to get the data from Yahoo Finance and test if it exists.

    :param ticker: stock ticker
    :param start_date: start date
    :param end_date: end date
    :param period: can be daily, weekly or monthly
    :return: return a dictionary which contains the stock data
    """
    yahoo_financials_blue_chips = YahooFinancials(ticker)
    blue_chips_dict = yahoo_financials_blue_chips.get_historical_price_data(start_date, end_date, period)

    if len(blue_chips_dict[ticker]) == 1:
        return False
    else:
        return blue_chips_dict


def _check_stock_dict(ticker,stock_dict, min_date):

    """
    Check if the dictionary queried from Yahoo Finance is not corrupted, the data exist or is long enough.

    :param stock_dict: dictionary extract with get_yahoo_hist_data()
    :return: return True if the dictionary is OK, False if it has a problem
    """

    if stock_dict == False:
        print(ticker, 'does not have data in Yahoo Finance')
        return False

    try:
        first_date_serie = datetime.datetime.strptime(stock_dict[ticker]['prices'][0]['formatted_date'], '%Y-%M-%d')
    except:
        print(ticker, 'does not have data in Yahoo Finance')
        return False

    if first_date_serie > min_date:
        print(ticker, 'price series is not long enough')
        return False

    return True


def _transform_dict_to_dataframe(ticker,stock_dict):

    """
    Transform the dictionary queried from Yahoo Finance into a pandas DataFrame to input in the database.

    :param stock_dict: dictionary extract with get_yahoo_hist_data()
    :return: pandas DataFrame prepared to be imputed into the database
    """

    df = pd.DataFrame(stock_dict[ticker]['prices'])
    df = df.dropna()
    df_col = ['formatted_date', 'volume', 'high', 'low', 'close', 'adjclose']
    df = df[df_col]
    df.drop_duplicates(subset='formatted_date', inplace=True)
    df.set_index('formatted_date', inplace=True)

    return df


def update_db_stocks(db_path):

    """
    Update the database with Yahoo Finance stock data. The function will get the tickers from all_tickers_available.py.

    After, will try to connect to database and then starting looping through the all_tickers (where is storage all
    the tickers available at the end of 2018 for BOVESPA in Yahoo Finance)

    Then, it will use get_yahoo_hist_data(ticker,start_date,end_date,period) to collect the stock data. It will
    return a dictionary.

    After, it will check if this dictionary is structured corrected and the stock price serie is long enought for the
    model using the function check_stock_dict(ticker, stock_dict, min_date) which returns True if everything is fine
    or False if there is an error or the serie is not long enough (that will be printed).

    If it returns True, it will use transform_dict_to_dataframe(ticker,stock_dict) to transform the dictionary into a
    pandas DataFrame in the correct format to store in the Database.

    At the end, will store into the Database naming the table the same as the ticker.

    It will return True if nothing interrupt the loop through all_tickers.

    :param db_path: path to the DataBase
    :return: True if everything went fine
    """

    # Yahoo Finance querying config
    start_date = '2000-01-01'  # starting date query
    end_date = '2018-12-31'  # end date query
    period = 'daily'  # can be 'daily', 'weekly' and 'monthly'

    # Update Db config
    data_input_type = 'replace'  # replace or append
    min_date = datetime.datetime.strptime('2006-01-01', '%Y-%M-%d')  # minimal starting date to input the data

    # Getting the list of all tickers available in 2018 in Yahoo Finance

    try:
        from all_tickers_available import all_tickers as all_tickers
    except:
        print("It was not possible to find all_tickers_available.py")
        raise ImportError

    # Trying to connect to the DataBase
    try:
        db_connection = sqlite3.connect(db_path)
    except sqlite3.Error as e:
        print(e)
        return False

    i=0
    for ticker in all_tickers:
        print(ticker, ' - ', i, 'of', len(all_tickers))
        i += 1

        stock_dict = get_yahoo_hist_data(ticker,start_date,end_date,period) # searching in Yahoo Finance

        check_dict = _check_stock_dict(ticker,stock_dict, min_date) # checking dict queried from Yahoo Finance

        if check_dict == False:
            continue

        df = _transform_dict_to_dataframe(ticker,stock_dict)

        if ticker == "^BVSP": # correcting yahoo finance Bovespa ticker
            ticker = "Bovespa"
        else:
            ticker = ticker[:5]  # RETIRANDO .SA DO TICKER
        df.to_sql(ticker, db_connection, if_exists=data_input_type)
        print(ticker, 'was successfully updated')

    db_connection.close()

    return True


def update_db_consumption(db_path):
    """
    Extract the consumption index from Instituto Brasileiro de Geografia e Estatistica (IBGE) using their API Sidra.

    Transform it in a pandas DataFrame and input into the Database.

    If there is no error, it will return True

    :param db_path: path to the DataBase
    :return: True if there is no error
    """

    # Update Db config
    data_input_type = 'replace'  # replace or append
    table_name = "Consumption_Index"

    # Trying to connect to the DataBase
    try:
        db_connection = sqlite3.connect(db_path)
    except sqlite3.Error as e:
        print(e)
        return False

    # URL DA API DO IBGE SIDRA PARA IMPORTAR TABELA
    sidra_api_url = "http://api.sidra.ibge.gov.br/values/t/1620/n1/all/v/all/p/last%2077/c11255/93404/d/v583%204"

    # IMPORTANDO TABELA EM JSON E ARRUMANDO HEADER
    sidra_df = pd.DataFrame(requests.get(url=sidra_api_url).json())
    sidra_df.columns = sidra_df.iloc[0]
    sidra_df = sidra_df.iloc[2:]
    sidra_df['Valor'] = sidra_df['Valor'].astype('float')
    sidra_df.to_sql(table_name, db_connection, if_exists=data_input_type)

    db_connection.close()

    return True




