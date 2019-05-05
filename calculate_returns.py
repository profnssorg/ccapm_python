"""
AUTHOR: CAETANO ROBERTI
DATE: 03/05/2019
"""

import numpy as np
import pandas as pd
import math



def calculate_consumption_return(consumption_df):

    """
    Calculates the Consumption Index return throwout the quarters from a pandas DataFrame which contains the Consumption
    Index Values by quarters (queried from the Database with query_db.query_household_consumption_index(db_path).

    The return is calculate by ln(Consumption t / Consumption t-1)

    Ex: 2o Quarter of 2000 = ln (Consumption 2o Q 2000/ Consumption 1o Q 2000)

    :param consumo: DataFrame which contains the Consumption Index Values by quarters
    :return: pandas DataFrame with Consumption return quarter by quarter
    """

    if type(consumption_df) != pd.DataFrame:
        raise TypeError ("Must be a pandas DataFrame")

    consumption_returns_df = np.log(consumption_df) - np.log(consumption_df.shift(1))
    consumption_returns_df  = consumption_returns_df.iloc[1:]

    return consumption_returns_df


def calculate_stock_returns_series(stock_series):

    """
    Calculate quarterly stock returns from a pandas Series.

    First, verify if stock_series is a pandas Series, if not raise TypeError.

    To compute the stock returns quarterly, the algorithm is composed with 2 loops.

    First, it will loop through the years an inside it will loop through the quarters.

    For every quarter, it will filter only the piece of the pandas Series which is in the quarter and year
    specified in the looping.

    From this piece, it will drop all the nan values and then will check if it is empty (there is no data for
    that quarter for that year). If true, the return for that quarter will be NaN.

    If is not empty, it will get the first price available in the quarter and the last price available in the quarter
    and then calculate the return.

    The return is calculate by ln(price_end_quarter / price_beg_quarter).

    It will store this value in the lst_returns and at the same time will store the name of the quarter (in Portuguese
    to match the Consumption Values index).

    After computed for every year, it will concatenate in one pandas DataFrame, which is composed of the quarters as
    index and the returns as the column. Than will sort it ascending using the index's month and year.

    At the end, returns a pandas DataFrame

    :param df: pandas Series which contains the stock prices daily
    :return: pandas DataFrame with quartely returns
    """

    lst_quarter = [[1, 2, 3], [4, 5, 6], [7, 8, 9], [10, 11, 12]] # list of the months per quarter

    if type(stock_series) == pd.Series:
        ticker = stock_series.name # getting stock ticker, assuming that is the series name
        lst_returns = []
        lst_quar_str = []
        years = list(dict.fromkeys([y.year for y in stock_series.index])) # getting the years sequence from the index

        for y in years: # looping through the years
            quart = 1 # used to name the quarter, does not impact the looping
            for q_ in lst_quarter: # looping through the quarters
                df_period = stock_series[((stock_series.index.month.isin(q_)) & (stock_series.index.year == y))] # filtering the quarter
                df_period.dropna(inplace=True)
                str_quarter = ("%sº trimestre %s" % (quart, y)) # naming the quarter
                quart += 1
                if df_period.empty == True:
                    retorno = float('NaN')
                else:
                    price_beg_quarter = df_period[0] # getting the beggining quarter price
                    price_end_quarter = df_period[-1] # getting the ending quarter price

                    retorno = np.log(price_end_quarter / price_beg_quarter) # calculating the return

                    if math.isnan(retorno) == True: # checking if there is a error in the series
                        print('Database has an Error')
                        return TypeError

                lst_returns.append(retorno)
                lst_quar_str.append(str_quarter)

        df_retorno = pd.DataFrame(lst_returns, index=lst_quar_str, columns=[ticker]) # creating pandas DataFrame

        df_retorno['m'] = [int(m[0]) for m in df_retorno.index]
        df_retorno['y'] = [int(y[-4:]) for y in df_retorno.index]
        df_retorno.sort_values(by=['y', 'm'], inplace=True)
        df_retorno.drop(['m', 'y'], axis=1, inplace=True)

        return df_retorno

    else:
        raise TypeError('Must be a pandas Series') # if the parameter is not a pandas Series


def calculate_stock_returns_df(df):

    """
    Calculate quarterly stock returns from a pandas DataFrame. Assumes that the DataFrame was get by the function
    query_db.query_stock_prices(db_path, ticker, price, filter_zero_volume).

    First, verify if stock_series is a pandas DataFrame, if not raise TypeError.

    To compute the stock returns quarterly, the algorithm is composed with 3 loops.

    First, it will loop through the columns (assuming each one is one ticker and the content of the column is the price)
    and calculate for every ticker one a one.

    It will filter to another pandas DataFrame the column in the looping named df_split.

    Second, it will loop through the years an inside it will loop through the quarters.

    For every quarter, it will filter, from df_split, only the piece of the df_split which is in the quarter and
    year specified in the looping.

    From this piece, it will drop all the nan values and then will check if it is empty (there is no data for
    that quarter for that year). If true, the return for that quarter will be NaN.

    If is not empty, it will get the first price available in the quarter and the last price available in the quarter
    and then calculate the return.

    The return is calculate by ln(price_end_quarter / price_beg_quarter).

    It will store this value in the lst_returns and at the same time will store the name of the quarter (in Portuguese
    to match the Consumption Values index).

    After computed for every year, it will concatenate in one pandas DataFrame, which is composed of the quarters as
    index and the returns as the column and the name of the column is the stock ticker. Than will store the DataFrame
    into a list (lst_df).

    After loop through all the columns, it will concatenate the lst_df into one main DataFrame, which is composed of
    the quarters as index and the returns as the column, being one column for each ticker.

    Than will sort it ascending using the index's month and year.

    At the end, returns the main Dataframe.

    :param df: DataFrame which contains the stock prices
    :return: DataFrame with the quarterly stock returns
    """

    lst_quarter = [[1, 2, 3], [4, 5, 6], [7, 8, 9], [10, 11, 12]] # list of the month per quarter
    lst_df = []

    if type(df) == pd.DataFrame: # checking if is a DataFrame

        for col_ in df.columns: # looping through the columns
            ticker = col_       # getting the tickers name, assuming is the column name
            df_split = df[ticker] # filtering only the column
            lst_returns = []
            lst_quar_str = []
            years = list(dict.fromkeys([y.year for y in df_split.index])) #getting the years sequence from the index
            for y in years: # looping through years
                quart = 1   # only used to name the quarter, does not impact the looping
                for q_ in lst_quarter: # looping through the quarter
                    df_period = df_split[((df_split.index.month.isin(q_)) & (df_split.index.year == y))] # filtering the
                                                                                                        # quarter
                    df_period.dropna(inplace=True)
                    str_quarter = ("%sº trimestre %s" % (quart, y)) # naming the quarter
                    quart += 1
                    if df_period.empty == True: # checking if there is no data for the quarter
                        retorno = float('NaN')
                    else:
                        price_beg_quarter = df_period[0] # getting the beginning of the quarter price
                        price_end_quarter = df_period[-1] # getting the ending of the quarter price
                        retorno = np.log(price_end_quarter / price_beg_quarter) # calculating the return

                        if math.isnan(retorno) == True:   # check if there is a problem in the DataBase
                            print('Database has an Error')
                            return TypeError

                    lst_returns.append(retorno)  # appending the return
                    lst_quar_str.append(str_quarter) # appending the name of the quarter

            df_ = pd.DataFrame(lst_returns, index=lst_quar_str, columns=[ticker]) # creating stock DataFrame
            lst_df.append(df_) # appending the DataFrame

        df_retorno = pd.concat(lst_df, axis=1,sort=False) # appending all the DataFrames into one main DataFrame
        df_retorno['m'] = [int(m[0]) for m in df_retorno.index]
        df_retorno['y'] = [int(y[-4:]) for y in df_retorno.index]
        df_retorno.sort_values(by=['y', 'm'], inplace=True) # sorting the main Dataframe
        df_retorno.drop(['m', 'y'], axis=1, inplace=True)

        return df_retorno

    else:
        raise TypeError('Must be a pandas DataFrame')
