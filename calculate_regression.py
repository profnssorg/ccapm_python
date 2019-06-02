"""
AUTHOR: CAETANO ROBERTI
DATE: 03/05/2019
"""

import pandas as pd
import statsmodels.api as sm
import statsmodels.stats.stattools as smt


def calculate_linear_regression(stock_return_df, consumption_return_df):

    """
    For every ticker (column) inside the stock_return_df, regresses against consumption_return_df.

    Rt = Const + B Ct + et

    After, the regression in form of statsmodels.regression is stored inside a dicitionary inside the key named the
    same as the ticker.

    Inside the loop, after the regression, extract the residuals values and stores inside a pandas Dataframe which is
    store inside a list. After the loop is finished, it concatenate the list in one main pandas Dataframe, being the
    quarters the index and the tickers residues inside the columns.


    :param stock_return_df: Pandas DataFrame that contains stock quarterly returns
    :param consumption_return_df: Pandas DataFrame that contains consumption quarterly returns
    :return: a dictionary that contains OLS.regression and the Dataframe that contains the residues
    """


    if type(stock_return_df) == pd.Series:
        stock_return_df = pd.DataFrame(stock_return_df)

    cols_retorno_stocks = stock_return_df.columns
    stock_return_df = pd.concat([stock_return_df, consumption_return_df], axis=1,sort=True).reindex(stock_return_df.index)
    stock_return_df['Constant'] = 1
    reg_dict = {}
    lst_df = []

    for stock in cols_retorno_stocks:
        slice_stock_return_df = stock_return_df[[stock,'Constant','Consumption']]
        start_quarter = slice_stock_return_df.dropna().index[0]  # getting where the price serie start
        end_quarter = slice_stock_return_df.dropna().index[-1]  # getting where the price serie start
        slice_stock_return_df = slice_stock_return_df.loc[start_quarter:end_quarter,:] # slicing to get where the
                                                                                        # series start
        slice_stock_return_df.fillna(0, inplace=True)  # changin remain nan to zero, assuming there were no negotiation

        reg_ = sm.OLS(endog=slice_stock_return_df[stock], exog=slice_stock_return_df[['Constant','Consumption']],
                      missing=0).fit()
        reg_dict[stock] = reg_
        df_ = pd.DataFrame(reg_dict[stock].resid,columns=[stock])
        lst_df.append(df_)

    df_resid = pd.concat(lst_df,axis=1,sort=False)  # concatenating all df that contain the residues
    df_resid['m'] = [int(m[0]) for m in df_resid.index]
    df_resid['y'] = [int(y[-4:]) for y in df_resid.index]
    df_resid.sort_values(by=['y','m'],inplace=True)  # ordering the index
    df_resid.drop(['m', 'y'], axis=1, inplace=True)

    return (reg_dict, df_resid)

def generate_regression_dataframe(reg_dict):

    """
    From the dictionary which contains the regressions, it will extract the regressions:
    - parameters coefficients, standard deviation and p values,
    - r squared and adjusted r squared
    - f statistic and its p value
    - durbin watson test

    It will store in a pandas Dataframe, where each one of the information above will be assign to one column and each
    row will be a ticker.

    :param reg_dict: dictionary that contains the regressions
    :return: pandas DataFrame
    """

    lst_df = []
    lst_index = []

    for key in reg_dict.keys():

        lst_ = []

        lst_index.append(key)


        lst_.append(reg_dict[key].params['Constant']) # constant parameter
        lst_.append(reg_dict[key].bse['Constant']) # constant standard error
        lst_.append(reg_dict[key].pvalues['Constant']) # constant p value

        lst_.append(reg_dict[key].params['Consumption']) # consumption parameter
        lst_.append(reg_dict[key].bse['Consumption']) # consumption standard error
        lst_.append(reg_dict[key].pvalues['Consumption']) # consumption p value

        lst_.append(reg_dict[key].rsquared) # r squared model
        lst_.append(reg_dict[key].rsquared_adj) # adjusted r squared model
        lst_.append(reg_dict[key].fvalue) # f statistic model
        lst_.append(reg_dict[key].f_pvalue) # p value f statistic
        lst_.append(smt.durbin_watson(reg_dict[key].resid)) # durbin watson

        lst_df.append(lst_)

    lst_columns = ['coef_constante', 'std_err_constante', 'p_value_constante', 'coef_consumption',
                   'std_err_consumption', 'p_value_consumption', 'r_squared', 'r_squared_adj', 'f_stats',
                   'p_value_f_stats', 'durb_watson']


    df_reg = pd.DataFrame(lst_df, columns = lst_columns, index=lst_index)

    return df_reg
