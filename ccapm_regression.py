"""
AUTHOR: CAETANO ROBERTI
DATE: 30/04/2019
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import pdb
import sqlite3
import datetime
import math
import statsmodels.api as sm
import statsmodels.stats.diagnostic as smd
import statsmodels.formula.api as smf
import statsmodels.stats.stattools as smt

# DECLARANDO FUNCOES

def create_connection(db_file):

    """

    Cria a conexao com o banco de dados contendo os dados das acoes.

    :param db_file: path do banco de dados
    :return: caso tenha sucesso, conexao com o banco de dados ou False em caso de falha

    """

    try:
        con = sqlite3.connect(db_file)
    except sqlite3.Error as e:
        print(e)
        return False
    return con

def query_consumo(con):

    """

    Faz o query dos trimestres e seus respectivos indices de consumo das familias do banco de dados.

    :param con: conexao com o banco de dados
    :return: retorna um Pandas DataFrame contendo o consumo por trimestre

    """

    consumo = pd.read_sql_query('SELECT Trimestre, Valor FROM Consumo',con)
    consumo.set_index('Trimestre', drop=True, inplace=True)
    consumo.columns = ['Consumo']

    return consumo

def query_stock(ticker, price,con, filter_zero_volume):

    """

    Faz o query dos precos das acoes no banco de dados.

    Caso ticker seja uma unica string, ira fazer o query e entregar um DataFrame

    Caso seja uma lista, ele ira fazer o query de cada ticker no banco de dados e guardara na forma de
    DataFrame dentro de uma lista. No final do looping, faz o merge da lista em um unico Dataframe

    Pode ser filtrado por volume, caso parametro volume seja positivo.

    obs: Filtra dados que nao estiverem  no intervalo 2000 a 2018


    :param ticker: ticker ou lista contendo os tickers das acoes
    :param price: pode ser 'adjclose' ou 'close'
    :param con: conexao com o banco de dados
    :param con: boolean, se verdadeiro, traz apenas as datas que possuem volume > 0
    :return: retorna um Pandas DataFrame contendo data e preco

    """

    if type(ticker) == str:
        if filter_zero_volume == True:
            main_df = pd.read_sql_query("SELECT formatted_date,%s FROM %s WHERE volume>0" % (price,ticker), con)
        else:
            main_df = pd.read_sql_query("SELECT formatted_date,%s FROM %s" % (price, ticker), con)
        main_df['formatted_date'] = pd.to_datetime(main_df['formatted_date'])
        main_df.set_index('formatted_date',drop=True,inplace=True)
        main_df = main_df['adjclose']
        main_df.columns = [ticker]


    else:
        lst_ = []
        for t_ in ticker:
            if filter_zero_volume == True:
                df_ = pd.read_sql_query("SELECT formatted_date,%s FROM %s WHERE volume>0" % (price, t_), con)
            else:
                df_ = pd.read_sql_query("SELECT formatted_date,%s FROM %s" % (price, t_), con)

            df_['formatted_date'] = pd.to_datetime(df_['formatted_date'])
            df_.drop_duplicates(subset='formatted_date', inplace=True)
            df_.set_index('formatted_date', drop=True, inplace=True)
            df_.columns = [t_]
            lst_.append(df_)

        main_df = pd.concat(lst_, axis=1, sort=False) # concatenando a lista de DataFrame em um so

    main_df.sort_index(inplace=True)
    main_df = main_df[(main_df.index.year >= 2000) & (main_df.index.year <= 2018)] # filtra ano < 2000 e > 2018

    return main_df

def calcular_retorno_consumo(consumo):

    """

    Calcula o retorno do consumo trimestre a trimestre.

    O retorno e calculado por ln(Cons t/ Cons t-1)

    Ex: 2o Trimestre de 2000 = ln (Consumo 2o Tri 2000 / Consumo 1o Tri 2000)

    :param consumo: DataFrame contendo consumo por trimestre
    :return: retorna um pandas DataFrame contendo retorno do consumo por trimestre

    """


    if type(consumo) != pd.DataFrame:
        print('Entrar com pandas.DataFrame')
        return ImportError
    consumo_retornos = np.log(consumo) - np.log(consumo.shift(1))
    consumo_retornos  = consumo_retornos.iloc[1:]

    return consumo_retornos


def calcular_retorno_series_stocks(df):

    """

    Calcula o retorno das acoes trimestrais utilizando uma pandas Series como entrada.

    Primeiro, verifica se o arquivo entrado e uma pandas Series. Caso nao seja, retorna Falso.

    Faz um looping de ano a ano e depois de quarter a quarter. Para cada quarter, separa o pedaco da serie
    daquele quarter e pega o primeiro preco do pedaco e o ultimo preco do pedaco, calcula o log do retorno.

    Armazena o retorno na lst_returns e ao mesmo tempo armazena o trimestre na lst_quar_str.

    Ao final, transforma as listas em um Pandas DataFrame.



    :param df: Pandas series contendo contendo retorno diarios
    :return: Pandas DataFrame contendo o retorno trimestral das acoes

    """

    lst_quarter = [[1, 2, 3], [4, 5, 6], [7, 8, 9], [10, 11, 12]]

    if type(df) == pd.Series:
        ticker = df.name # pegando nome do ticker
        lst_returns = []
        lst_quar_str = []
        years = list(dict.fromkeys([y.year for y in df.index])) # pegando a sequencia de anos

        for y in years:
            quart = 1
            for q_ in lst_quarter:
                df_period = df[((df.index.month.isin(q_)) & (df.index.year == y))]
                df_period.dropna(inplace=True)
                str_quarter = ("%sº trimestre %s" % (quart, y))
                quart += 1
                if df_period.empty == True:
                    retorno = float('NaN')
                else:
                    price_beg_quarter = df_period[0]
                    price_end_quarter = df_period[-1]

                    retorno = np.log(price_end_quarter / price_beg_quarter)

                    if math.isnan(retorno) == True:
                        print('Database has an Error')
                        return TypeError

                lst_returns.append(retorno)
                lst_quar_str.append(str_quarter)

        df_retorno = pd.DataFrame(lst_returns, index=lst_quar_str, columns=[ticker])

        df_retorno['m'] = [int(m[0]) for m in df_retorno.index]
        df_retorno['y'] = [int(y[-4:]) for y in df_retorno.index]
        df_retorno.sort_values(by=['y', 'm'], inplace=True)
        df_retorno.drop(['m', 'y'], axis=1, inplace=True)

        return df_retorno

    else:
        raise TypeError('Utilizar pandas Series')


def calcular_retorno_df_stocks(df):

    """

    Calcula o retorno das acoes trimestrais utilizando uma pandas Series como entrada.

    Primeiro, verifica se o arquivo entrado e uma pandas DataFrame. Caso nao seja, retorna Falso.

    Faz um looping de ticker a ticker, depois de ano a ano e depois de quarter a quarter. Para cada ticker separa
    a serie de precos dele. Para cada quarter, separa o pedaco da serie de precos daquele quarter e pega o primeiro
    preco do pedaco e o ultimo preco do pedaco, calcula o log do retorno.

    :param df:
    :return:

    """

    lst_quarter = [[1, 2, 3], [4, 5, 6], [7, 8, 9], [10, 11, 12]]
    lst_df = []

    if type(df) == pd.DataFrame:

        for col_ in df.columns:
            ticker = col_
            df_split = df[ticker]
            lst_returns = []
            lst_quar_str = []
            years = list(dict.fromkeys([y.year for y in df_split.index]))
            for y in years:
                quart = 1
                for q_ in lst_quarter:
                    df_period = df_split[((df_split.index.month.isin(q_)) & (df_split.index.year == y))]
                    df_period.dropna(inplace=True)
                    str_quarter = ("%sº trimestre %s" % (quart, y))
                    quart += 1
                    if df_period.empty == True:
                        retorno = float('NaN')
                    else:
                        price_end_quarter = df_period[0]
                        price_beg_quarter = df_period[-1]
                        retorno = np.log(price_end_quarter / price_beg_quarter)

                        if math.isnan(retorno) == True:
                            print('Database has an Error')
                            return TypeError

                    lst_returns.append(retorno)
                    lst_quar_str.append(str_quarter)

            df_ = pd.DataFrame(lst_returns, index=lst_quar_str, columns=[ticker])
            lst_df.append(df_)

        df_retorno = pd.concat(lst_df, axis=1,sort=False)
        df_retorno['m'] = [int(m[0]) for m in df_retorno.index]
        df_retorno['y'] = [int(y[-4:]) for y in df_retorno.index]
        df_retorno.sort_values(by=['y', 'm'], inplace=True)
        df_retorno.drop(['m', 'y'], axis=1, inplace=True)

        return df_retorno

    else:
        raise TypeError('Utilizar pandas DataFrame')


def calcular_regressao_linear(retorno_stocks, retorno_consumo):

    """

    Faz a regressao do log do retorno de cada ticker (variavel dependente) contra o consumo (variavel explicativa) e
    uma constante.

    Rt = Const + B Ct + et

    Apos fazer a regressao, armazena em um dicionario, sendo a chave o ticker e o conteudo a propria regressao.

    Dentro do loop, tambem extrai os residuos para um DataFrame, armazenando cada um em uma lista. Apos finalizado,
    concatena todos os DataFrames em um so.

    :param retorno_stocks: Pandas DataFrame contendo os retornos das acoes trimestrais
    :param retorno_consumo: Pandas DataFrame contendo os retornos do consumo trimestrais
    :return: Retorna um dicionario contendo como chave o ticker e para cada ticker a regressao do stats model.
             Tambem retorna um Pandas DataFrame contendo os residuos de cada trimestre.
    """

    # Calcula utilizando statsmodels a regressao
    # variavel dependente acao
    # variavel explicativa consumo


    if type(retorno_stocks) == pd.Series:
        retorno_stocks = pd.DataFrame(retorno_stocks)

    cols_retorno_stocks = retorno_stocks.columns
    retorno_stocks = retorno_stocks.join(retorno_consumo)
    retorno_stocks['constante'] = 1
    reg_dict = {}
    lst_df = []

    for stock in cols_retorno_stocks:
        reg_ = sm.OLS(endog=retorno_stocks[stock], exog=retorno_stocks[['constante','Consumo']],missing='drop').fit()
        reg_dict[stock] = reg_
        df_ = pd.DataFrame(reg_dict[stock].resid,columns=[stock])
        lst_df.append(df_)

    pdb.set_trace()
    df_resid = pd.concat(lst_df,axis=1,sort=False)
    df_resid['m'] = [int(m[0]) for m in df_resid.index]
    df_resid['y'] = [int(y[-4:]) for y in df_resid.index]
    df_resid.sort_values(by=['y','m'],inplace=True)
    df_resid.drop(['m', 'y'], axis=1, inplace=True)

    return (reg_dict, df_resid)

def generate_regression_dataframe(reg_dict):

    """

    Extrai do dicionario contendo as regressoes os parametros, desvios padroes, p valores, r quadrado,
    r quadrado ajustado, estatistica f, p valor da estatistica f e durbin watson.

    Armazena todas essas informacoes em um DataFrame, sendo cada linha respectiva a um ticker.

    :param reg_dict: Dicionario contendo as regressoes.
    :return: DataFrame contendo os parametros das regressoes nas colunas e os tickers nas linhas
    """

    # Retira as informacoes do dicionario com as regressoes
    # entrada e o dicionario contendo as regressoes
    # retorna um dataframe com as informacoes

    lst_df = []
    lst_index = []

    for key in reg_dict.keys():

        lst_ = []

        lst_index.append(key)


        lst_.append(reg_dict[key].params['constante']) # parametro constante
        lst_.append(reg_dict[key].bse['constante']) # standard error constante
        lst_.append(reg_dict[key].pvalues['constante']) # p_value constante

        lst_.append(reg_dict[key].params['Consumo']) # parametro consumo
        lst_.append(reg_dict[key].bse['Consumo']) # standard error consumo
        lst_.append(reg_dict[key].pvalues['Consumo']) # #p_value consumo

        lst_.append(reg_dict[key].rsquared) # r squared model
        lst_.append(reg_dict[key].rsquared_adj) # adjusted r squared model
        lst_.append(reg_dict[key].fvalue) # f statistic model
        lst_.append(reg_dict[key].f_pvalue) # p value f statistic
        lst_.append(smt.durbin_watson(reg_dict[key].resid)) # durbin watson

        lst_df.append(lst_)

        del lst_

    lst_columns = ['coef_constante', 'std_err_constante', 'p_value_constante', 'coef_consumo', 'std_err_consumo',
                  'p_value_consumo', 'r_squared', 'r_squared_adj', 'f_stats', 'p_value_f_stats', 'durb_watson']


    df_reg = pd.DataFrame(lst_df, columns = lst_columns, index=lst_index)

    return df_reg


# Caminho para o banco de dados
db_file = '/miniconda3/envs/ccapm_regression/Code/yahoo_financials_daily.db'


# Criando a conexao
con = create_connection(db_file)
cursor = con.cursor()

# Pegando todas as acoes (tabelas) disponiveis no banco de dados
available_tables_1 = cursor.execute("SELECT name FROM sqlite_master WHERE type ='table'").fetchall()
available_tables = [tab[0] for tab in available_tables_1 if tab[0] != 'Consumo']


# Quering do consumo no banco de dados
df_consumo = query_consumo(con)


# Quering das tabelas contendo informacoes das acoes
df_stocks = query_stock(available_tables, 'adjclose', con, False)


# Calculando o retorno das acoes
if type(df_stocks) == pd.DataFrame:
    df_retorno_stocks = calcular_retorno_df_stocks(df_stocks)
elif type(df_stocks)==pd.Series:
    df_retorno_stocks = calcular_retorno_series_stocks(df_stocks)
else:
    oi=1
    pdb.set_trace()



# df_retorno_stocks = calcular_retorno_stocks(df_stocks)

# Calculando o retorno do consumo
df_retorno_consumo = calcular_retorno_consumo(df_consumo)

# calulando regressao linear
reg_dict, df_resid = calcular_regressao_linear(df_retorno_stocks,df_retorno_consumo)


# extraindo os dados das regressoes
df_reg = generate_regression_dataframe(reg_dict)




