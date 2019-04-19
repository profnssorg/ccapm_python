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

    # cria conexao com o banco de dados
    # entrada e o caminho para o banco de dados

    try:
        con = sqlite3.connect(db_file)
    except sqlite3.Error as e:
        print(e)
        return False
    return con

def query_consumo(con):

    # Faz o query da tabela Consumo dentro do banco de dados
    # Para facilidade de calculo depois, retorna um dataframe contendo os dados
    # comecando da data mais nova a mais antiga
    # con: variavel que contem a conexao com banco de dados
    # Retorna um DataFrame com os dados

    consumo = pd.read_sql_query('SELECT Trimestre, Valor FROM Consumo',con)
    consumo.set_index('Trimestre', drop=True, inplace=True)
    consumo.columns = ['Consumo']
    if int(consumo.index[0][-4:]) < int(consumo.index[-1][-4:]):
        consumo = consumo.iloc[::-1]

    return consumo

def query_stock(ticker, price,con):

    # Faz o query do banco de dados das acoes
    # ticker: pode ser str, para buscar apenas uma acao
    # ticker: pode ser uma lista, para buscar varias acoes
    # price: deve ser ou 'adjclose' ou 'close'
    # con: variavel que contem a conexao com banco de dados
    # Retorna um DataFrame com os Dados

    if type(ticker) == str:
        main_df = pd.read_sql_query("SELECT formatted_date,%s FROM %s WHERE volume>0" % (price,ticker), con)
        main_df['formatted_date'] = pd.to_datetime(main_df['formatted_date'])
        main_df.set_index('formatted_date',drop=True,inplace=True)
        main_df = main_df['adjclose']
        main_df.columns = [ticker]

    else:
        main_df = pd.read_sql_query("SELECT formatted_date,%s FROM %s WHERE volume>0" % (price,ticker[0]), con)
        main_df['formatted_date'] = pd.to_datetime(main_df['formatted_date'])
        main_df.set_index('formatted_date', drop=True, inplace=True)
        main_df.columns = [ticker[0]]
        main_df = main_df[main_df.index.year < 2019]
        try:
            len_ticker = len(ticker)
        except:
            len_ticker = ticker.shape[0]

        if len_ticker > 1:
            for t_ in ticker[1:]:
                sec_df = pd.read_sql_query("SELECT formatted_date,%s FROM %s WHERE volume>0" % (price, t_), con)
                sec_df['formatted_date'] = pd.to_datetime(sec_df['formatted_date'])
                sec_df.set_index('formatted_date', drop=True, inplace=True)
                sec_df.columns = [t_]
                main_df = main_df.join(sec_df)

    main_df = main_df[main_df.index.year < 2019]
    if main_df.index[0] < main_df.index[-1]:
        main_df = main_df.iloc[::-1 ]
    return main_df


def calcular_retorno_consumo(consumo):

    # Calcula o retorno do consumo
    # consumo: deve ser entrado o DataFrame que foi feito o query utilizando function "query_consumo(con)"
    # Retorna um DataFrame contendo o retorno do consumo trimestre a trimestre

    if type(consumo) != pd.DataFrame:
        print('Entrar com pandas.DataFrame')
        return ImportError

    consumo_retornos = np.log(np.array(consumo['Consumo'].iloc[:-1])/np.array(consumo['Consumo'].iloc[1:]))

    consumo_retornos_df = pd.DataFrame(consumo_retornos, index=consumo.index[:-1], columns=['Consumo'])

    return consumo_retornos_df

# Calcula o retorno para cada ticker dentro do dataframe
#
def calcular_retorno_stocks(df):

    # calcula o retorno trimestral das acoes
    # retorna um dataframe com o retorno das acoes por trimestre

    lst_quarter = [[1,2,3],[4,5,6],[7,8,9],[10,11,12]]

    if type(df) == pd.Series:
        ticker = df.name
        lst_returns = []
        lst_quar_str = []
        years = pd.unique([y_.year for y_ in df.index])
        if years[0]<years[-1]:
            years = years[::-1]
        for y_ in years:
            quart = 1
            for q_ in lst_quarter:
                df_period = df[((df.index.month.isin(q_)) & (df.index.year == y_))]
                df_period.dropna(inplace=True)
                if df_period.empty == True:
                    continue
                str_quarter = ("%sº trimestre %s" % (quart, y_))
                quart += 1
                price_end_quarter = df_period[0]
                price_beg_quarter = df_period[-1]


                retorno = np.log(price_end_quarter/price_beg_quarter)


                if math.isnan(retorno) == True:
                    print('Database has an Error')
                    return TypeError

                lst_returns.append(retorno)
                lst_quar_str.append(str_quarter)

        df_retorno = pd.DataFrame(lst_returns, index=lst_quar_str, columns=[ticker])

        return df_retorno

    elif type(df) == pd.DataFrame:
        ticker = df.columns[0]
        df_split = df[ticker]
        lst_returns = []
        lst_quar_str = []
        years = pd.unique([y_.year for y_ in df_split.index])
        if years[0] < years[-1]:
            years = years[::-1]
        for y_ in years:
            quart = 1
            for q_ in lst_quarter:
                df_period = df_split[((df_split.index.month.isin(q_)) & (df_split.index.year == y_))]
                df_period.dropna(inplace=True)
                if df_period.empty == True:
                    continue
                str_quarter = ("%sº trimestre %s" % (quart, y_))
                quart += 1
                price_end_quarter = df_period[0]
                price_beg_quarter = df_period[-1]

                retorno = np.log(price_end_quarter / price_beg_quarter)

                if math.isnan(retorno) == True:
                    print('Database has an Error')
                    oi=1
                    return TypeError

                lst_returns.append(retorno)
                lst_quar_str.append(str_quarter)

        df_retorno = pd.DataFrame(lst_returns, index=lst_quar_str, columns=[ticker])

        if df.columns.shape[0] > 1:
            for col_ in df.columns[1:]:
                ticker = col_
                df_split = df[ticker]
                lst_returns = []
                lst_quar_str = []
                years = pd.unique([y_.year for y_ in df_split.index])
                if years[0] < years[-1]:
                    years = years[::-1]
                for y_ in years:
                    quart = 1
                    for q_ in lst_quarter:
                        df_period = df_split[((df_split.index.month.isin(q_)) & (df_split.index.year == y_))]
                        df_period.dropna(inplace=True)
                        if df_period.empty == True:
                            continue
                        str_quarter = ("%sº trimestre %s" % (quart, y_))
                        quart += 1

                        price_end_quarter = df_period[0]
                        price_beg_quarter = df_period[-1]

                        retorno = np.log(price_end_quarter / price_beg_quarter)

                        if math.isnan(retorno) == True:
                            print('Database has an Error')
                            return TypeError

                        lst_returns.append(retorno)
                        lst_quar_str.append(str_quarter)

                df_retorno_2 = pd.DataFrame(lst_returns, index=lst_quar_str, columns=[ticker])
                df_retorno = df_retorno.join(df_retorno_2)

        return df_retorno
    else:
        return False

def calcular_regressao_linear(retorno_stocks, retorno_consumo):

    # Calcula utilizando statsmodels a regressao
    # variavel dependente acao
    # variavel explicativa consumo


    if type(retorno_stocks) == pd.Series:
        retorno_stocks = pd.DataFrame(retorno_stocks)

    cols_retorno_stocks = retorno_stocks.columns
    retorno_stocks = retorno_stocks.join(retorno_consumo)
    retorno_stocks['constante'] = 1
    reg_dict = {}
    first_iterarion = 1

    for stock in cols_retorno_stocks:
        reg_ = sm.OLS(endog=retorno_stocks[stock], exog=retorno_stocks[['constante','Consumo']],missing='drop').fit()
        reg_dict[stock] = reg_
        if first_iterarion == 1:
            df_resid = pd.DataFrame(reg_dict[stock].resid,columns=[stock])
            first_iterarion = 0
        else:
            df_resid_2 = pd.DataFrame(reg_dict[stock].resid,columns=[stock])
            df_resid =  pd.merge(df_resid, df_resid_2, left_index=True, right_index=True, how='outer')
    df_resid['m'] = [int(m[0]) for m in df_resid.index]
    df_resid['y'] = [int(y[-4:]) for y in df_resid.index]
    df_resid.sort_values(by=['y','m'],inplace=True)
    df_resid = df_resid.iloc[::-1]
    df_resid.drop(['m', 'y'], axis=1, inplace=True)

    return (reg_dict, df_resid)

def generate_regression_dataframe(reg_dict):

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
consumo = query_consumo(con)


# Quering das tabelas contendo informacoes das acoes
df_main = query_stock(available_tables, 'adjclose', con)


# Calculando o retorno das acoes
retorno_stocks = calcular_retorno_stocks(df_main)

# Calculando o retorno do consumo
retorno_consumo = calcular_retorno_consumo(consumo)

# calulando regressao linear
reg_dict, df_resid = calcular_regressao_linear(retorno_stocks,retorno_consumo)


# extraindo os dados das regressoes
df_reg = generate_regression_dataframe(reg_dict)
