# IMPORTANDO BIBLIOTECAS
import requests
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import pdb
from yahoofinancials import YahooFinancials
import sqlite3
import datetime


print()    
print('--------------------------------------------------------------------')
print('Iniciando')
print('--------------------------------------------------------------------')
print() 

# CONECTANDO COM A BASE DE DADOS

db_file = '/miniconda3/envs/ccapm_regression/Code/yahoo_financials_daily.db'
conn = sqlite3.connect(db_file)
cur = conn.cursor()

# DECLARACAO DAS VARIAVEIS 

# CONFIGURACAO DO QUERYING NO YAHOO
start_date = '1999-12-01'  # DATA INICIAL DA SERIE PARA BUSCAR NO YAHOO
end_date = '2019-12-31'  # DATA FINAL DA SERIE PARA BUSCAR NO YAHOO
period = 'daily'  # TIPO DE DADO, PODE SER 'daily', 'weekly' and 'monthly'
data_input_type = 'replace' # CASO A TABLEA JA EXISTA, 'replace' PARA SUBSTITUIR
                            # 'append' PARA ADICIONAR A TABELA EXISTENTE

min_date = datetime.datetime.strptime('2006-01-01', '%Y-%M-%d')


# TICKERS DO YAHOO

all_tickers = ["^BVSP",
"LAME4.SA",
"ABEV3.SA",
"SBSP3.SA",
"VALE3.SA",
"PETR4.SA",
"OIBR4.SA",
"BBDC4.SA",
"MRVE3.SA",
"ITUB4.SA",
"EMBR3.SA",
"CYRE3.SA",
"BTOW3.SA",
"BRKM5.SA",
"USIM3.SA",
"BBDC3.SA",
"MRFG3.SA",
"JBSS3.SA",
"DTEX3.SA",
"CPLE6.SA",
"CPFE3.SA",
"CGAS5.SA",
"USIM5.SA",
"RSID3.SA",
"PFRM3.SA",
"PCAR4.SA",
"CESP6.SA",
"BBAS3.SA",
"VLID3.SA",
"RPMG3.SA",
"JBDU4.SA",
"GRND3.SA",
"GEPA3.SA",
"REDE3.SA",
"PDGR3.SA",
"MMXM3.SA",
"KROT3.SA",
"ELET3.SA",
"ECOR3.SA",
"WEGE3.SA",
"HGTX3.SA",
"CEGR3.SA",
"JBDU3.SA",
"MEND5.SA",
"TEKA3.SA",
"MYPK3.SA",
"KLBN4.SA",
"ELEK3.SA",
"NATU3.SA",
"TECN3.SA",
"OIBR3.SA",
"CCRO3.SA",
"PRIO3.SA",
"OSXB3.SA",
"ODPV3.SA",
"CPLE3.SA",
"CMIG4.SA",
"OFSA3.SA",
"JOPA4.SA",
"UGPA3.SA",
"TIMP3.SA",
"QUAL3.SA",
"QGEP3.SA",
"GGBR4.SA",
"SEER3.SA",
"VIVT4.SA",
"UCAS3.SA",
"MGLU3.SA",
"GOAU4.SA",
"AZEV4.SA",
"CRIV4.SA",
"BRIV4.SA",
"ENEV3.SA",
"VIVR3.SA",
"WHRL4.SA",
"RNEW3.SA",
"SHOW3.SA",
"KEPL3.SA",
"LINX3.SA",
"TXRX4.SA",
"DTCY3.SA",
"TUPY3.SA",
"SNSY5.SA",
"FLRY3.SA",
"ARZZ3.SA",
"ITSA4.SA",
"TGMA3.SA",
"PSSA3.SA",
"ITSA3.SA",
"GFSA3.SA",
"DASA3.SA",
"BAZA3.SA",
"ALPA4.SA",
"CTSA4.SA",
"FJTA4.SA",
"CGRA3.SA",
"MDIA3.SA",
"IGTA3.SA",
"CTSA3.SA",
"MSPA3.SA",
"HAGA4.SA",
"HAGA3.SA",
"FESA4.SA",
"FESA3.SA",
"TEKA4.SA",
"JOPA3.SA",
"HETA4.SA",
"GEPA4.SA",
"CTKA4.SA",
"EUCA3.SA",
"TCSA3.SA",
"CGRA4.SA",
"LUPA3.SA",
"MTSA3.SA",
"EUCA4.SA",
"FJTA3.SA",
"FRTA3.SA",
"CSNA3.SA",
"ALPA3.SA",
"MTSA4.SA",
"BPHA3.SA",
"ABCB4.SA",
"EALT3.SA",
"EALT4.SA",
"CALI4.SA",
"BTTL3.SA",
"BRGE3.SA",
"BRGE8.SA",
"BRGE6.SA",
"TIET4.SA",
"TIET3.SA",
"AFLT3.SA",
"AGRO3.SA",
"CASN3.SA",
"CASN4.SA",
"SLCE3.SA",
"CVCB3.SA",
"EMAE4.SA",
"AHEB3.SA",
"GOLL4.SA",
"ALSC3.SA",
"RPAD6.SA",
"TAEE3.SA",
"MEAL3.SA",
"PEAB4.SA",
"BAUH4.SA",
"RPAD3.SA",
"BRIV3.SA",
"CSAB4.SA",
"PEAB3.SA",
"TAEE4.SA",
"CRIV3.SA",
"RPAD5.SA",
"AMAR3.SA",
"LAME3.SA",
"LCAM3.SA",
"CBEE3.SA",
"ANIM3.SA",
"MOAR3.SA",
"ATOM3.SA",
"VULC3.SA",
"AZEV3.SA",
"SANB4.SA",
"SANB3.SA",
"TOYB3.SA",
"TELB4.SA",
"BMEB4.SA",
"BGIP4.SA",
"BPAN4.SA",
"CAMB4.SA",
"BALM4.SA",
"BBSE3.SA",
"BBRK3.SA",
"BRSR5.SA",
"BRSR6.SA",
"BNBR3.SA",
"IDVL4.SA",
"BMIN4.SA",
"BRSR3.SA",
"BMIN3.SA",
"BPAR3.SA",
"IDVL3.SA",
"BGIP3.SA",
"BDLL4.SA",
"BEEF3.SA",
"BEES4.SA",
"BIOM3.SA",
"BSEV3.SA",
"BMKS3.SA",
"BOBR4.SA",
"LIGT3.SA",
"BRML3.SA",
"BRFS3.SA",
"TPIS3.SA",
"SHUL4.SA",
"RLOG3.SA",
"RENT3.SA",
"RAPT4.SA",
"RANI3.SA",
"PLAS3.SA",
"PETR3.SA",
"PATI3.SA",
"MTIG4.SA",
"LREN3.SA",
"HYPE3.SA",
"GGBR3.SA",
"ETER3.SA",
"EQTL3.SA",
"ELET6.SA",
"EKTR4.SA",
"CTNM4.SA",
"CSAN3.SA",
"CIEL3.SA",
"BRPR3.SA",
"BRKM3.SA",
"BRAP4.SA",
"MERC3.SA",
"IDNT3.SA",
"ENGI4.SA",
"CELP3.SA",
"MULT3.SA",
"SLED3.SA",
"TOYB4.SA",
"BAHI3.SA",
"CLSC4.SA",
"EVEN3.SA",
"PNVL4.SA",
"TCNO4.SA",
"GOAU3.SA",
"TRPL4.SA",
"MNPR3.SA",
"SNSL3.SA",
"CRPG6.SA",
"RADL3.SA",
"TRIS3.SA",
"SGPS3.SA",
"CSMG3.SA",
"ENMT3.SA",
"RANI4.SA",
"PCAR3.SA",
"ITEC3.SA",
"EKTR3.SA",
"SAPR4.SA",
"PINE4.SA",
"CESP5.SA",
"MGEL4.SA",
"CORR4.SA",
"CEPE5.SA",
"MERC4.SA",
"ELPL3.SA",
"BUET3.SA",
"DIRR3.SA",
"CRPG5.SA",
"EEEL3.SA",
"CEEB5.SA",
"CEDO3.SA",
"ITUB3.SA",
"LUXM4.SA",
"CGAS3.SA",
"IGBR3.SA",
"BSLI4.SA",
"CCXC3.SA",
"CCPR3.SA",
"CEPE6.SA",
"CELP6.SA",
"COCE5.SA",
"COCE3.SA",
"CELP5.SA",
"CEED4.SA",
"CEBR6.SA",
"MAPT4.SA",
"CEDO4.SA",
"CELP7.SA",
"CEBR5.SA",
"CEBR3.SA",
"CESP3.SA",
"CEED3.SA",
"CEEB3.SA",
"ELET5.SA",
"CSRN5.SA",
"TRPL3.SA",
"CTNM3.SA",
"SAPR3.SA",
"CSRN3.SA",
"PRVI3.SA",
"CMIG3.SA",
"CPRE3.SA",
"CRPG3.SA",
"CRDE3.SA",
"CARD3.SA",
"SOND5.SA",
"SLED4.SA",
"SOND6.SA",
"NORD3.SA",
"MEND6.SA",
"ENMT4.SA",
"PNVL3.SA",
"DOHL4.SA",
"BEES3.SA",
"ENBR3.SA",
"BMEB3.SA",
"DOHL3.SA",
"LEVE3.SA",
"ECPR3.SA",
"ECPR4.SA",
"SEDU3.SA",
"EEEL4.SA",
"ELEK4.SA",
"LIPR3.SA",
"SCAR3.SA",
"HBOR3.SA",
"EZTC3.SA",
"ESTR4.SA",
"MILS3.SA",
"ESTR3.SA",
"ESTC3.SA",
"JHSF3.SA",
"FHER3.SA",
"JFEN3.SA",
"FRAS3.SA",
"FRIO3.SA",
"JSLG3.SA",
"NAFG3.SA",
"NAFG4.SA",
"GSHP3.SA",
"USIM6.SA",
"GPCP3.SA",
"GUAR3.SA",
"HBTS5.SA",
"SPRI5.SA",
"SPRI3.SA",
"ENGI3.SA",
"POSI3.SA",
"RDNI3.SA",
"ROMI3.SA",
"PATI4.SA",
"LPSB3.SA",
"RAPT3.SA",
"KLBN3.SA",
"RCSL4.SA",
"MNDL3.SA",
"WHRL3.SA",
"RCSL3.SA",
"RSUL4.SA",
"PTBL3.SA",
"LLIS3.SA",
"LOGN3.SA",
"PMAM3.SA",
"MPLU3.SA",
"MWET4.SA",
"TRPN3.SA",
"SMTO3.SA",
"POMO4.SA",
"TCNO3.SA",
"POMO3.SA",
"HOOT4.SA",
"INEP4.SA",
"UNIP3.SA",
"UNIP5.SA",
"UNIP6.SA",
"PTNT4.SA",
"SSBR3.SA",
"VVAR3.SA",
"RNEW4.SA",
"VIVT3.SA",
"TOTS3.SA",
"TELB3.SA",
"BALM3.SA",
"BRAP3.SA",
"INEP3.SA",
"WLMM4.SA",
"EGIE3.SA",
"TESA3.SA",
"STBP3.SA",
"SULA3.SA",
"ADHM3.SA",
"RAIL3.SA",
"DMMO3.SA",
"BPAC5.SA",
"BPAC3.SA",
"ALUP4.SA",
"ALUP3.SA",
"SULA4.SA",
"AZUL4.SA",
"MOVI3.SA",
"PARD3.SA",
"AALR3.SA",
"WLMM3.SA",
"OMGE3.SA",
"IRBR3.SA",
"CAML3.SA"]

# DEFINICAO DAS FUNCOES

# PEGA NO YAHOO OS DADOS, CHECA SE EXISTE E ENTREGA UM DICIONARIO
def get_yahoo_hist_data(ticker):
    yahoo_financials_blue_chips = YahooFinancials(ticker)
    blue_chips_dict = yahoo_financials_blue_chips.get_historical_price_data(start_date, end_date, period)
    
    if len(blue_chips_dict[ticker])==1:
        return False
    else:
        return blue_chips_dict

# CRIA TABELA DENTRO DA BASE DE DADOS
def create_table(ticker):
    cur.execute('CREATE TABLE IF NOT EXISTS '+ str(ticker)+ '(fomartted_date TEXT, volume REAL, high REAL, low REAL, open REAL, close REAL, adjclose REAL)')

# FUNCAO QUE ADICIONA UMA LINHA EM UMA TABELA DA BASE DE DADOS,
def dynamic_data_entry(table, formatted_date,volume, high,low,opn,close, adjclose):

    cur.execute("INSERT INTO "+ table + "(fomartted_date, volume, high, low, open, close, adjclose) VALUES (?, ?, ?, ?, ?, ?, ?)",(formatted_date, volume, high, low, opn, close, adjclose))
    
    conn.commit()
    
# COLOCA LINHA A LINHA DE UM DATAFRAME EM UMA TABELA
def update_data_base(tabela, df):
    
    if len(tabela)>5:
        tabela = tabela[:5]
        
    create_table(tabela)
    
    for row in range(df.shape[0]):
        arg1 = df.iloc[row]['formatted_date']
        arg2 = df.iloc[row]['volume']
        arg3 = df.iloc[row]['high']
        arg4 = df.iloc[row]['low']
        arg5 = df.iloc[row]['open']
        arg6 = df.iloc[row]['close']
        arg7 = df.iloc[row]['adjclose']

        dynamic_data_entry(tabela, arg1, arg2, arg3, arg4, arg5, arg6, arg7)
        
    print(ticker, "atualizado com sucesso")


# ENTRADA DE DADOS, PROCESSAMENTO E SAIDA

i = 0
for ticker in all_tickers:
    print(i, 'de ', len(all_tickers))
    i += 1
    
# BUSCA NO YAHOO USANDO A FUNCAO DEFINIDA ACIMA
    stock_dict = get_yahoo_hist_data(ticker)
        
    if stock_dict == False:
        print(ticker, 'nao possui dados de cotacao')
        continue
        
    try:   
        first_date_serie = datetime.datetime.strptime(stock_dict[ticker]['prices'][0]['formatted_date'], '%Y-%M-%d')
    except:
        print(ticker, 'nao possui dados de cotacao')
        continue
    
    if first_date_serie > min_date:
        print(ticker, 'nao possui serie longa suficiente')
        continue

    df = pd.DataFrame(stock_dict[ticker]['prices'])
    df = df.dropna()
    df_col = ['formatted_date','volume','high','low', 'close', 'adjclose']
    df = df[df_col]
    df.drop_duplicates(subset='formatted_date', inplace=True)
    df.set_index('formatted_date',inplace=True)
    
# SAIDA PARA A BASE DE DADOS
    if ticker == "^BVSP":
        ticker = "Bovespa"
    else:
        ticker = ticker[:5] # RETIRANDO .SA DO TICKER
    df.to_sql(ticker, conn, if_exists=data_input_type)
    print(ticker, 'atualizado com sucesso')
    
    
    
cur.close()
conn.close()


print()    
print('--------------------------------------------------------------------')
print('Finalizado')
print('--------------------------------------------------------------------')
print() 
