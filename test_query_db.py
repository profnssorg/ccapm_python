import unittest
import query_db
import datetime
import pandas as pd
from pandas.util.testing import assert_frame_equal

# Program config
db_path = 'ccapm_regression_database.db'  # path to the Database
price = 'adjclose'  # can be 'adjclose' or 'close'
filter_zero_volume = False  # filter stock prices where volume = 0, it is advised to set True
ticker = ['PETR3', "Bovespa"]
price = 'adjclose'
filter_zero_volume = False

# Test Data

# List containing some stocks that should have being queried
test_lst = ['ABEV3', 'AHEB3', 'ALPA3', 'ATOM3', 'AZEV4', 'BAHI3', 'BAZA3', 'BBAS3', 'BBDC3', 'BDLL4', 'BEES3']

# consumption dataframed with data extracted from the original source
test_consumption = pd.DataFrame([111.09, 105.24, 108.36, 111.88, 115.63], columns=['Consumption'],
                                index=['4º trimestre 1999', '1º trimestre 2000', '2º trimestre 2000',
                                       '3º trimestre 2000', '4º trimestre 2000'])
test_consumption.index.name = 'Quarter'

# stock dataframe with data extracted from the original source
test_stocks = pd.DataFrame([[4.158596515655518, 3.9252066612243652, 3.978247880935669, 3.9464259147644043,
                             3.978247880935669], [16930.0, 15851.0, 16245.0, 16107.0, 16309.0]], index=['PETR3',
                                                                                                        'Bovespa'],
                           columns=[datetime.datetime.strptime('2000-01-03 00:00:00', '%Y-%m-%d %H:%M:%S'),
                                    datetime.datetime.strptime('2000-01-04 00:00:00', '%Y-%m-%d %H:%M:%S'),
                                    datetime.datetime.strptime('2000-01-05 00:00:00', '%Y-%m-%d %H:%M:%S'),
                                    datetime.datetime.strptime('2000-01-06 00:00:00', '%Y-%m-%d %H:%M:%S'),
                                    datetime.datetime.strptime('2000-01-07 00:00:00', '%Y-%m-%d %H:%M:%S')])
test_stocks = test_stocks.T
test_stocks.index.name = 'formatted_date'


class TestQuery(unittest.TestCase):

    def test_get_all_available_stock_table(self):
        all_stocks = query_db.get_all_available_stock_table(db_path)
        self.assertGreaterEqual(len(all_stocks), 180)  # test the length
        self.assertTrue([i for e in test_lst for i in all_stocks if e in i] == test_lst)  # test if all the ticker from
        # the test_lst are present in the all_stocks

    def test_query_household_consumption_index(self):
        consumption = query_db.query_household_consumption_index(db_path)

        self.assertGreaterEqual(consumption.shape[0], 77)
        assert_frame_equal(consumption.iloc[:5], test_consumption)

    def test_query_stock_prices(self):
        stock_df = query_db.query_stock_prices(db_path, ticker, price, filter_zero_volume)

        self.assertGreaterEqual(stock_df.shape[0], 4000)
        assert_frame_equal(stock_df.iloc[:5], test_stocks)
