import unittest
import query_db
import datetime

# Program config
db_path = 'ccapm_regression_database.db' # path to the Database
price = 'adjclose' # can be 'adjclose' or 'close'
filter_zero_volume = False # filter stock prices where volume = 0, it is advised to set True
ticker = ['PETR3', "Bovespa"]
price = 'adjclose'
filter_zero_volume = False


class TestQuery(unittest.TestCase):

    def test_get_all_available_stock_table(self):

        all_stocks = query_db.get_all_available_stock_table(db_path)

        self.assertGreaterEqual(len(all_stocks), 180)


    def test_query_household_consumption_index(self):

        consumption = query_db.query_household_consumption_index(db_path)

        self.assertGreaterEqual(consumption.shape[0], 77)
        self.assertEqual(consumption.iloc[0]['Consumption'], 111.09)
        self.assertEqual(consumption.columns[0],'Consumption')
        self.assertEqual(consumption.index[0], '4º trimestre 1999')
        self.assertEqual(consumption.dtypes[0], 'float64')

    def test_query_stock_prices(self):

        stock_df = query_db.query_stock_prices(db_path, ticker, price, filter_zero_volume)

        self.assertGreaterEqual(stock_df.shape[0], 4000)
        self.assertLessEqual(stock_df.index[0], datetime.datetime.strptime('2006-01-01', '%Y-%M-%d'))
        self.assertEqual(stock_df.columns[0]+stock_df.columns[1], 'PETR3Bovespa')
        self.assertEqual(stock_df.dtypes[0], 'float64')
        self.assertEqual(stock_df.dtypes[1], 'float64')