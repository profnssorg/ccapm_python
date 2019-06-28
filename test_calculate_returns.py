import unittest
import calculate_returns
import query_db
import pandas as pd

# Program config
db_path = 'ccapm_regression_database.db' # path to the Database
price = 'adjclose' # can be 'adjclose' or 'close'
filter_zero_volume = False # filter stock prices where volume = 0, it is advised to set True
ticker = ['PETR3', 'Bovespa', 'ABEV3']
price = 'adjclose'
filter_zero_volume = False

# Test Data
consumption = query_db.query_household_consumption_index(db_path)
stock_df = query_db.query_stock_prices(db_path, ticker, price, filter_zero_volume)
stock_series = pd.Series(stock_df['PETR3'])

# expected test data
expected_consumption_returns = [-0.054097227421539, 0.0292155610448024, 0.0319678511168648, 0.0329685698071112,
                                -0.0546535539683921, 0.0201643434102783, -0.0169725117825017, 0.0327810115127924,
                                -0.0336007956822177, 0.0247503971739045, 0.00284065881209461, 0.022092596621019,
                                -0.0498659181684449, 0.00427448243028294, 0.0162034330949163, 0.0352668126927131,
                                -0.0467622244159402, 0.0259453772576057, 0.0332383029653931, 0.0516805852925444]


expected_stock_series_returns = [0.11551277037405, 0.26437047782665, 0.04029370542712, -0.13301578875244,
                                 0.06001798863794, 0.19891731604171, -0.0954819856253, 0.0287969197596,
                                 0.15515727832734, -0.16327326091063, -0.27668433322043, 0.17575227962449,
                                 -0.03881173889708, 0.06118676917771, 0.15846270334252, 0.22507784957999,
                                 0.14425385957094, -0.11661546358603, 0.1953275562202, 0.01800157824392]

expected_stock_df_returns_petr3 = [0.11551277037405, 0.26437047782665, 0.04029370542712, -0.13301578875244,
                                 0.06001798863794, 0.19891731604171, -0.0954819856253, 0.0287969197596,
                                 0.15515727832734, -0.16327326091063, -0.27668433322043, 0.17575227962449,
                                 -0.03881173889708, 0.06118676917771, 0.15846270334252, 0.22507784957999,
                                 0.14425385957094, -0.11661546358603, 0.1953275562202, 0.01800157824392]


expected_stock_df_returns_bovespa = [0.0512342258976193, -0.0304382842624718, -0.0703564149028767, -0.0194697564232271,
                                     -0.0661259500744823, 0.0405787777628525, -0.313690103816813, 0.256980345155599,
                                     -0.0454975802971972, -0.189789784604852, -0.233595522841326, 0.224964519595146,
                                     -0.0287644957060251, 0.112555069570494, 0.186188871580722, 0.293575762833895,
                                     -0.0135916147966204, -0.0684347697749846, 0.0850851549723612, 0.0968880004855417]

expected_stock_df_returns_abev3 = [0.152336182163651, 0.228591792421808, 0.304932002660492, 0.409730330401345,
                                  0.0416708296182645, 0.0415497790196211, -0.396161888850265, 1.74966458374024,
                                  -0.0676588925676219, -0.0416757028508958, 0.0104738352017255, 0.19072467927402,
                                  0.010580109109104, 0.0201991398634884, 0.172355914369492, 0.0239204648478252,
                                  0.37129451342177, 0.233615541930205, 0.0248981485967356, 1.73216573530158]


class TestCalculateReturns(unittest.TestCase):

    def test_calculate_consumption_return(self):

        consumption_return = calculate_returns.calculate_consumption_return(consumption)

        self.assertGreaterEqual(consumption_return.shape[0], 76)
        self.assertEqual(sum([round(i - j, 12) for i, j in zip(consumption_return['Consumption'],
                                                               expected_consumption_returns)]), 0)
        self.assertEqual(consumption_return.columns[0], 'Consumption')
        self.assertEqual(consumption_return.index[0], '1º trimestre 2000')
        self.assertEqual(consumption_return.dtypes[0], 'float64')

    def test_calculate_stock_returns_series(self):

        stock_series_return = calculate_returns.calculate_stock_returns_series(stock_series)

        # It is valid to note that, even though we have entered as argument a pandas Series, the function will return
        # as a pandas DataFrame

        self.assertGreaterEqual(stock_series_return.shape[0], 76)
        self.assertEqual(sum([round(i-j,12) for i,j in zip(stock_series_return['PETR3'],
                                                           expected_stock_series_returns)]), 0)
        self.assertEqual(stock_series_return.columns[0], 'PETR3')
        self.assertEqual(stock_series_return.index[0], '1º trimestre 2000')
        self.assertEqual(stock_series_return.dtypes[0], 'float64')

    def test_calculate_stock_returns_df(self):
        stock_df_returns = calculate_returns.calculate_stock_returns_df(stock_df)

        # It is valid to note that, even though we have entered as argument a pandas Series, the function will return
        # as a pandas DataFrame

        self.assertGreaterEqual(stock_df_returns.shape[0], 76)
        self.assertEqual(sum([round(i - j, 12) for i, j in zip(stock_df_returns['PETR3'],
                                                               expected_stock_df_returns_petr3)]), 0)
        self.assertEqual(sum([round(i - j, 12) for i, j in zip(stock_df_returns['Bovespa'],
                                                               expected_stock_df_returns_bovespa)]), 0)
        self.assertEqual(sum([round(i - j, 12) for i, j in zip(stock_df_returns['ABEV3'],
                                                               expected_stock_df_returns_abev3)]), 0)
        self.assertEqual(stock_df_returns.columns[0], 'PETR3')
        self.assertEqual(stock_df_returns.columns[1], 'Bovespa')
        self.assertEqual(stock_df_returns.columns[2], 'ABEV3')
        self.assertEqual(stock_df_returns.index[0], '1º trimestre 2000')
        self.assertEqual(stock_df_returns.dtypes[0], 'float64')






