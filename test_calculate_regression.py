import unittest
import calculate_returns
import calculate_regression
import query_db
import pandas as pd


# Program config
db_path = 'ccapm_regression_database.db' # path to the Database
price = 'adjclose' # can be 'adjclose' or 'close'
filter_zero_volume = False # filter stock prices where volume = 0, it is advised to set True
ticker = ['PETR3', 'Bovespa']
price = 'adjclose'
filter_zero_volume = False

# Test Data Input
consumption = query_db.query_household_consumption_index(db_path)
stock_df = query_db.query_stock_prices(db_path, ticker, price, filter_zero_volume)
stock_series = pd.Series(stock_df['PETR3'])

# Regression Input Data

consumption_return = calculate_returns.calculate_consumption_return(consumption)
stock_df_returns = calculate_returns.calculate_stock_returns_df(stock_df)

# Test Residues Data

expected_resid_petr3 = [0.07052, 0.2489327, 0.02583, -0.147122, 0.01483, 0.1802684, -0.1273064, 0.01462, 0.1174335,
                    -0.1802952, -0.3014794, 0.1577874, -0.08231, 0.0369, 0.1384085, 0.211787, 0.1018606, -0.1332134,
                    0.181317, 0.01053, 0.08983, 0.0008268, 0.2583258, 0.01218, 0.05555, -0.01155, -0.09386, 0.1841479,
                    -0.111799, 0.1171238, 0.1232297, 0.3917842, -0.1883271, 0.1957553, -0.3018062, -0.4376677,
                    0.1282729, 0.08982, 0.0325, 0.02346]

expected_resid_bovespa = [0.05471, -0.05419, -0.095, -0.04444, -0.06247, 0.01979, -0.3223467, 0.2320668, -0.04872,
                      -0.2120793, -0.2487261, 0.2035434, -0.02667, 0.09696, 0.166692, 0.26785, -0.01251, -0.09111,
                      0.06002, 0.0658, 0.03906, -0.08754, 0.197609, 0.02126, 0.1246054, -0.07909, -0.04685, 0.1551936,
                      0.005506, 0.1532066, 0.06696, -0.006902, -0.03365, 0.01232, -0.2716675, -0.2987463, 0.01692,
                      0.1756523, 0.1502633, 0.1020412]

# Test Regression Parameters Data
# it follows the reg_df column order
# the parameters were calculated using Gretl

expected_params_petr3 = [0.0258029, 0.0211389, 0.226, -0.354781, 0.602433, 0.5577, 0.004603, -0.008669, 0.346819,
                         0.557689, 2.218187]


expected_params_bovespa = [0.0142024, 0.0146125, 0.3342, 0.326749, 0.41644, 0.4351, 0.008142, -0.005083, 0.615636,
                           0.435145, 1.976528]


class TestCalculateRegression(unittest.TestCase):

    def test_calculate_linear_regression(self):

        reg_dict, resid_df = calculate_regression.calculate_linear_regression(stock_df_returns, consumption_return)

        self.assertGreaterEqual(len(reg_dict), 2)
        self.assertEqual(sum([round(i - j, 5) for i, j in zip(resid_df['PETR3'],
                                                               expected_resid_petr3)]), 0)
        self.assertEqual(sum([round(i - j, 5) for i, j in zip(resid_df['Bovespa'],
                                                              expected_resid_bovespa)]), 0)

    def test_generate_regression_dataframe(self):

        reg_dict, resid_df = calculate_regression.calculate_linear_regression(stock_df_returns, consumption_return)

        reg_df = calculate_regression.generate_regression_dataframe(reg_dict)

        self.assertEqual(sum([round(i - j, len(str(j))-3) for i, j in zip(reg_df.loc['PETR3', :].to_list(),
                                                              expected_params_petr3)]), 0)
        self.assertEqual(sum([round(i - j, len(str(j))-3) for i, j in zip(reg_df.loc['Bovespa', :].to_list(),
                                                                            expected_params_bovespa)]), 0)













