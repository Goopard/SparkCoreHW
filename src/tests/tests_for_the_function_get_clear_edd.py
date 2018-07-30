"""
This module contains the tests for the function get_clear_rdd from the module bids.
"""
import unittest
from pyspark import SparkContext, SparkConf
from bids import get_clear_rdd
from bid_classes import Bid, CountryBid


class Tests(unittest.TestCase):
    def setUp(self):
        self.conf = SparkConf().setMaster('local').setAppName('testing')
        self.sc = SparkContext(conf=self.conf)
        self.first_test_set = (['0000006,05-26-02-2016,0.63,1.23,1.32,1.38,0.50,0.99,0.51,1.72,,1.32,0.57,1.47,0.85,1.89,0.51,1.98',
                                '0000008,18-25-02-2016,1.08,1.81,1.88,0.44,1.95,0.55,0.74,1.61,1.68,1.10,,1.18,1.22,0.47,1.12,1.13',
                                '0000004,08-26-02-2016,ERROR_BAD_REQUEST',
                                '0000006,11-25-02-2016,ERROR_BID_SERVICE_TIMEOUT',
                                '0000006,02-26-02-2016,1.27,0.91,,1.11,0.38,1.88,2.01,0.84,1.96,0.99,0.89,0.86,1.79,2.02,1.62,0.87',
                                '0000008,22-25-02-2016,1.66,0.88,2.09,1.55,0.49,1.81,1.93,1.74,0.99,1.05,2.00,1.34,1.64,1.61,0.44,1.63',
                                '0000004,07-26-02-2016,ERROR_BID_SERVICE_UNAVAILABLE'])
        self.second_test_set = (['0000006,21-28-02-2016,0.84,1.01,1.94,,0.78,1.41,0.66,0.56,1.06,1.85,0.37,1.81,1.96,1.83,1.11,0.77',
                                 '0000006,10-28-02-2016,,0.77,0.81,1.57,1.32,1.84,0.52,0.39,1.98,0.45,0.97,,1.62,1.15,1.11,1.42',
                                 '0000003,22-27-02-2016,ERROR_BAD_REQUEST',
                                 '0000006,08-28-02-2016,1.98,0.48,1.78,,1.37,0.33,1.45,1.32,1.30,1.40,1.95,1.99,1.55,1.80,1.12,1.15'])
        self.first_test_rdd = self.sc.parallelize(self.first_test_set)
        self.second_test_rdd = self.sc.parallelize(self.second_test_set)

    def test_first(self):
        result_rdd = get_clear_rdd(self.first_test_rdd)
        self.assertEqual(result_rdd.collect(), [CountryBid(Bid(self.first_test_set[0]), 'US'),
                                                CountryBid(Bid(self.first_test_set[0]), 'MX'),
                                                CountryBid(Bid(self.first_test_set[0]), 'CA'),
                                                CountryBid(Bid(self.first_test_set[1]), 'US'),
                                                CountryBid(Bid(self.first_test_set[1]), 'MX'),
                                                CountryBid(Bid(self.first_test_set[1]), 'CA'),
                                                CountryBid(Bid(self.first_test_set[4]), 'US'),
                                                CountryBid(Bid(self.first_test_set[4]), 'MX'),
                                                CountryBid(Bid(self.first_test_set[4]), 'CA'),
                                                CountryBid(Bid(self.first_test_set[5]), 'US'),
                                                CountryBid(Bid(self.first_test_set[5]), 'MX'),
                                                CountryBid(Bid(self.first_test_set[5]), 'CA')])
        self.sc.stop()

    def test_second(self):
        result_rdd = get_clear_rdd(self.second_test_rdd)
        self.assertEqual(result_rdd.collect(), [CountryBid(Bid(self.second_test_set[0]), 'MX'),
                                                CountryBid(Bid(self.second_test_set[0]), 'CA'),
                                                CountryBid(Bid(self.second_test_set[1]), 'US'),
                                                CountryBid(Bid(self.second_test_set[1]), 'MX'),
                                                CountryBid(Bid(self.second_test_set[1]), 'CA'),
                                                CountryBid(Bid(self.second_test_set[3]), 'MX'),
                                                CountryBid(Bid(self.second_test_set[3]), 'CA')])
        self.sc.stop()


if __name__ == '__main__':
    unittest.main()
