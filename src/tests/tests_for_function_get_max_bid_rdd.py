"""
This module contains the tests for the function get_max_bid_rdd from the module bids.
"""
import unittest
from pyspark import SparkContext, SparkConf
from bids import get_max_bid_rdd, get_clear_rdd
from bid_classes import Bid, CountryBid


class Tests(unittest.TestCase):
    def setUp(self):
        self.conf = SparkConf().setMaster('local').setAppName('testing')
        self.sc = SparkContext(conf=self.conf)
        self.first_test_set = \
            (['1,05-26-02-2016,0.63,1.23,1.32,1.38,0.50,0.99,0.51,1.72,,1.32,0.57,1.47,0.85,1.89,0.51,1.98'])
        self.first_test_rdd = get_clear_rdd(self.sc.parallelize(self.first_test_set))

    def test_first(self):
        result_rdd = get_max_bid_rdd(self.first_test_rdd)
        self.assertEqual(result_rdd.collect(), [CountryBid(Bid(self.first_test_set[0]), 'US')])
        self.sc.stop()


if __name__ == '__main__':
    unittest.main()
