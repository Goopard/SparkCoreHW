"""
This module contains the tests for the function get_errors_rdd from the module bids.
"""
import unittest
from pyspark import SparkContext, SparkConf
from bids import get_errors_rdd
from bid_classes import ErrorBidWithFrequency


class Tests(unittest.TestCase):
    def setUp(self):
        self.conf = SparkConf().setMaster('local').setAppName('testing')
        self.sc = SparkContext(conf=self.conf)
        self.first_test_rdd = self.sc.parallelize(['0000002,14-29-04-2016,ERROR_BID_SERVICE_UNAVAILABLE',
                                                   '0000002,14-29-04-2016,ERROR_BID_SERVICE_UNAVAILABLE',
                                                   '0000002,14-29-04-2016,ERROR_BID_SERVICE_UNAVAILABLE'])
        self.second_test_rdd = self.sc.parallelize(['0000002,14-29-04-2016,ERROR_BID_SERVICE_UNAVAILABLE',
                                                    '0000002,14-29-04-2016,ERROR_BID_SERVICE_UNAVAILABLE',
                                                    '0000005,04-08-02-2016,ERROR_ACCESS_DENIED',
                                                    '0000005,04-08-02-2016,ERROR_ACCESS_DENIED'])

    def test_3_same_errors(self):
        result_rdd = get_errors_rdd(self.first_test_rdd)
        self.assertEqual(result_rdd.collect(), [ErrorBidWithFrequency('2016-04-29 14:00',
                                                                      'ERROR_BID_SERVICE_UNAVAILABLE', 3)])
        self.sc.stop()

    def test_some_different_errors(self):
        result_rdd = get_errors_rdd(self.second_test_rdd)
        self.assertEqual(result_rdd.collect(),
                         [ErrorBidWithFrequency('2016-04-29 14:00', 'ERROR_BID_SERVICE_UNAVAILABLE', 2),
                          ErrorBidWithFrequency('2016-02-08 04:00', 'ERROR_ACCESS_DENIED', 2)])
        self.sc.stop()


if __name__ == '__main__':
    unittest.main()
