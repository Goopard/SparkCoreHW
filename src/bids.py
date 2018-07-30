import os
from bid_classes import ErrorBid, Bid, CountryBid, CountryBidWithName, Exchange, ErrorBidWithFrequency, Motel
from pyspark import SparkConf, SparkContext
from operator import attrgetter
from functools import partial


os.environ['JAVA_HOME'] = 'C:\\Progra~1\\Java\\jdk1.8.0_181'
os.environ['HADOOP_HOME'] = 'C:\\hadoop'


def get_errors_rdd(rdd):
    """This functions finds all the erroneous records in the given rdd, cast those records to the ErrorBid class, than
    groups them by the date and code, then counts the frequency for each error code in each hour, and returns the result
    as rdd.

    :param rdd: The input rdd with some bids.
    :type rdd: RDD.
    :return: RDD -- the required list of errors with the frequencies in each hour.
    """
    return rdd.filter(lambda line: 'ERROR' in line).map(ErrorBid)\
        .groupBy(lambda error: (error.date, error.error_code)).mapValues(len).map(ErrorBidWithFrequency)


def collect_countries(bid):
    """This is a function which simply turns a class Bid instance into a list of three CountryBid instances.

    :param bid: Some given instance of class Bid.
    :type bid: Bid.
    :return: list(CountryBid) -- the required CountryBid instances.
    """
    return [CountryBid(bid, 'US'), CountryBid(bid, 'MX'), CountryBid(bid, 'CA')]


def get_clear_rdd(raw_unclear_rdd):
    """This function takes some raw bids rdd with errors, filters out all the erroneous records, casts the rest of the
    records to the class RawBid, then to the class Bid, then splits each of those instances of the class Bid into three
    instances of the class CountryBid, and, at last, filters out all the CountryBids with an incorrect price.

    :param raw_unclear_rdd: Some raw bids rdd.
    :type raw_unclear_rdd: RDD.
    :return: RDD - required error-clear rdd with only the CountryBids left.
    """
    return raw_unclear_rdd.filter(lambda line: 'ERROR' not in line).map(Bid).flatMap(collect_countries)\
        .filter(attrgetter('price'))


def get_eur_prices_rdd(rdd, path_to_exchange, spark_context):
    """This function converts all the bid prices in the given rdd from dollar to euro.

    :param rdd: Given bid rdd.
    :type rdd: RDD.
    :param path_to_exchange: Path to the file with exchange rates.
    :type path_to_exchange: str.
    :param spark_context: Spark context to use.
    :type spark_context: SparkContext.
    :return: RDD.
    """
    def bid_to_eu(bid):
        """This function simply converts bid's price from dollar to euro.

        :param bid: Input bid.
        :type bid: CountryBid.
        :return: CountryBid.
        """
        bid.price = bid.price * exchange.value[bid.date]
        return bid

    exchange_rdd = spark_context.textFile(path_to_exchange).map(Exchange)
    exchange = spark_context.broadcast({ex.date: ex.rate for ex in exchange_rdd.collect()})
    return rdd.map(bid_to_eu)


def get_motels_names_rdd(rdd, path_to_motels, spark_context):
    """This function enriches the bids rdd with the names of the motels.

    :param rdd: Bids rdd to enrich.
    :type rdd: RDD.
    :param path_to_motels: Path to the file with the motels information.
    :type path_to_motels: str.
    :param spark_context: Spark context to use.
    :type spark_context: SparkContext.
    :return: RDD.
    """
    motels_rdd = spark_context.textFile(path_to_motels).map(Motel)
    motels_dict = {motel.id: motel.name for motel in motels_rdd.collect()}
    motels = spark_context.broadcast(motels_dict)
    return rdd.map(lambda country_bid: CountryBidWithName(country_bid, motels.value[country_bid.motel_id]))


def find_max(group):
    """This function is used to return the CountryBidWithName with a biggest price from a group.
     :param group: Some group of CountryBidWithName objects.
    :type group: tuple.
    :return: CountryBidWithName -- the required most expensive bid.
    """
    return max(group[1], key=attrgetter('price'))


def get_max_bid_rdd(rdd):
    """This function leaves only the most expensive bids per date and motel from some given rdd.

    :param rdd: Some rdd of CountryBidWithName objects.
    :type rdd: RDD.
    :return: RDD - the required rdd with only the most expensive bids.
    """
    return rdd.groupBy(lambda bid: bid.motel_id + ',' + bid.date).map(find_max)


if __name__ == '__main__':
    conf = SparkConf().setMaster('local').setAppName('bids')
    conf.set('spark.submit.pyFiles', '/usr/bid_classes.py')
    conf.set('spark.executor.memory', '1g')
    conf.set('spark.driver.memory', '1g')
    sc = SparkContext(conf=conf)
    sc.addPyFile('/usr/bid_classes.py')

    raw_unclear_bids = sc.textFile('/user/raj_ops/bid_data/bids.txt')

    error_bids = get_errors_rdd(raw_unclear_bids)
    error_bids.saveAsTextFile('/user/raj_ops/error_bids')

    bids = get_clear_rdd(raw_unclear_bids)
    bids = get_eur_prices_rdd(bids, '/user/raj_ops/bid_data/exchange_rate.txt', sc)
    bids.saveAsTextFile('/user/raj_ops/bids_euro')

    bids = get_max_bid_rdd(bids)
    bids = get_motels_names_rdd(bids, '/user/raj_ops/bid_data/motels.txt', sc)
    bids.saveAsTextFile('/user/raj_ops/bids')
