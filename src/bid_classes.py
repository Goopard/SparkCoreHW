"""
This module contains all the domain classes for the module bids.py.
"""


def get_date(raw_date):
    """This function takes the date in format HH-DD-MM-YYYY and casts it to YYYY-MM-DD HH:00 format.

    :param raw_date: date in format HH-DD-MM-YYYY.
    :type raw_date: str.
    :return: str -- date in required format.
    """
    values = raw_date.split('-')
    return '-'.join([values[3], values[2], values[1]]) + ' ' + values[0] + ':00'


class Bid:
    """
    This class embodies the bid item, which contains only these fields: motel_id, date, US, MX, CA.
    """
    def __init__(self, line):
        values = line.split(',')
        self.motel_id = values[0]
        self.date = get_date(values[1])
        self.US = values[5]
        self.MX = values[6]
        self.CA = values[8]

    def __repr__(self):
        return ','.join(self.__dict__.values())


class CountryBid:
    """
    This class embodies a bid with the information on only one country: motel_id, date, country.
    """
    def __init__(self, bid, country):
        self.motel_id = bid.motel_id
        self.date = bid.date
        self.country = country
        try:
            self.price = float(getattr(bid, 'country'))
        except ValueError:
            self.price = None

    def __repr__(self):
        return ','.join([self.motel_id, self.date, self.country, '{0:.3f}'.format(self.price) if self.price else ''])

    def __eq__(self, other):
        return self.motel_id == other.motel_id and self.date == other.date and self.country == other.country


class CountryBidWithName:
    """
    This class embodies a bid with the information on only one country, but also with a name of the motel: motel_id,
    name, date, country.
    """
    def __init__(self, country_bid, name):
        self.motel_id = country_bid.motel_id
        self.date = country_bid.date
        self.price = country_bid.price
        self.country = country_bid.country
        self.name = name

    def __repr__(self):
        return ','.join([self.motel_id, self.name, self.date, self.country, '{0:.3f}'.format(self.price)])

    def __eq__(self, other):
        return self.motel_id == other.motel_id and self.date == other.date and self.country == other.country \
               and self.name == other.name


class ErrorBid:
    """
    This class embodies the erroneous bid, which contains fields date and error_code.
    """
    def __init__(self, line):
        values = line.split(',')
        self.date = get_date(values[1])
        self.error_code = values[2]

    def __repr__(self):
        return ','.join([self.date, self.error_code])


class ErrorBidWithFrequency:
    """
    This class embodies the group of erroneous bids, which contains fields date, error_code and frequency (amount of
    such bids).
    """
    def __init__(self, date, code, frequency):
        self.date = date
        self.error_code = code
        self.frequency = frequency

    def __repr__(self):
        return ','.join([self.date, self.error_code, str(self.frequency)])

    def __eq__(self, other):
        return self.error_code == other.error_code and self.date == other.date and self.frequency == other.frequency


class Exchange:
    """
    This class embodies the exchange rate item, which contains fields date and rate.
    """
    def __init__(self, line):
        values = line.split(',')
        self.date = get_date(values[0])
        self.rate = float(values[3])

    def __repr__(self):
        return ','.join([self.date, str(self.rate)])


class Motel:
    """
    This class embodies the motel item, which contains fields id and name.
    """
    def __init__(self, line):
        values = line.split(',')
        self.id = values[0]
        self.name = values[1]

    def __repr__(self):
        return ','.join([self.id, self.name])
