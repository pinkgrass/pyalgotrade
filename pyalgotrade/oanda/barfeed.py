# PyAlgoTrade
#
# Copyright 2011-2015 Gabriel Martin Becedillas Ruiz
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
.. moduleauthor:: Gabriel Martin Becedillas Ruiz <gabriel.becedillas@gmail.com>
"""

from pyalgotrade.barfeed import csvfeed
from pyalgotrade import bar
from pyalgotrade import dataseries

OANDA_FREQUENCY = {
    bar.Frequency.TRADE: 'all', # The bar represents a single trade.
    bar.Frequency.SECOND: 'second', # The bar summarizes the trading activity during 1 second.
    bar.Frequency.MINUTE: 'M1', # The bar summarizes the trading activity during 1 minute.
    bar.Frequency.HOUR: 'H1', # The bar summarizes the trading activity during 1 hour.
    bar.Frequency.DAY: 'D', #The bar summarizes the trading activity during 1 day.
    bar.Frequency.WEEK: 'W', #The bar summarizes the trading activity during 1 week.
    bar.Frequency.MONTH: 'M' # The bar summarizes the trading activity during 1 month.
}


class Feed(csvfeed.GenericBarFeed):
    """A :class:`pyalgotrade.barfeed.csvfeed.BarFeed` that loads bars from CSV files downloaded from OANDA.

    example CSV
    [u'complete', u'closeMid', u'highMid', u'lowMid', u'volume', u'openMid', u'time']
    [True, 1.70403, 1.70407, 1.704025, 8, 1.70407, u'2014-06-19T15:47:30.000000Z']

    :param frequency: The frequency of the bars. Only **pyalgotrade.bar.Frequency.DAY** or **pyalgotrade.bar.Frequency.WEEK**
        are supported.
    :param timezone: The default timezone to use to localize bars. Check :mod:`pyalgotrade.marketsession`.
    :type timezone: A pytz timezone.
    :param maxLen: The maximum number of values that the :class:`pyalgotrade.dataseries.bards.BarDataSeries` will hold.
        Once a bounded length is full, when new items are added, a corresponding number of items are discarded from the opposite end.
    :type maxLen: int.

    .. note::
        When working with multiple instruments:

            * If all the instruments loaded are in the same timezone, then the timezone parameter may not be specified.
            * If any of the instruments loaded are in different timezones, then the timezone parameter must be set.
    """

    def __init__(self, frequency=bar.Frequency.MINUTE, timezone=None, maxLen=dataseries.DEFAULT_MAX_LEN):
        if frequency not in OANDA_FREQUENCY:
            raise Exception("barfeed.py - Invalid frequency")

        csvfeed.GenericBarFeed.__init__(self, frequency, timezone, maxLen)

        self.setDateTimeFormat("%Y-%m-%dT%H:%M:%S.000000Z")
        self.setColumnName("datetime", "time")
        self.setColumnName("open", "openMid")
        self.setColumnName("high", "highMid")
        self.setColumnName("low", "lowMid")
        self.setColumnName("volume", "volume")
        self.setColumnName("close", "closeMid")
        self.setNoAdjClose()


if __name__ == '__main__':
    # DEBUG PURPOSES ONLY - please ignore
    ret = Feed()
    ret.addBarsFromCSV('GBP_USD', '../../samples/data/oanda.csv')
    for datetime, volume in ret:
        print datetime, volume
