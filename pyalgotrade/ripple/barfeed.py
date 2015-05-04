# -*- coding: utf-8 -*-
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
.. moduleauthor:: Maciej Żok <maciek.zok@gmail.com>
.. moduleauthor:: Richard Crook <richard@pinkgrass.org>
"""

from pyalgotrade.barfeed import csvfeed
from pyalgotrade.barfeed import common
from pyalgotrade.utils import dt
from pyalgotrade import bar
from pyalgotrade import dataseries

import datetime


######################################################################
# Ripple Charts CSV parser
# Each bar must be on its own line and fields must be separated by comma (,).
#
# Bars Format:
# startTime, baseVolume, counterVolume, count, open, high, low, close, vwap, openTime, closeTime
#
# adjclose is set to vwap
#
# TO DO: Refactor to use genericbarfeed, remove trailing space in delimiter.


RIPPLECHART_FREQUENCY = {
    bar.Frequency.TRADE: 'all', # The bar represents a single trade.
    bar.Frequency.SECOND: 'second', # The bar summarizes the trading activity during 1 second.
    bar.Frequency.MINUTE: 'minute', # The bar summarizes the trading activity during 1 minute.
    bar.Frequency.HOUR: 'hour', # The bar summarizes the trading activity during 1 hour.
    bar.Frequency.DAY: 'day', #The bar summarizes the trading activity during 1 day.
    #bar.Frequency.WEEK:  The bar summarizes the trading activity during 1 week.
    bar.Frequency.MONTH: 'month' # The bar summarizes the trading activity during 1 month.
}

class RowParser(csvfeed.RowParser):
    def __init__(self, dailyBarTime, frequency, timezone=None, sanitize=False):
        self.__dailyBarTime = dailyBarTime
        self.__frequency = frequency
        self.__timezone = timezone
        self.__sanitize = sanitize

    def __parseDate(self, dateString):
        ret = datetime.datetime.strptime(dateString,'%Y-%m-%dT%H:%M:%S+00:00')
        # Time on Google Finance CSV files is empty. If told to set one, do it.
        if self.__dailyBarTime is not None:
            ret = datetime.datetime.combine(ret, self.__dailyBarTime)
        # Localize the datetime if a timezone was given.
        if self.__timezone:
            ret = dt.localize(ret, self.__timezone)
        return ret

    def getFieldNames(self):
        # It is expected for the first row to have the field names.
        return None

    def getDelimiter(self):
        return ","

    def parseBar(self, csvRowDict):
        dateTime = self.__parseDate(csvRowDict["startTime"])
        close = float(csvRowDict[" close"])
        open_ = float(csvRowDict[" open"])
        high = float(csvRowDict[" high"])
        low = float(csvRowDict[" low"])
        volume = float(csvRowDict[" counterVolume"])
        adjClose = float(csvRowDict[" vwap"]) #

        if self.__sanitize:
            open_, high, low, close = common.sanitize_ohlc(open_, high, low, close)

        return bar.BasicBar(dateTime, open_, high, low, close, volume,
                            adjClose, self.__frequency)


class Feed(csvfeed.BarFeed):
    """A :class:`pyalgotrade.barfeed.csvfeed.BarFeed` that loads bars from CSV files downloaded from Google Finance.

    :param frequency: The frequency of the bars.
    :param timezone: The default timezone to use to localize bars. Check :mod:`pyalgotrade.marketsession`.
    :type timezone: A pytz timezone.
    :param maxLen: The maximum number of values that the :class:`pyalgotrade.dataseries.bards.BarDataSeries` will hold.
        Once a bounded length is full, when new items are added, a corresponding number of items are discarded from the opposite end.
    :type maxLen: int.

    """

    def __init__(self, frequency=bar.Frequency.HOUR, timezone=None, maxLen=dataseries.DEFAULT_MAX_LEN):
        if frequency not in RIPPLECHART_FREQUENCY:
                    raise Exception("Invalid frequency.")

        csvfeed.BarFeed.__init__(self, frequency, maxLen)
        #self.setColumnName("adj_close", "vwap")
        self.__timezone = timezone
        self.__sanitizeBars = True

    def sanitizeBars(self, sanitize):
        self.__sanitizeBars = sanitize

    def barsHaveAdjClose(self):
        return True

    def addBarsFromCSV(self, instrument, path, timezone=None):
        """Loads bars for a given instrument from a CSV formatted file.
        The instrument gets registered in the bar feed.

        :param instrument: Instrument identifier.
        :type instrument: string.
        :param path: The path to the CSV file.
        :type path: string.
        :param timezone: The timezone to use to localize bars. Check :mod:`pyalgotrade.marketsession`.
        :type timezone: A pytz timezone.
        """

        if timezone is None:
            timezone = self.__timezone

        rowParser = RowParser(self.getDailyBarTime(), self.getFrequency(), timezone, self.__sanitizeBars)
        csvfeed.BarFeed.addBarsFromCSV(self, instrument, path, rowParser)

if __name__ == '__main__':
    # DEBUG PURPOSES ONLY - please ignore
    ret = Feed()
    ret.addBarsFromCSV('BTC.E2q/XRP', '../../samples/data/BTC.E2q_XRP-2014-ripplecharts.csv')
