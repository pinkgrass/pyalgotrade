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
.. moduleauthor:: Richard Crook <richard@pinkgrass.org>
"""

import urllib2
import urllib
import os
import datetime
import json
import csv

import pyalgotrade.logger
from pyalgotrade import bar
from pyalgotrade.oanda import barfeed

OANDA_FREQUENCY = {
    bar.Frequency.TRADE: 'all', # The bar represents a single trade.
    bar.Frequency.SECOND: 'second', # The bar summarizes the trading activity during 1 second.
    bar.Frequency.MINUTE: 'M1', # The bar summarizes the trading activity during 1 minute.
    bar.Frequency.HOUR: 'H1', # The bar summarizes the trading activity during 1 hour.
    bar.Frequency.DAY: 'D', #The bar summarizes the trading activity during 1 day.
    bar.Frequency.WEEK: 'W', #The bar summarizes the trading activity during 1 week.
    bar.Frequency.MONTH: 'M' # The bar summarizes the trading activity during 1 month.
}

def get_instruments():
    '''
    # https://api-sandbox.oanda.com/v1/instruments
    # Returns list of instruments available with key:
    #
    # http://developer.oanda.com/rest-live/rates/#getInstrumentList
    # 
    # example:
   [
    {
      "instrument" : "AUD_CAD",
      "displayName" : "AUD\/CAD",
      "pip" : "0.0001",
      "maxTradeUnits" : 10000000
    },
    ]
    '''

    req = urllib2.Request('https://api-sandbox.oanda.com/v1/instruments')
    f = urllib2.urlopen(req)

    if f.getcode() != 200:
        raise Exception("Failed to download data: %s" % f.getcode())

    response = json.loads(f.read())
    instruments = response['instruments']

    markets = {}
    for instrument in instruments:
        markets[instrument['instrument']] = instrument

    return markets

def download_json(instrument, begin, end, frequency=bar.Frequency.HOUR, authToken=None):
    '''
    https://api-fxtrade.oanda.com/v1/candles?instrument=EUR_USD&start=2014-06-19T15%3A47%3A40Z&end=2014-06-19T15%3A47%3A50Z
    See docs:
    http://developer.oanda.com/rest-live/rates/#retrieveInstrumentHistory


    '''
    if instrument not in get_instruments():
        raise Exception('%s is not in instruments list' % instrument)

    params = {
        'instrument': instrument,
        'start': begin.isoformat('T')+'Z', #2014-06-19T15:47:50Z RFC3339 format
        'end': end.isoformat('T')+'Z',
        'granularity': OANDA_FREQUENCY[frequency],
        'candleFormat': 'midpoint'
    }
    if authToken is not None:
        params["auth_token"] = authToken

    print 'OANDA SANDBOX : Market data is simulated' 
    url = 'https://api-sandbox.oanda.com/v1/candles'
    url += '?' + urllib.urlencode(params)

    #req = urllib2.Request('https://api-sandbox.oanda.com/v1/instruments')
    f = urllib2.urlopen(url)

    if f.getcode() != 200:
        raise Exception("Failed to download data: %s" % f.getcode())

    response = json.loads(f.read())
    f.close()
    candles = response['candles']

    return candles

def download_bars(instrument, begin, end, frequency, csvFile):
    bars = download_json(instrument, begin, end, frequency)

    with open(csvFile, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(bars[0].keys())  # header row
        for candle in bars:
            writer.writerow(candle.values())

def download_recent_bars(instrument, periods, frequency, step, csvFile):
    """Download most recent bars from Oanda for a given frequency and number of days
    """
    end = datetime.datetime.now() - datetime.timedelta(seconds=frequency) # Last period, not present
    begin = end - datetime.timedelta(seconds=((periods + 1) * frequency)) # X periods

    bars = download_json(instrument, begin, end, frequency)
    with open(csvFile, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(bars[0].keys())  # header row
        for candle in bars:
            writer.writerow(candle.values())

def download_daily_bars(instrument, year, csvFile):
    """Download daily bars from Ripple Charts for a given year.
    """
    begin = datetime.datetime(year=year,month=1,day=1)
    end = datetime.datetime(year=year,month=12,day=31)
    bars = download_json(instrument, begin, end, bar.Frequency.DAY)
    with open(csvFile, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(bars[0].keys())  # header row
        for candle in bars:
            writer.writerow(candle.values())


def download_hourly_bars(instrument, year, csvFile):
    """Download hourly bars from Ripple Charts for a given year.

    :param instrument : instrument dict 
    :param year: The year. int
    :param csvFile: The path to the CSV file to write. string
    """
    begin = datetime.datetime(year=year,month=1,day=1)
    end_H1 = datetime.datetime(year=year,month=6,day=30)
    begin_H2 = datetime.datetime(year=year,month=7,day=1)
    end = datetime.datetime(year=year,month=12,day=31)

    bars = download_json(instrument, begin, end_H1, bar.Frequency.HOUR)
    bars += download_json(instrument, begin_H2, end, bar.Frequency.HOUR)

    with open(csvFile, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(bars[0].keys())  # header row
        for candle in bars:
            writer.writerow(candle.values())

def build_feed_recent(instruments, periods=1, storage='.', frequency=bar.Frequency.HOUR, step=1, timezone=None, skipErrors=False):
    """Build and load a :class:`pyalgotrade.ripple.Feed` using CSV files downloaded from Ripple Charts.
    CSV files are downloaded if they haven't been downloaded before.

    :param instruments: Instrument. 
    :type instruments: list.
    :param days: number of days to build int
    :param storage: The path were the files will be loaded from, or downloaded to. string
    :param frequency: The frequency of the bars. 
    :param timezone: The default timezone to use to localize bars. Check :mod:`pyalgotrade.marketsession`.
    :type timezone: A pytz timezone.
    :param skipErrors: True to keep on loading/downloading files in case of errors.
    :type skipErrors: boolean.
    :rtype: :class:`pyalgotrade.ripple.Feed`.
    """

    logger = pyalgotrade.logger.getLogger("oanda")
    ret = barfeed.Feed(frequency, timezone)

    if not os.path.exists(storage):
        logger.info("Creating %s directory" % (storage))
        os.mkdir(storage)

    for instrument in instruments:
        fileName = os.path.join(storage, "%s-last-%d-%s-oanda.csv" % (instrument.replace('/','_'), periods, OANDA_FREQUENCY[frequency]))
        if not os.path.exists(fileName):
            logger.info("Downloading %s to %s" % (instrument, fileName))
            try:
                if frequency in OANDA_FREQUENCY:
                    download_recent_bars(instrument, periods, frequency, step, fileName)
                else:
                    raise Exception("recent feed: Invalid frequency")
            except Exception, e:
                if skipErrors:
                    logger.error(str(e))
                else:
                    raise e
        ret.addBarsFromCSV(instrument, fileName)
    return ret


def build_feed(instruments, fromYear, toYear, storage, frequency=bar.Frequency.DAY, timezone=None, skipErrors=False):
    """Build and load a :class:`pyalgotrade.ripple.Feed` using CSV files downloaded from Ripple Charts.
    CSV files are downloaded if they haven't been downloaded before.

    :param instrument: Instrument identifiers.
    :type instruments: list.
    :param fromYear: The first year.
    :type fromYear: int.
    :param toYear: The last year.
    :type toYear: int.
    :param storage: The path were the files will be loaded from, or downloaded to.
    :type storage: string.
    :param frequency: The frequency of the bars. Only **pyalgotrade.bar.Frequency.DAY** or **pyalgotrade.bar.Frequency.WEEK**
        are supported.
    :param timezone: The default timezone to use to localize bars. Check :mod:`pyalgotrade.marketsession`.
    :type timezone: A pytz timezone.
    :param skipErrors: True to keep on loading/downloading files in case of errors.
    :type skipErrors: boolean.
    :rtype: :class:`pyalgotrade.ripple.Feed`.
    """

    logger = pyalgotrade.logger.getLogger("ripple")
    ret = barfeed.Feed(frequency, timezone)

    if not os.path.exists(storage):
        logger.info("Creating %s directory" % (storage))
        os.mkdir(storage)

    for year in range(fromYear, toYear+1):
        for instrument in instruments:
            fileName = os.path.join(storage, "%s-%d-oanda.csv" % (instrument.replace('/','_'), year))
            if not os.path.exists(fileName):
                logger.info("Downloading %s %d to %s" % (instrument, year, fileName))
                try:
                    if frequency == bar.Frequency.DAY:
                        download_daily_bars(instrument, year, fileName)
                    elif frequency == bar.Frequency.HOUR:
                        download_hourly_bars(instrument, year, fileName)
                    else:
                        raise Exception("Invalid frequency")
                except Exception, e:
                    if skipErrors:
                        logger.error(str(e))
                        continue
                    else:
                        raise e
            ret.addBarsFromCSV(instrument, fileName)
    return ret


if __name__ == '__main__':
    import csv
    
    instruments = get_instruments()
    assert len(instruments) > 0

    instrument = 'GBP_USD'
    test_csvFile = '../../samples/data/' + instrument + '-sample-oanda.csv'
    start = datetime.datetime.strptime('2015-03-30','%Y-%m-%d')
    end = datetime.datetime.strptime('2015-03-31','%Y-%m-%d')
    
    # Test download from Oanda
    bars = download_json(instrument,start,end,bar.Frequency.HOUR)
    assert len(bars) == 24

    # Test download to CSV
    download_bars(instrument,start,end,bar.Frequency.HOUR,test_csvFile)
    with open(test_csvFile, 'rb') as f:
        reader = csv.reader(f)
        data = list(reader)
    assert len(data) == (24 + 1)
    
    # Test download_daily_bars
    download_daily_bars(instrument,2014,test_csvFile)
    with open(test_csvFile, 'rb') as f:
        reader = csv.reader(f)
        data = list(reader)
    assert len(data) == 355
    
    # Test download_hourly_bars
    download_hourly_bars(instrument,2014,test_csvFile)
    with open(test_csvFile, 'rb') as f:
        reader = csv.reader(f)
        data = list(reader)
    assert len(data) == 8287
    
    # Test download recent bars
    download_recent_bars(instrument, 90, bar.Frequency.DAY, 1, test_csvFile)
    with open(test_csvFile, 'rb') as f:
        reader = csv.reader(f)
        data = list(reader)
    assert len(data) == 93

    

    # Test build_feed
    feed = build_feed([instrument], 2014, 2015, '../../samples/data/')
    feed = build_feed(instruments.keys()[:2], 2014, 2015, '../../samples/data/')
    
    # Test build_feed_recent
    feed = build_feed_recent([instrument], periods=24, storage='../../samples/data/', frequency=bar.Frequency.HOUR)
    