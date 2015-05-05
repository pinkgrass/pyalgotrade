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
import os
import datetime
import json

import pyalgotrade.logger
from pyalgotrade import bar
from pyalgotrade.ripple import barfeed

RIPPLECHART_FREQUENCY = {
    bar.Frequency.TRADE: 'all', # The bar represents a single trade.
    bar.Frequency.SECOND: 'second', # The bar summarizes the trading activity during 1 second.
    bar.Frequency.MINUTE: 'minute', # The bar summarizes the trading activity during 1 minute.
    bar.Frequency.HOUR: 'hour', # The bar summarizes the trading activity during 1 hour.
    bar.Frequency.DAY: 'day', #The bar summarizes the trading activity during 1 day.
    #bar.Frequency.WEEK:  The bar summarizes the trading activity during 1 week.
    bar.Frequency.MONTH: 'month' # The bar summarizes the trading activity during 1 month.
}

def top_markets(startTime=None,endTime=None):
    '''
    # http://docs.rippledataapi.apiary.io/#reference/api-routes/topmarkets/post
    # Returns dict of markets available with key:
    # <Currency Code>[.<Last three chars of issuer ripple address>]/<Currency Code>[.<Last three chars of issuer ripple address>]
    # Last three chars of issuer ripple address will be missing if XRP is the currency
    #
    # See the ripplecharts api for details on data fields returned
    # http://docs.rippledataapi.apiary.io/#reference/api-routes/topmarkets/post
    # 
    # example:
    {u'JPY.PcN/XRP':
        {
        u'count': 2617,
        u'convertedAmount': 6073277.593270997,
        u'counter': {u'currency': u'XRP'},
        u'amount': 5965743.25122951,
        u'rate': 1.018025305064767, 
        u'base': {u'currency': u'JPY', u'issuer': u'r94s8px6kSw1uZ1MV98dhSRTvc6VMPoPcN'}
        }
    }

    '''
    data = {  
        'exchange' : {'currency': 'XRP'},
        'startTime' : startTime,
        'endTime' : endTime
    }

    req = urllib2.Request('http://api.ripplecharts.com/api/top_markets')
    req.add_header('Content-Type', 'application/json')
    f = urllib2.urlopen(req, json.dumps(data))

    if f.getcode() != 200:
        raise Exception("Failed to download data: %s" % f.getcode())

    response = json.loads(f.read())
    components = response['components']

    markets = {}
    for component in components:
        if 'issuer' in component['base']:
            symbol = component['base']['currency'] + '.' + component['base']['issuer'][-3:] + '/'
        else:
            symbol = component['base']['currency'] + '/'

        if 'issuer' in component['counter']:
            symbol += component['counter']['currency'] + '.' + component['counter']['issuer'][-3:]
        else:
            symbol += component['counter']['currency']
    
        if component['count'] > 0:
            markets[symbol] = component

    return markets

def download_csv(instrument, begin, end, frequency):

    markets = top_markets()
    if instrument not in markets:
        raise Exception("Instrument is not known: %s" % instrument)

    market = markets[instrument]

    data = {  
        'counter' : market['counter'],
        'base' : market['base'],
        'startTime' : begin,
        'endTime' : end,
        'timeIncrement' : RIPPLECHART_FREQUENCY[frequency],
        'timeMultiple' : 1,
        'format' : "csv"
    }

    req = urllib2.Request('http://api.ripplecharts.com/api/offers_exercised')
    req.add_header('Content-Type', 'application/json')

    f = urllib2.urlopen(req, json.dumps(data))

    if f.getcode() != 200:
        raise Exception("Failed to download data: %s" % f.getcode())
    buff = f.read()
    f.close()

    # Remove the BOM
    #while not buff[0].isalnum():
    #    buff = buff[1:]

    return buff

def download_bars(instrument, begin, end, frequency, csvFile):
    bars = download_csv(instrument, begin, end, frequency)
    f = open(csvFile, 'w')
    f.write(bars)
    f.close()

def download_recent_bars(instrument, periods, frequency, step, csvFile):
    """Download most recent bars from Ripple Charts for a given frequency and number of days
    """
    end = datetime.datetime.now() - datetime.timedelta(seconds=frequency) # Last period, not present
    begin = end - datetime.timedelta(seconds=((periods + 1) * frequency)) # X periods

    bars = download_csv(instrument, str(begin), str(end), frequency)
    f = open(csvFile, "w")
    f.write(bars)
    f.close()

def download_daily_bars(instrument, year, csvFile):
    """Download daily bars from Ripple Charts for a given year.
    """
    begin = str(year)+'-01-01'
    end = str(year)+'-12-31'
    bars = download_csv(instrument, begin, end, bar.Frequency.DAY)
    f = open(csvFile, "w")
    f.write(bars)
    f.close()


def download_hourly_bars(instrument, year, csvFile):
    """Download hourly bars from Ripple Charts for a given year.

    :param instrument : instrument dict 
    :param year: The year. int
    :param csvFile: The path to the CSV file to write. string
    """
    begin = str(year)+'-01-01'
    end = str(year)+'-12-31'
    bars = download_csv(instrument, begin, end, bar.Frequency.HOUR)
    f = open(csvFile, "w")
    f.write(bars)
    f.close()

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

    logger = pyalgotrade.logger.getLogger("ripple")
    ret = barfeed.Feed(frequency, timezone)

    if not os.path.exists(storage):
        logger.info("Creating %s directory" % (storage))
        os.mkdir(storage)

    for instrument in instruments:
        fileName = os.path.join(storage, "%s-last-%d-%s-ripplecharts.csv" % (instrument.replace('/','_'), periods, RIPPLECHART_FREQUENCY[frequency]))
        if not os.path.exists(fileName):
            logger.info("Downloading %s to %s" % (instrument, fileName))
            try:
                if frequency in RIPPLECHART_FREQUENCY:
                    download_recent_bars(instrument, periods, frequency, step, fileName)
                else:
                    raise Exception("Invalid frequency")
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
            fileName = os.path.join(storage, "%s-%d-ripplecharts.csv" % (instrument.replace('/','_'), year))
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

    markets = top_markets()
    for market in markets:
        print market
    assert len(markets) > 0

    instrument = 'BTC.E2q/XRP' # BTC/XRP rMwjYedjc7qqtKYVLiAccJSmCwih4LnE2q
    test_csvFile = '../../samples/data/' + instrument.replace('/','_') + '-sample-ripplecharts.csv'

    # Test download from RippleData
    bars = download_csv(instrument,'2015-03-30', '2015-03-31',bar.Frequency.HOUR)
    print bars
    assert len(bars) == 5549
      
    # Test download to CSV
    download_bars(instrument,'2015-03-30', '2015-03-31',bar.Frequency.HOUR,test_csvFile)
    with open(test_csvFile, 'rb') as f:
        reader = csv.reader(f)
        data = list(reader)
    assert len(data) == 25
    
    # Test download_daily_bars
    download_daily_bars(instrument,2014,test_csvFile)
    with open(test_csvFile, 'rb') as f:
        reader = csv.reader(f)
        data = list(reader)
    assert len(data) == 356
    
    # Test download_hourly_bars
    download_hourly_bars(instrument,2014,test_csvFile)
    with open(test_csvFile, 'rb') as f:
        reader = csv.reader(f)
        data = list(reader)
    assert len(data) == 6424
    
    # Test download recent bars
    download_recent_bars(instrument, 90, bar.Frequency.DAY, 1, test_csvFile)
    with open(test_csvFile, 'rb') as f:
        reader = csv.reader(f)
        data = list(reader)
    assert len(data) == 92

    # Test build_feed
    feed = build_feed([instrument], 2014, 2015, '../../samples/data/')
    #feed = build_feed(instruments, 2014, 2015, '../../samples/data/')
    
    # Test build_feed_recent
    feed = build_feed_recent([instrument], periods=24, storage='../../samples/data/', frequency=bar.Frequency.HOUR)
