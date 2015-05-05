from pyalgotrade import strategy
from pyalgotrade.oanda import barfeed


class MyStrategy(strategy.BacktestingStrategy):
    def __init__(self, feed, instrument):
        strategy.BacktestingStrategy.__init__(self, feed)
        self.__instrument = instrument

    def onBars(self, bars):
        bar = bars[self.__instrument]
        self.info(bar.getClose())
        self.info(bar.getVolume())
        self.info(bar.getOpen())
        self.info(bar.getHigh())
        self.info(bar.getLow())

# Load the yahoo feed from the CSV file
feed = barfeed.Feed()
feed.addBarsFromCSV('GBP_USD', 'data/oanda.csv')

# Evaluate the strategy with the feed's bars.
myStrategy = MyStrategy(feed, "GBP_USD")
myStrategy.run()
