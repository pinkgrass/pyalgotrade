import sma_crossover
from pyalgotrade import plotter
#from pyalgotrade.tools import yahoofinance
from pyalgotrade.oanda import oanda
from pyalgotrade.stratanalyzer import sharpe

# Example Ripple strategy using sma_crossover_sample as the base
# Richard Crook <richard@pinkgrass.org>

def main(plot):
    instrument = 'GBP_USD' 

    smaPeriod = 163

    # Download the bars.
    feed = oanda.build_feed([instrument], 2014, 2015, 'data')

    strat = sma_crossover.SMACrossOver(feed, instrument, smaPeriod)
    sharpeRatioAnalyzer = sharpe.SharpeRatio()
    strat.attachAnalyzer(sharpeRatioAnalyzer)

    if plot:
        plt = plotter.StrategyPlotter(strat, True, False, True)
        plt.getInstrumentSubplot(instrument).addDataSeries("sma", strat.getSMA())

    strat.run()
    print "Sharpe ratio: %.2f" % sharpeRatioAnalyzer.getSharpeRatio(0.05)

    if plot:
        plt.plot()


if __name__ == "__main__":
    main(True)
