"""
Copyright Blackarbs LLC.
Use entirely at your own risk.
This algorithm contains open source code from other sources and no claim is being
made to that code.

author: Brian Christopher, CFA, Blackarbs LLC
contact: bcr@blackarbs.com
"""
import pandas as pd
import numpy as np


class BetAgainstStreaks(QCAlgorithm):
    def Initialize(self):
        self.SetStartDate(2008, 1, 1)  # Set Start Date
        self.SetEndDate(2020, 12, 31)  # Set Start Date
        self.SetCash(250000)  # Set Strategy Cash

        self.SetBrokerageModel(
            BrokerageName.InteractiveBrokersBrokerage, AccountType.Margin
        )

        # -----------------------------------------------------------------------------
        # init custom universe
        # -----------------------------------------------------------------------------

        self.RESOLUTION = Resolution.Minute
        self.UNIVERSE = []
        self._num_coarse = 100  # max number of coarse symbols to return

        # this add universe method accepts two parameters:
        # - coarse selection function: accepts an IEnumerable<CoarseFundamental> and returns an IEnumerable<Symbol>
        self.AddUniverse(self.CoarseSelectionFunction)

        self.spy = self.AddEquity("SPY", Resolution.Minute).Symbol

        self.streak_number = 5  # how many days in a row to fade
        self.allocation_pct = 0.01  # how much to allocate per trade
        self.holdings = (
            dict()
        )  # to hold information on how long we have been in a position
        self.max_holding_period = 5  # man number of days to hold
        self.plot_count = 0  # to track how many holdings we have on avg

        self.Schedule.On(
            self.DateRules.EveryDay(self.spy),
            self.TimeRules.BeforeMarketClose(self.spy, 30),
            Action(self.rebalance),
        )

        self.splotName = "Strategy Info"
        sPlot = Chart(self.splotName)
        sPlot.AddSeries(Series("NumHoldings", SeriesType.Line, 0))
        self.AddChart(sPlot)

    def CoarseSelectionFunction(self, coarse):

        # Filter the values of the dict: by price
        values = filter(
            lambda x: (x.AdjustedPrice >= 5.0) and (x.AdjustedPrice <= 1000.0), coarse
        )

        # sort descending by daily dollar volume
        sortedByDollarVolume = sorted(
            values, key=lambda x: x.DollarVolume, reverse=True
        )

        # return the symbol objects of the top entries from our sorted collection
        self.UNIVERSE = [
            x.Symbol
            for x in sortedByDollarVolume[: self._num_coarse]
            if x.HasFundamentalData
        ]
        return self.UNIVERSE

    def OnSecuritiesChanged(self, changes):
        """
        This is an automatically generated event that occurs when the Universe is changed.

        In this function a symbol that is removed from the Universe is removed and liquidated from
        the portfolio.
        """
        self.changes = changes

        for sec in self.changes.RemovedSecurities:

            if sec.Symbol in self.UNIVERSE:
                self.UNIVERSE.remove(sec.Symbol)
        return

    def get_streaking_symbols(self):
        """
        Get history +1 day for the streaks we are looking for.
        Compute cl/cl returns and identify if there are any streaks to bet against.
        store symbols for rebalance.
        Run once daily
        """
        hist = self.History(self.UNIVERSE, self.streak_number + 1, Resolution.Daily)
        if "close" in hist.columns:
            hist = (
                hist["close"]
                .unstack(level=0)
                .astype(np.float32)
                .sort_index()
                .ffill()
                .fillna(0.0)
            )

        else:
            self.Debug(f"{self.Time} | close column missing from history! return empty")
            return [], []

        returns = hist.pct_change().dropna().apply(np.sign)

        short_cond = returns.sum() == self.streak_number
        long_cond = returns.sum() == -self.streak_number

        short_symbols = short_cond[short_cond == True].index.tolist()
        long_symbols = long_cond[long_cond == True].index.tolist()
        return short_symbols, long_symbols

    def remove_duplicate_symbols(self, shorts, longs):
        """
        Function to remove symbols from our target lists that are already in our holdings
        """
        for ss in shorts:
            if ss in self.holdings:
                shorts.remove(ss)

        for ls in longs:
            if ls in self.holdings:
                longs.remove(ls)

        return shorts, longs

    def increment_holding_period(self):
        """
        convenience function to increment the counter for the holding period
        for each symbol held
        """
        for holding in self.holdings:
            self.holdings[holding] += 1
        return

    def liquidate_stale_holdings(self):
        """
        liquidate any positions that have reached our maximum holding period
        """
        for holding, holding_period in self.holdings.copy().items():
            if holding_period >= self.max_holding_period:
                self.Liquidate(holding)
                del self.holdings[holding]
                self.Debug(
                    f"{self.Time} | {holding} max holding period reached liquidating"
                )
        return

    def rebalance(self):
        """
        - Run once daily.
        - Get list of stocks with streaks to bet against.
        - Enter positions (what's the allocation? eq weight, inverse vol, dollar value?)
         and save entry date/counter for each position.
        - If already in position check how long algo has been holding.
        - If it is gt or eq to holding period liquidate
        """

        self.increment_holding_period()

        shorts, longs = self.get_streaking_symbols()
        shorts, longs = self.remove_duplicate_symbols(shorts, longs)

        self.liquidate_stale_holdings()

        for short_symbol in shorts:
            self.SetHoldings(short_symbol, -self.allocation_pct)
            self.holdings[short_symbol] = 0

        for long_symbol in longs:
            self.SetHoldings(long_symbol, self.allocation_pct)
            self.holdings[long_symbol] = 0

        # track number of holdings
        if self.plot_count >= self.max_holding_period:
            self.Plot(self.splotName, "NumHoldings", len(self.holdings.keys()))
            self.plot_count = 0
        else:
            self.plot_count += 1
        return

    def OnData(self, data):
        """OnData event is the primary entry point for your algorithm. Each new data point will be pumped in here.
        Arguments:
            data: Slice object keyed by symbol containing the stock data
        """
        pass
