import pandas as pd
import numpy as np
import math
import copy
import QSTK.qstkutil.qsdateutil as du
import datetime as dt
import QSTK.qstkutil.DataAccess as da
import QSTK.qstkutil.tsutil as tsu
import QSTK.qstkstudy.EventProfiler as ep

"""
Accepts a list of symbols along with start and end date
Returns the Event Matrix which is a pandas Datamatrix
Event matrix has the following structure :
	|IBM |GOOG|XOM |MSFT| GS | JP |
(d1)|nan |nan | 1  |nan |nan | 1  |
(d2)|nan | 1  |nan |nan |nan |nan |
(d3)| 1  |nan | 1  |nan | 1  |nan |
(d4)|nan |  1 |nan | 1  |nan |nan |
...................................
...................................
Also, d1 = start date
nan = no information about any event.
1 = status bit(positively confirms the event occurence)
"""


def find_events(ls_symbols, d_data):
	''' Finding the event dataframe '''
	df_close = d_data['actual_close']
	ts_market = df_close['SPY']

	print "Finding Events"

	# Creating an empty dataframe
	df_events = copy.deepcopy(df_close)
	df_events = df_events * np.NAN

	# Time stamps for the event range
	ldt_timestamps = df_close.index

	print "Creating orders.csv file"
	for s_sym in ls_symbols:
		for i in range(1, len(ldt_timestamps)):
			# Calculating the returns for this timestamp
			f_symprice_today = df_close[s_sym].ix[ldt_timestamps[i]]
			f_symprice_yest = df_close[s_sym].ix[ldt_timestamps[i - 1]]

			with open('orders.csv','a') as f:
				if f_symprice_today < 8.0 and f_symprice_yest >= 8.0:
					buy_date = ldt_timestamps[i]

					try:
						sell_date = ldt_timestamps[i+5]
					except IndexError:
						sell_date = ldt_timestamps[-1]

					f.write(str(buy_date.year) + ',' + str(buy_date.month) + ',' + str(buy_date.day) + ',' + str(s_sym) + ',Buy,100\n')
					f.write(str(sell_date.year) + ',' + str(sell_date.month) + ',' + str(sell_date.day) + ',' + str(s_sym) + ',Sell,100\n')
					df_events[s_sym].ix[ldt_timestamps[i]] = 1
			# Event is found if the symbol is down more then 3% while the
			# market is up more then 2%
			# if f_symprice_today < 7.0 and f_symprice_yest >= 7.0:
			#    df_events[s_sym].ix[ldt_timestamps[i]] = 1

	print "orders.csv file created!"
	
	return df_events



class BollingerEvents:
	def __init__(self, startDate, endDate, symbols):
		print "Initializing Event Study"
		self.startDate = startDate
		self.endDate = endDate
		self.symbols = symbols
		self.mainData = self.getStockPrices(self.startDate, self.endDate, self.symbols)
		self.stockClosingPrices = self.mainData['close'].values
		self.dataFrameBollinger = self.getBollingerValue(self.stockClosingPrices, lookback = 20)
		self.bollEvents = self.findBollingerEvents ( self.symbols, self.dataFrameBollinger)
		# self.eventProfiler(self.bollEvents)

	def getStockPrices(self, startDate, endDate, symbols):
		print "Getting Stock Closing prices"
		dt_timeofday = dt.timedelta(hours=16)
		self.marketOpenDays = du.getNYSEdays(startDate, endDate, dt_timeofday)
		dataAccessObj = da.DataAccess('Yahoo')
		readKeys = ['close','actual_close']
		ldf_data = dataAccessObj.get_data(self.marketOpenDays, symbols, readKeys)
		d_data = dict(zip(readKeys, ldf_data))

		for s_key in readKeys:
		   d_data[s_key] = d_data[s_key].fillna(method='ffill')
		   d_data[s_key] = d_data[s_key].fillna(method='bfill')
		   d_data[s_key] = d_data[s_key].fillna(1.0)

		return d_data

	def getBollingerValue(self,stockClosingPrices, lookback = 20):
		rollingMean = pd.rolling_mean(stockClosingPrices, lookback)
		rollingStd = pd.rolling_std(stockClosingPrices, lookback)
		bollingerBands = (stockClosingPrices - rollingMean) / rollingStd
		df_bollinger = pd.DataFrame(bollingerBands, index=self.marketOpenDays, columns=self.symbols)
		return df_bollinger

	def findBollingerEvents(self,ls_symbols, d_data):
		print "Finding Events"
		df_events = copy.deepcopy(d_data)
		df_events = df_events * np.NAN

		ldt_timestamps = df_events.index
		for s_sym in ls_symbols:
			for i in range(1, len(ldt_timestamps)):
				spy_today = d_data['SPY'].ix[ldt_timestamps[i]]
				f_boll_today = d_data[s_sym].ix[ldt_timestamps[i]]
				f_boll_yest = d_data[s_sym].ix[ldt_timestamps[i - 1]]

				with open('orders.csv','a') as f:
					if (f_boll_today < -2.0 and f_boll_yest >= -2.0 and spy_today >= 1.5):
						d = ldt_timestamps[i]
						try:
							d2 = ldt_timestamps[i+5]
						except IndexError:
							d2 = ldt_timestamps[-1]

						f.write(str(d.year) + ',' + str(d.month) + ',' + str(d.day) + ',' + str(s_sym) + ',Buy,100\n')
						f.write(str(d2.year) + ',' + str(d2.month) + ',' + str(d2.day) + ',' + str(s_sym) + ',Sell,100\n')
						df_events[s_sym].ix[d] = 1
		return df_events

	def eventProfiler(self, df_events):
		print "Running the Profiler"
		ep.eventprofiler(df_events, self.mainData, i_lookback=20, i_lookforward=20,
				 s_filename='2012Bollinger.pdf', b_market_neutral=True,
				 b_errorbars=True, s_market_sym='SPY')



if __name__ == '__main__':
	# Start Bollinger Event Study
	dataobj = da.DataAccess('Yahoo')
	ls_symbols = dataobj.get_symbols_from_list('sp5002012')
	ls_symbols.append('SPY')
	bollEvents = BollingerEvents(startDate=dt.datetime(2008, 1, 1), endDate=dt.datetime(2009, 12, 31), symbols=ls_symbols)
