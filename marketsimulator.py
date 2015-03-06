# QSTK Imports
import QSTK.qstkutil.qsdateutil as du
import QSTK.qstkutil.tsutil as tsu
import QSTK.qstkutil.DataAccess as da

# Third Party Imports
import datetime as dt
import pandas as pd
import numpy as np

# import eventstudies.py
import eventstudies as es

_ordersfile = "orders.csv"


class MarketSimulator:
		
	def __init__(self, ordersFile, startingCash):        
		self.ordersList = self.readCSVFile(ordersFile)
		self.startDate, self.endDate = self.findDates()
		self.uniqueSymbols = self.findUniqueSymbols()
		self.mainData = self.getStockPrices()
		self.startingCash = startingCash
		self.dailyValues = self.getDailyValues()
		self.sharpeRatio, self.totalReturn, self.standardDeviation, self.averageDailyReturns = self.reportOutput()
		print self.sharpeRatio, self.totalReturn, self.standardDeviation, self.averageDailyReturns
		
	def readCSVFile(self, ordersFile):
		# 'S10' is a 10 character string
		dataType = [('year', int), ('month', int), ('day', int), ('symbol', 'S10'), ('typeOfOrder', 'S10'), ('noOfShares', int)]
		self.ordersList = np.loadtxt(ordersFile, delimiter=',', dtype=dataType)
		self.ordersList = np.sort(self.ordersList, order=['year', 'month', 'day'])
		return self.ordersList

	def findDates(self):
		firstRow = self.ordersList[0]
		lastRow = self.ordersList[-1]
		startDate = dt.datetime (firstRow['year'], firstRow['month'], firstRow['day'])
		endDate = dt.datetime (lastRow['year'], lastRow['month'], lastRow['day']) + dt.timedelta(days=1)
		return startDate, endDate

	def findUniqueSymbols(self):
		return np.unique(self.ordersList['symbol'])

	def getStockPrices(self):
		dt_timeofday = dt.timedelta(hours=16)
		self.marketOpenDays = du.getNYSEdays(self.startDate, self.endDate, dt_timeofday)
		dataAccessObj = da.DataAccess('Yahoo')
		readKeys = ['open', 'high', 'low', 'close', 'volume', 'actual_close']
		ldf_data = dataAccessObj.get_data(self.marketOpenDays, self.uniqueSymbols, readKeys)
		d_data = dict(zip(readKeys, ldf_data))

		for s_key in readKeys:
		   d_data[s_key] = d_data[s_key].fillna(method='ffill')
		   d_data[s_key] = d_data[s_key].fillna(method='bfill')
		   d_data[s_key] = d_data[s_key].fillna(1.0)

		return d_data

	def getDailyValues(self):
		startingCash = self.startingCash
		uniqueSymbols = self.uniqueSymbols
		marketOpenDays = self.marketOpenDays
		stockClosingPrices = self.mainData['close'].values
		dailyValues = np.zeros ( (len(marketOpenDays),4))

		currentPortfolioHoldings = {}

		for symbol in uniqueSymbols:
			currentPortfolioHoldings[symbol] = 0


		dailyPortfolioValues = np.zeros( (len ( marketOpenDays ), len (uniqueSymbols)))
		
		for marketOpenDay in marketOpenDays:
			dateIndex = marketOpenDays.index(marketOpenDay)

			for order in self.ordersList:
				orderDate = dt.datetime(order['year'], order['month'], order['day'],16)

				if marketOpenDay.date() == orderDate.date():
					symbol = order['symbol']
					typeOfOrder = order['typeOfOrder']
					noOfShares = order['noOfShares']
					symbolIndex = np.where(uniqueSymbols == symbol)
					stockPrice = stockClosingPrices[dateIndex][symbolIndex]
					

					if symbol not in currentPortfolioHoldings:
						currentPortfolioHoldings[symbol] = 0


					if typeOfOrder == 'Buy':
						# print date,symbol, typeOfOrder, noOfShares
						currentPortfolioHoldings[symbol] += noOfShares
						startingCash -= stockPrice * noOfShares

					elif typeOfOrder =='Sell':
						# print date,symbol, typeOfOrder, noOfShares
						currentPortfolioHoldings[symbol] -= noOfShares
						startingCash += stockPrice * noOfShares


			for symbol in uniqueSymbols:
				symbolIndex = np.where(uniqueSymbols == symbol)
				dailyPortfolioValues[dateIndex][symbolIndex] = stockClosingPrices[dateIndex][symbolIndex]*currentPortfolioHoldings[symbol]

			dayValue = np.sum(np.nan_to_num(dailyPortfolioValues[dateIndex])) + startingCash
			dailyValues[dateIndex] = [marketOpenDay.year, marketOpenDay.month, marketOpenDay.day, dayValue]

		return dailyValues




	def reportOutput(self):
		dailyValues = self.dailyValues
		startDate = dt.datetime(int(dailyValues[0, 0]), int(dailyValues[0, 1]), int(dailyValues[0, 2]))
		endDate = dt.datetime(int(dailyValues[-1, 0]), int(dailyValues[-1, 1]), int(dailyValues[-1, 2]))

		totalReturn = dailyValues[-1,3]/dailyValues[0,3]

		dailyReturns = dailyValues.copy()[:,3]
		tsu.returnize0(dailyReturns)
		standardDeviation = np.std(dailyReturns)
		averageDailyReturns = np.average(dailyReturns)

		sharpeRatio = (averageDailyReturns/standardDeviation * np.sqrt(252))

		return sharpeRatio, totalReturn, standardDeviation, averageDailyReturns



if __name__ == '__main__':
		sim = MarketSimulator(_ordersfile, startingCash = 100000)

