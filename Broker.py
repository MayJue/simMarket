from Tariff import Tariff

class Broker():

    def __init__( self, idx ):

        ## ID number, cash balance, energy balance
        self.idx   = idx
        self.cash  = 0
        self.power = 0

        self.customer_usage = None
        self.other_data = None
        self.currentData = None

        self.pastAverage = 0
        ## Lists to contain:
        ##     asks: tuples of the form ( quantity, price )
        ##     tariffs: Tariff objects to submit to the market
        ##     customers: integers representing which customers are subscribed
        ##                to your tariffs.
        self.asks      = []
        self.tariffs   = []
        self.customers = []

        self.currentPrice = []
        self.currentQuantity = []
        self.imbalances = 0
    ## A function to accept the bootstrap data set.  The data set contains:
    ##     usage_data, a dict in which keys are integer customer ID numbers,
    ##                     and values are lists of customer's past usage profiles.
    ##     other_data, a dict in which 'Total Demand' is a list of past aggregate demand
    ##                 figures, 'Cleared Price' is a list of past wholesale prices,
    ##                 'Cleared Quantity' is a list of past wholesale quantities,
    ##                 and 'Difference' is a list of differences between cleared
    ##                 quantities and actual usage.
    def get_initial_data( self, usage_data, other_data ):
        self.customer_usage = usage_data
        self.other_data = other_data

    def csvAveragePrice(self, time):
        prices = self.other_data['Cleared Price']
        temp = []
        for i in range(len(prices)): #goes through all the time and iterates through 24 hours
            if i %24 == time% 24: #price at the given time
                temp.append(prices[i])
        averagePrice = sum(temp)/len(temp)
        return averagePrice

    def currentAveragePrice(self, time):
        prices = self.currentPrice
        temp = []
        for i in range(len(prices)): #goes through all the time and iterates through 24 hours
            if i %24 == (time) % 24: #price at the given time
                temp.append(prices[i])
        averagePrice = sum(temp)/len(temp)
        return averagePrice
    ## Returns a list of asks of the form ( price, quantity ).
    def post_asks( self, time ):
        #gets all the prices in the 'OtherData.csv'
        print(time)
        averagePrice = 0
        if len(self.currentPrice) == 0:
            averagePrice = self.csvAveragePrice(time)
            self.pastAverage = averagePrice
        else:
            pastCsvAveragePrice = self.pastAverage
            curAveragePrice = self.currentAveragePrice(time -1)

            csvAveragePrice = self.csvAveragePrice(time)
            diff = pastCsvAveragePrice - curAveragePrice
            if diff <= 5 or diff >= -5:
                averagePrice = csvAveragePrice
            elif diff > 5 or diff < -5:
                averagePrice = csvAveragePrice + diff
        pastUsage = []
        usage = []
        if len(self.customers) > 0:
            for customer in self.customers:
                usage = self.customer_usage[customer+1]
                for i in range(len(usage)):
                    if i % 24 == time% 24:
                        pastUsage.append(usage[i])
            quantity = sum(usage)/len(usage)

            quantity *= len(self.customers)
        else:
            totalCustomer = 0
            for customer in self.customer_usage:
                totalCustomer += 1
                usage = self.customer_usage[customer]
                for i in range(len(usage)):
                    if i % 24 == time% 24:
                        pastUsage.append(usage[i])
            quantity = sum(usage)/len(usage)
            quantity *= (totalCustomer * .2)
        print(self.imbalances)
        if self.imbalances > 0 and self.imbalances < quantity:
            print('here')
            quantity -= self.imbalances
        elif self.imbalances < 0:
            print('this')
            quantity -= self.imbalances/2

        return [ (averagePrice*.9, quantity), (averagePrice*.8, quantity/2), (averagePrice*.5, quantity/2) ]

    ## Returns a list of Tariff objects.
    def post_tariffs( self, time ):
        averagePrice = 0
        if len(self.currentPrice) == 0:
            averagePrice = self.csvAveragePrice(time)
        else:
            pastCsvAveragePrice = self.csvAveragePrice(time-1)
            curAveragePrice = self.currentAveragePrice(time)
            csvAveragePrice = self.csvAveragePrice(time)
            diff = pastCsvAveragePrice - curAveragePrice
            if diff <= 5 or diff >= -5:
                averagePrice = csvAveragePrice
            elif diff > 5 or diff < -5:
                averagePrice = csvAveragePrice + diff
        return [Tariff( self.idx, price=averagePrice, duration=3, exitfee=averagePrice/2 )]

    ## Receives data for the last time period from the server.
    def receive_message( self, msg ):
        self.currentData = msg
        self.currentPrice.append(msg['Cleared Price'])
        self.currentQuantity.append(msg['Cleared Quantity'])
        self.imbalances = msg['Imbalance']

    ## Returns a negative number if the broker doesn't have enough energy to
    ## meet demand.  Returns a positive number otherwise.
    def get_energy_imbalance( self, data ):
        return self.power

    def gain_revenue( self, customers, data ):
        for c in self.customers:
            self.cash += data[c] * customers[c].tariff.price
            self.power -= data[c]

    ## Alter broker's cash balance based on supply/demand match.
    def adjust_cash( self, amt ):
        self.cash += amt
