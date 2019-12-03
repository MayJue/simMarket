from Tariff import Tariff

class Broker():

    def __init__( self, idx ):

        ## ID number, cash balance, energy balance
        self.idx   = idx
        self.cash  = 0
        self.power = 0

        self.customer_usage = None
        self.other_data = None

        ## Lists to contain:
        ##     asks: tuples of the form ( quantity, price )
        ##     tariffs: Tariff objects to submit to the market
        ##     customers: integers representing which customers are subscribed
        ##                to your tariffs.
        self.asks      = []
        self.tariffs   = []
        self.customers = []

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

    ## Returns a list of asks of the form ( price, quantity ).
    def post_asks( self, time ):
        
        return [ (i, 100) for i in range(1,101) ]

    ## Returns a list of Tariff objects.
    def post_tariffs( self, time ):

        return [Tariff( self.idx, price=100, duration=3, exitfee=0 )]

    ## Receives data for the last time period from the server.
    def receive_message( self, msg ):
        pass
        
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
