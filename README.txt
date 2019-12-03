Changes:

	- If lowest ask price > highest bid price, no longer throws an error
	- After each time step, brokers pay (or are paid) for any deficit (surplus) of energy, then have their energy balance zeroed out.
	- In Broker.py, the functions get_energy_imbalance and gain_revenue have been slightly changed.  You can copy and paste
over the old ones to fix this - they shouldn't affect anything you've coded so far.
	- Customer choice randomness tweaked.  Top choice has a large chance of being chosen, second and third options have significantly reduced chances.