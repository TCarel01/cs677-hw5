Dependencies:
pandas (to install, run "pip install pandas" to install)
plotly (only needed for benchmarking script. To install, run "pip install plotly")

Note that our program's stopping often generates I/O errors in the terminal, as we close the log files when
we send the STOP message to each node, but sometimes a thread will try to write to that log.
Given that no shutdown requirements are included, this should be fine.

To run the main program,
one need only run “python main.py {number of total peer nodes} {number of leaders}”
where brackets and their contents are replaced with desired values,
and with the needed dependencies (pandas) installed.
Note that specifying number of leaders means that N_T is configurable.

To run the experiments (benchmarking) script,
run "python benchmarking.py"
with the needed dependencies (pandas and plotly) installed.
This file can be used to run the network with various different options,
including with a fault simulation as well as with different numbers of buyers, sellers, and traders.
To use these different configurations, update the parameters in the "if __name__ == '__main__'" statement.
Specifically, update the values assigned to the variables num_traders, num_buyers, num_sellers, use_caching_version,
time_to_die, and runtime = 300. Note that setting time_to_die to 0 will disable the fault simulation, while
setting it to a nonzero time will determine when the fault simulation is triggered.
Note that the included output files (in the logs folder) can be used to show all benchmarking.
Note that for eahc experiment, we manually altered the the parameters in the "if __name__ == '__main__'" statement
and then recorded the result metrics.

To run the unit testing script,
run "python hw5_unit_testing.py"
with the needed dependencies (pandas) installed.

To run the other testing script,
run "python hw5_testing.py"
with the needed dependencies (pandas) installed.
Note that you'll need to specify which test to run by changing the
value of the test_case variable in the "if __name__ == '__main__'" statement.
Note that when running the fault tolerance script, you'll need to manually shut down the program,
as the node simulating a fault will not recieve the STOP message once runtime has ended.

Note that all tests were run and passed.
