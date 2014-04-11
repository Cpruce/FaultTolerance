Tum Chaturapruek
Cory Pruce
CSCI182E - Assignment 5


This assignment provides a lot of design decisions, which are quite handy
when implementing. We got confused with the race condition when a process
registers globally but other processes could not see it. We learn a lot 
about message passing and how to make the system fault tolerant.

Note about ets: we use the ets module to store tables. Please increase
the limit of the number of tales to store from 1400 to ____
by setting the environment variable ERL_MAX_ETS_TABLES before starting the
Erlang runtime system (i.e. with the -env option to erl/werl).
