Tum Chaturapruek
Cory Pruce
CSCI182E - Assignment 5


This assignment provides a lot of design decisions, which are quite handy
when implementing. We got confused with the race condition when a process
registers globally but other processes could not see it. We learn a lot 
about message passing and how to make the system fault tolerant.

Note about ets: we use the ets module to store tables. Please increase
the limit of the number of tales to store from 1400 to 100000
by setting the environment variable ERL_MAX_ETS_TABLES before starting the
Erlang runtime system (i.e. with the -env option to erl/werl).

For example, you can run
erlc storage_process.erl
erlc advertise_id.erl
erlc key_value_node.erl
erl -env ERL_MAX_ETS_TABLES=100000 -noshell -run key_value_node main 3 ctnode
# erl -env ERL_MAX_ETS_TABLES=100000 -noshell -run key_value_node_working main 12 node2 node1@ash


Our test console can be found here:
https://gist.github.com/tummykung/3ed57af6d7ed3bf8eccd

We don't have much time to do node list testing, but otherwise we tested quite thoroughly.