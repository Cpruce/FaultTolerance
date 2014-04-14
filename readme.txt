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


We had a lot of issues with this assignment. Cory doesn't like to test
his code but writes what things a program should contain, so it's a little difficult to merge
code and have the whole thing run and see which part broke.
Tum likes to have the code running and keeps adding things one by one and tested in each step,
so we end up with two different versions...


(a) Tum's version (storage_process_working.erl, key_value_node_working.erl) that
works for all commands and tested, but the second node couldn't join and no backup code
(b) Cory's version which has node joining and backup code but hasn't been tested
and doesn't respond to any commands...


For example, you can run

bash simple_test_working.sh

to start the first node of the '_working' version. 

To start Cory's version, run 

bash run_all_tests.sh

If you look to get some respond back, we recommend testing with the '_working'
version and looking at storage_process_working.erl, key_value_node_working.erl
to see how back up might work.

Then you can interact through the external controller (after running bash simple_test_working.sh):

MAINNODE=[INSERT_YOUR_NODE@HOST_HERE]
erlc external_controller.erl
erl -noshell -run external_controller main $MAINNODE num_keys
erl -noshell -run external_controller main $MAINNODE store apple "is cool"
erl -noshell -run external_controller main $MAINNODE num_keys
erl -noshell -run external_controller main $MAINNODE store apple "is actually ok"
erl -noshell -run external_controller main $MAINNODE num_keys
erl -noshell -run external_controller main $MAINNODE node_list
erl -noshell -run external_controller main $MAINNODE store cucumber "ewww"
erl -noshell -run external_controller main $MAINNODE store banana "is fantastic"
erl -noshell -run external_controller main $MAINNODE first_key
erl -noshell -run external_controller main $MAINNODE last_key
erl -noshell -run external_controller main $MAINNODE num_keys


More testing can be found in testing.txt.