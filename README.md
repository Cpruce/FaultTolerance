test my running

	erlc storage_process_temp.erl && erlc key_value_node_temp.erl && erl -noshell -run key_value_node_temp main 3 node1


	erlc storage_process_temp.erl && erlc key_value_node_temp.erl && erl -noshell -run key_value_node_temp main 3 node2 node1@Js-MacBook-Pro-8

or

	bash ./tests/automated_testing/simple_test.sh
